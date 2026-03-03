use std::sync::Arc;

use datafusion::common::{JoinSide, Result, exec_err};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::{ProjectionExpr, update_expr};

/// Spatial predicate is the join condition of a spatial join. It can be a
/// distance predicate or a relation predicate.
#[derive(Debug, Clone)]
pub enum SpatialPredicate {
    Distance(DistancePredicate),
    Relation(RelationPredicate),
}

impl std::fmt::Display for SpatialPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpatialPredicate::Distance(predicate) => write!(f, "{predicate}"),
            SpatialPredicate::Relation(predicate) => write!(f, "{predicate}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DistancePredicate {
    /// The expression for evaluating the geometry value on the left side. The
    /// expression should be evaluated directly on the left side batches.
    pub left: Arc<dyn PhysicalExpr>,
    /// The expression for evaluating the geometry value on the right side. The
    /// expression should be evaluated directly on the right side batches.
    pub right: Arc<dyn PhysicalExpr>,
    /// The expression for evaluating the distance value. The expression
    /// should be evaluated directly on the left or right side batches according
    /// to `distance_side`.
    pub distance: Arc<dyn PhysicalExpr>,
    /// The side of the distance expression. It could be `JoinSide::None` if the
    /// distance expression is not a column reference. The most common case
    /// is that the distance expression is a literal value.
    pub distance_side: JoinSide,
}

impl DistancePredicate {
    pub fn new(
        left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>, distance: Arc<dyn PhysicalExpr>,
        distance_side: JoinSide,
    ) -> Self {
        Self {
            left,
            right,
            distance,
            distance_side,
        }
    }
}

impl std::fmt::Display for DistancePredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ST_Distance({}, {}) < {}",
            self.left, self.right, self.distance
        )
    }
}

/// Spatial relation predicate is the join condition of a spatial join.
#[derive(Debug, Clone)]
pub struct RelationPredicate {
    /// The expression for evaluating the geometry value on the left side. The
    /// expression should be evaluated directly on the left side batches.
    pub left: Arc<dyn PhysicalExpr>,
    /// The expression for evaluating the geometry value on the right side. The
    /// expression should be evaluated directly on the right side batches.
    pub right: Arc<dyn PhysicalExpr>,
    /// The spatial relation type.
    pub relation_type: SpatialRelationType,
}

impl RelationPredicate {
    pub fn new(
        left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>,
        relation_type: SpatialRelationType,
    ) -> Self {
        Self {
            left,
            right,
            relation_type,
        }
    }
}

impl std::fmt::Display for RelationPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ST_{}({}, {})",
            self.relation_type, self.left, self.right
        )
    }
}

/// Type of spatial relation predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpatialRelationType {
    Intersects,
    Contains,
    Within,
    Covers,
    CoveredBy,
    Touches,
    Crosses,
    Overlaps,
    Equals,
}

impl SpatialRelationType {
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "st_intersects" => Some(SpatialRelationType::Intersects),
            "st_contains" => Some(SpatialRelationType::Contains),
            "st_within" => Some(SpatialRelationType::Within),
            "st_covers" => Some(SpatialRelationType::Covers),
            "st_coveredby" => Some(SpatialRelationType::CoveredBy),
            "st_touches" => Some(SpatialRelationType::Touches),
            "st_crosses" => Some(SpatialRelationType::Crosses),
            "st_overlaps" => Some(SpatialRelationType::Overlaps),
            "st_equals" => Some(SpatialRelationType::Equals),
            _ => None,
        }
    }

    pub fn invert(&self) -> Self {
        match self {
            SpatialRelationType::Intersects => SpatialRelationType::Intersects,
            SpatialRelationType::Covers => SpatialRelationType::CoveredBy,
            SpatialRelationType::CoveredBy => SpatialRelationType::Covers,
            SpatialRelationType::Contains => SpatialRelationType::Within,
            SpatialRelationType::Within => SpatialRelationType::Contains,
            SpatialRelationType::Touches => SpatialRelationType::Touches,
            SpatialRelationType::Crosses => SpatialRelationType::Crosses,
            SpatialRelationType::Overlaps => SpatialRelationType::Overlaps,
            SpatialRelationType::Equals => SpatialRelationType::Equals,
        }
    }
}

impl std::fmt::Display for SpatialRelationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpatialRelationType::Intersects => write!(f, "intersects"),
            SpatialRelationType::Contains => write!(f, "contains"),
            SpatialRelationType::Within => write!(f, "within"),
            SpatialRelationType::Covers => write!(f, "covers"),
            SpatialRelationType::CoveredBy => write!(f, "coveredby"),
            SpatialRelationType::Touches => write!(f, "touches"),
            SpatialRelationType::Crosses => write!(f, "crosses"),
            SpatialRelationType::Overlaps => write!(f, "overlaps"),
            SpatialRelationType::Equals => write!(f, "equals"),
        }
    }
}

/// Common operations needed by the planner/executor to keep spatial predicates
/// valid when join inputs are swapped or projected.
pub trait SpatialPredicateTrait: Sized {
    /// Returns a semantically equivalent predicate after the join children are
    /// swapped.
    ///
    /// Used by `SpatialJoinExec::swap_inputs` to keep the predicate aligned
    /// with the new left/right inputs.
    fn swap_for_swapped_children(&self) -> Self;

    /// Rewrites the predicate to reference projected child expressions.
    ///
    /// Returns `Ok(None)` when the predicate cannot be expressed using the
    /// projected inputs (so projection pushdown must be skipped).
    fn update_for_child_projections(
        &self, projected_left_exprs: &[ProjectionExpr], projected_right_exprs: &[ProjectionExpr],
    ) -> Result<Option<Self>>;
}

impl SpatialPredicateTrait for SpatialPredicate {
    fn swap_for_swapped_children(&self) -> Self {
        match self {
            SpatialPredicate::Relation(pred) => {
                SpatialPredicate::Relation(pred.swap_for_swapped_children())
            }
            SpatialPredicate::Distance(pred) => {
                SpatialPredicate::Distance(pred.swap_for_swapped_children())
            }
        }
    }

    fn update_for_child_projections(
        &self, projected_left_exprs: &[ProjectionExpr], projected_right_exprs: &[ProjectionExpr],
    ) -> Result<Option<Self>> {
        match self {
            SpatialPredicate::Relation(pred) => {
                Ok(pred
                    .update_for_child_projections(projected_left_exprs, projected_right_exprs)?
                    .map(SpatialPredicate::Relation))
            }
            SpatialPredicate::Distance(pred) => {
                Ok(pred
                    .update_for_child_projections(projected_left_exprs, projected_right_exprs)?
                    .map(SpatialPredicate::Distance))
            }
        }
    }
}

impl SpatialPredicateTrait for RelationPredicate {
    fn swap_for_swapped_children(&self) -> Self {
        Self {
            left: self.right.clone(),
            right: self.left.clone(),
            relation_type: self.relation_type.invert(),
        }
    }

    fn update_for_child_projections(
        &self, projected_left_exprs: &[ProjectionExpr], projected_right_exprs: &[ProjectionExpr],
    ) -> Result<Option<Self>> {
        let Some(left) = update_expr(&self.left, projected_left_exprs, false)? else {
            return Ok(None);
        };
        let Some(right) = update_expr(&self.right, projected_right_exprs, false)? else {
            return Ok(None);
        };

        Ok(Some(Self {
            left,
            right,
            relation_type: self.relation_type,
        }))
    }
}

impl SpatialPredicateTrait for DistancePredicate {
    fn swap_for_swapped_children(&self) -> Self {
        Self {
            left: self.right.clone(),
            right: self.left.clone(),
            distance: self.distance.clone(),
            distance_side: self.distance_side.negate(),
        }
    }

    fn update_for_child_projections(
        &self, projected_left_exprs: &[ProjectionExpr], projected_right_exprs: &[ProjectionExpr],
    ) -> Result<Option<Self>> {
        let Some(left) = update_expr(&self.left, projected_left_exprs, false)? else {
            return Ok(None);
        };
        let Some(right) = update_expr(&self.right, projected_right_exprs, false)? else {
            return Ok(None);
        };

        let distance = match self.distance_side {
            JoinSide::Left => {
                let Some(distance) = update_expr(&self.distance, projected_left_exprs, false)?
                else {
                    return Ok(None);
                };
                distance
            }
            JoinSide::Right => {
                let Some(distance) = update_expr(&self.distance, projected_right_exprs, false)?
                else {
                    return Ok(None);
                };
                distance
            }
            JoinSide::None => self.distance.clone(),
        };

        Ok(Some(Self {
            left,
            right,
            distance,
            distance_side: self.distance_side,
        }))
    }
}
