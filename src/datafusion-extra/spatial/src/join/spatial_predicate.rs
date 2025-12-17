use std::sync::Arc;

use datafusion::common::JoinSide;
use datafusion::physical_expr::PhysicalExpr;

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
