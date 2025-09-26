use anyhow::Result;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::{
    Expr, Ident, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement as SqlStatement,
};

use super::StatementRewrite;

#[derive(Debug)]
pub struct PostgresStmtRewrite;

impl StatementRewrite for PostgresStmtRewrite {
    fn name(&self) -> &str {
        "postgres_stmt_rewrite"
    }

    fn rewrite(&self, stmt: Statement) -> Result<Statement> {
        let mut stmt = stmt;
        if let Statement::Statement(sql_stmt) = &mut stmt
            && let SqlStatement::Query(query) = sql_stmt.as_mut()
            && let SetExpr::Select(select) = query.body.as_mut()
        {
            'block: {
                let mut wildcard_tables = Vec::new();
                let mut has_simple_wildcard = false;
                for p in &select.projection {
                    match p {
                        SelectItem::QualifiedWildcard(name, _) => {
                            match name {
                                SelectItemQualifiedWildcardKind::ObjectName(objname) => {
                                    let idents = objname
                                        .0
                                        .iter()
                                        .map(|v| v.as_ident().unwrap().value.clone())
                                        .collect::<Vec<_>>()
                                        .join(".");

                                    wildcard_tables.push(idents);
                                }
                                SelectItemQualifiedWildcardKind::Expr(_expr) => {}
                            }
                        }
                        SelectItem::Wildcard(_) => {
                            has_simple_wildcard = true;
                        }
                        _ => {}
                    }
                }

                if wildcard_tables.is_empty() && !has_simple_wildcard {
                    break 'block;
                }

                let mut new_projection = Vec::new();
                for p in select.projection.drain(..) {
                    match p {
                        SelectItem::UnnamedExpr(expr) => {
                            let alias_partial = match &expr {
                                Expr::Identifier(ident) => Some(ident.clone()),
                                Expr::CompoundIdentifier(idents) => {
                                    if idents.len() > 1 {
                                        let table_name = &idents[..idents.len() - 1]
                                            .iter()
                                            .map(|i| i.value.clone())
                                            .collect::<Vec<_>>()
                                            .join(".");
                                        if wildcard_tables.iter().any(|name| name == table_name) {
                                            Some(idents[idents.len() - 1].clone())
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            };

                            if let Some(name) = alias_partial {
                                let alias = format!("__alias_{name}");
                                new_projection.push(SelectItem::ExprWithAlias {
                                    expr,
                                    alias: Ident::new(alias),
                                });
                            } else {
                                new_projection.push(SelectItem::UnnamedExpr(expr));
                            }
                        }
                        _ => new_projection.push(p),
                    }
                }
                select.projection = new_projection;
            }
        }

        Ok(stmt)
    }
}
