use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;

use super::filter::{FilterNode, parse_filter};
use super::sort::{SortField, parse_sort};

const MAX_QUERY_LIMIT: u64 = 10_000;

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub filter: Option<Value>,
    pub sort: Option<Vec<Value>>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub fields: Option<Vec<String>>,
    pub count_only: Option<bool>,
}

#[derive(Debug)]
pub struct ParsedQuery {
    pub filter: Option<FilterNode>,
    pub sort: Vec<SortField>,
    pub limit: u64,
    pub offset: u64,
    pub fields: Option<Vec<String>>,
    pub count_only: bool,
}

pub fn parse_query(req: QueryRequest) -> Result<ParsedQuery, AppError> {
    let filter = match req.filter {
        Some(f) => Some(parse_filter(&f)?),
        None => None,
    };

    let sort = match req.sort {
        Some(s) => parse_sort(&s),
        None => vec![],
    };

    Ok(ParsedQuery {
        filter,
        sort,
        limit: req.limit.unwrap_or(100).min(MAX_QUERY_LIMIT),
        offset: req.offset.unwrap_or(0),
        fields: req.fields,
        count_only: req.count_only.unwrap_or(false),
    })
}
