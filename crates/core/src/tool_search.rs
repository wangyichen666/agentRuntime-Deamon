//! 纯冻结目录检索：不缓存权限，不持执行体或 repository。
use crate::{ExactOwner, ToolSpec};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const TOOL_SEARCH_LIMIT: usize = 8;
pub const TOOL_SEARCH_BYTES: usize = 16_384;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSearchQuery {
    pub query: String,
    #[serde(default)]
    pub limit: Option<usize>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSearchInvoke {
    pub name: String,
    pub arguments: serde_json::Value,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum ToolSearchRequest {
    Search(ToolSearchQuery),
    Invoke(ToolSearchInvoke),
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSearchResult {
    pub schema_version: u16,
    pub catalog_generation: String,
    pub tools: Vec<ToolSpec>,
    pub has_more: bool,
    pub dispatch_hint: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDiscoveryReceipt {
    pub schema_version: u16,
    pub owner: ExactOwner,
    pub operation_id: String,
    pub query: ToolSearchQuery,
    pub result: ToolSearchResult,
}
#[derive(Debug, thiserror::Error)]
pub enum ToolSearchError {
    #[error("冻结目录无效或超过预算")]
    InvalidCatalog,
    #[error("搜索 query/limit 无效")]
    InvalidQuery,
    #[error("精确工具名不在冻结授权目录：{0}")]
    UnknownName(String),
    #[error("完整工具 schema 超过发现字节预算")]
    OutputBudget,
    #[error("目录序列化失败：{0}")]
    Json(#[from] serde_json::Error),
}
pub struct ToolSearchIndex {
    generation: String,
    catalog: Vec<ToolSpec>,
    fields: Vec<Vec<BTreeMap<String, usize>>>,
}

/// snake/kebab/camel/alnum 按稳定字符边界拆词，中文独立单字和相邻双字。
pub fn tool_search_tokens(text: &str) -> BTreeMap<String, usize> {
    let mut tokens = BTreeMap::new();
    let chars = text.chars().collect::<Vec<_>>();
    let mut word = String::new();
    let mut chinese = Vec::new();
    let flush_word = |word: &mut String, tokens: &mut BTreeMap<String, usize>| {
        if !word.is_empty() {
            *tokens.entry(word.to_ascii_lowercase()).or_default() += 1;
            word.clear();
        }
    };
    let flush_chinese = |chinese: &mut Vec<char>, tokens: &mut BTreeMap<String, usize>| {
        for ch in chinese.iter() {
            *tokens.entry(ch.to_string()).or_default() += 1;
        }
        for pair in chinese.windows(2) {
            *tokens.entry(pair.iter().collect()).or_default() += 1;
        }
        chinese.clear();
    };
    for (i, ch) in chars.iter().copied().enumerate() {
        if ('\u{3400}'..='\u{9fff}').contains(&ch) {
            flush_word(&mut word, &mut tokens);
            chinese.push(ch);
            continue;
        }
        flush_chinese(&mut chinese, &mut tokens);
        if ch.is_ascii_alphanumeric() {
            let prev = i.checked_sub(1).and_then(|i| chars.get(i)).copied();
            let next = chars.get(i + 1).copied();
            if prev.is_some_and(|prev| {
                (prev.is_ascii_lowercase() && ch.is_ascii_uppercase())
                    || (prev.is_ascii_digit() != ch.is_ascii_digit()
                        && prev.is_ascii_alphanumeric())
                    || (prev.is_ascii_uppercase()
                        && ch.is_ascii_uppercase()
                        && next.is_some_and(|ch| ch.is_ascii_lowercase()))
            }) {
                flush_word(&mut word, &mut tokens);
            }
            word.push(ch);
        } else {
            flush_word(&mut word, &mut tokens);
        }
    }
    flush_word(&mut word, &mut tokens);
    flush_chinese(&mut chinese, &mut tokens);
    tokens
}

impl ToolSearchIndex {
    pub fn new(mut catalog: Vec<ToolSpec>, generation: String) -> Result<Self, ToolSearchError> {
        catalog.sort_by(|a, b| a.name.cmp(&b.name));
        if generation.is_empty()
            || catalog.len() > 8192
            || serde_json::to_vec(&catalog)?.len() > 16 * 1024 * 1024
            || catalog.iter().any(|tool| {
                tool.name.is_empty()
                    || tool.name.len() > 256
                    || tool.name.chars().any(char::is_whitespace)
            })
            || catalog.windows(2).any(|pair| pair[0].name == pair[1].name)
        {
            return Err(ToolSearchError::InvalidCatalog);
        }
        let fields = catalog
            .iter()
            .map(|tool| {
                let namespace = tool
                    .name
                    .rsplit_once("__")
                    .or_else(|| tool.name.rsplit_once('.'))
                    .map_or("", |(namespace, _)| namespace);
                let keywords = tool
                    .parameters
                    .get("keywords")
                    .map_or_else(String::new, serde_json::Value::to_string);
                vec![
                    tool_search_tokens(&tool.name),
                    tool_search_tokens(namespace),
                    tool_search_tokens(&keywords),
                    tool_search_tokens(&tool.description),
                    tool_search_tokens(&tool.parameters.to_string()),
                ]
            })
            .collect();
        Ok(Self {
            generation,
            catalog,
            fields,
        })
    }
    pub fn search(&self, query: &ToolSearchQuery) -> Result<ToolSearchResult, ToolSearchError> {
        let limit = query.limit.unwrap_or(5);
        let text = query.query.trim();
        if text.is_empty() || text.len() > 2048 || !(1..=TOOL_SEARCH_LIMIT).contains(&limit) {
            return Err(ToolSearchError::InvalidQuery);
        }
        let exact = text.strip_prefix("select:");
        let candidates = if let Some(names) = exact {
            let names = names.split(',').collect::<BTreeSet<_>>();
            if names.is_empty() || names.len() > TOOL_SEARCH_LIMIT {
                return Err(ToolSearchError::InvalidQuery);
            }
            let mut selected = Vec::new();
            for name in names {
                let index = self
                    .catalog
                    .binary_search_by(|tool| tool.name.as_str().cmp(name))
                    .map_err(|_| ToolSearchError::UnknownName(name.into()))?;
                selected.push(index);
            }
            selected
        } else if text == "list" {
            (0..self.catalog.len()).collect()
        } else {
            let terms = tool_search_tokens(text);
            let mut scores = self
                .fields
                .iter()
                .enumerate()
                .filter_map(|(i, fields)| {
                    let mut score = if self.catalog[i].name == text {
                        1_000_000
                    } else {
                        0
                    };
                    for (field, weight) in fields.iter().zip([64, 32, 24, 12, 8]) {
                        for term in terms.keys() {
                            if let Some(freq) = field.get(term) {
                                score += weight * (1 + (*freq).min(3));
                            }
                        }
                    }
                    (score > 0).then_some((i, score))
                })
                .collect::<Vec<_>>();
            scores.sort_by(|a, b| {
                b.1.cmp(&a.1)
                    .then_with(|| self.catalog[a.0].name.cmp(&self.catalog[b.0].name))
            });
            scores.into_iter().map(|(i, _)| i).collect()
        };
        let mut result=ToolSearchResult {schema_version:1,catalog_generation:self.generation.clone(),tools:vec![],has_more:candidates.len()>limit,dispatch_hint:"延迟工具须再次调用顶层 tool_search，arguments={name: canonical_name, arguments: 原工具参数}；只允许本 run 已发现的精确名称。".into()};
        for index in candidates.into_iter().take(limit) {
            result.tools.push(self.catalog[index].clone());
            if serde_json::to_vec(&result)?.len() > TOOL_SEARCH_BYTES {
                result.tools.pop();
                result.has_more = true;
                if exact.is_some() {
                    return Err(ToolSearchError::OutputBudget);
                }
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn spec(name: &str, description: &str, parameters: serde_json::Value) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }
    #[test]
    fn ranking_fields_chinese_english_exact_select_list_and_ties_are_stable() {
        let index = ToolSearchIndex::new(
            vec![
                spec(
                    "db__fetchInvoice2",
                    "读取发票金额",
                    json!({"properties":{"customerEmail":{"description":"billing contact"}}}),
                ),
                spec("a_shipping", "发送货物", json!({})),
                spec("b_shipping", "发送货物", json!({})),
            ],
            "g1".into(),
        )
        .unwrap();
        for query in [
            "发票",
            "invoice",
            "customer email",
            "billing",
            "db",
            "fetchInvoice2",
        ] {
            assert_eq!(
                index
                    .search(&ToolSearchQuery {
                        query: query.into(),
                        limit: Some(1)
                    })
                    .unwrap()
                    .tools[0]
                    .name,
                "db__fetchInvoice2"
            );
        }
        let result = index
            .search(&ToolSearchQuery {
                query: "shipping".into(),
                limit: Some(2),
            })
            .unwrap();
        assert_eq!(
            result
                .tools
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            ["a_shipping", "b_shipping"]
        );
        assert_eq!(
            index
                .search(&ToolSearchQuery {
                    query: "db__fetchInvoice2".into(),
                    limit: Some(1)
                })
                .unwrap()
                .tools[0]
                .name,
            "db__fetchInvoice2"
        );
        assert_eq!(
            index
                .search(&ToolSearchQuery {
                    query: "select:b_shipping,a_shipping".into(),
                    limit: Some(2)
                })
                .unwrap()
                .tools,
            result.tools
        );
        assert!(
            index
                .search(&ToolSearchQuery {
                    query: "select:unknown".into(),
                    limit: None
                })
                .is_err()
        );
        let list = index
            .search(&ToolSearchQuery {
                query: "list".into(),
                limit: Some(1),
            })
            .unwrap();
        assert_eq!(list.tools[0].name, "a_shipping");
        assert!(list.has_more);
        for text in ["snake_case", "kebab-case", "snakeCase", "snake2Case"] {
            let tokens = tool_search_tokens(text);
            assert!(tokens.contains_key("case"));
        }
    }
    #[test]
    fn strict_dispatch_and_output_budget_never_grant_an_omitted_schema() {
        for request in [
            json!({"query":"x","name":"y","arguments":{}}),
            json!({"name":"x","arguments":{},"owner":"fake"}),
            json!({"query":"x","limit":1,"generation":"fake"}),
        ] {
            assert!(serde_json::from_value::<ToolSearchRequest>(request).is_err());
        }
        let index = ToolSearchIndex::new(
            vec![spec(
                "large",
                "lookup",
                json!({"description":"x".repeat(TOOL_SEARCH_BYTES)}),
            )],
            "g".into(),
        )
        .unwrap();
        assert!(matches!(
            index.search(&ToolSearchQuery {
                query: "select:large".into(),
                limit: None
            }),
            Err(ToolSearchError::OutputBudget)
        ));
        assert!(
            index
                .search(&ToolSearchQuery {
                    query: "lookup".into(),
                    limit: None
                })
                .unwrap()
                .tools
                .is_empty()
        );
        assert!(
            ToolSearchIndex::new(
                vec![spec("x", "", json!({})), spec("x", "", json!({}))],
                "g".into()
            )
            .is_err()
        );
    }
}
