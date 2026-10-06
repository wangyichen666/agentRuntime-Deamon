//! 计划定义、执行身份和同源 Markdown；进度和审阅不属于定义版本。
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    #[default]
    Pending,
    InProgress,
    Done,
}

impl std::fmt::Display for PlanStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Done => "done",
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanStep {
    pub id: String,
    pub description: String,
    #[serde(default)]
    pub status: PlanStatus,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanDefinition {
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub goal: String,
    #[serde(default)]
    pub success_criteria: Vec<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub verification: Vec<String>,
    pub steps: Vec<PlanStep>,
}

#[derive(Debug, thiserror::Error)]
pub enum PlanDefinitionError {
    #[error("计划定义无效：{0}")]
    Invalid(String),
    #[error("计划定义序列化失败：{0}")]
    Encoding(#[from] serde_json::Error),
}

impl PlanDefinition {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, PlanDefinitionError> {
        let mut ids = BTreeSet::new();
        if self.steps.len() > 256 {
            return Err(PlanDefinitionError::Invalid("步骤超过 256 项".into()));
        }
        for step in &self.steps {
            if step.id.trim().is_empty()
                || step.description.trim().is_empty()
                || !ids.insert(&step.id)
            {
                return Err(PlanDefinitionError::Invalid(
                    "步骤身份/描述为空或重复".into(),
                ));
            }
        }
        // 固定结构字段和有序步骤；不使用本地化显示、HashMap 顺序或进度。
        #[derive(Serialize)]
        struct Definition<'a> {
            title: &'a str,
            goal: &'a str,
            success_criteria: &'a [String],
            constraints: &'a [String],
            verification: &'a [String],
            steps: Vec<(&'a str, &'a str)>,
        }
        let bytes = serde_json::to_vec(&Definition {
            title: &self.title,
            goal: &self.goal,
            success_criteria: &self.success_criteria,
            constraints: &self.constraints,
            verification: &self.verification,
            steps: self
                .steps
                .iter()
                .map(|s| (s.id.as_str(), s.description.as_str()))
                .collect(),
        })?;
        if bytes.len() > 65536 {
            return Err(PlanDefinitionError::Invalid("定义超过 64 KiB".into()));
        }
        Ok(bytes)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanReviewStatus {
    PendingReview,
    PendingExecution,
    Executing,
    Rejected,
    Blocked,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanArtifact {
    pub content_digest: String,
    pub byte_length: usize,
    pub media_type: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanDocument {
    pub plan_id: String,
    pub revision: u64,
    pub content_digest: String,
    pub definition: PlanDefinition,
    pub review: PlanReviewStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<PlanArtifact>,
}

impl PlanDocument {
    pub fn validate(&self) -> Result<(), PlanDefinitionError> {
        if self.plan_id.is_empty()
            || self.revision == 0
            || self.content_digest.len() != 64
            || !self.content_digest.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(PlanDefinitionError::Invalid(
                "身份、版本或定义摘要不匹配".into(),
            ));
        }
        self.definition.canonical_bytes()?;
        Ok(())
    }

    pub fn markdown(&self) -> String {
        let mut out = format!("# {}\n\n{}\n", self.definition.title, self.definition.goal);
        for (heading, values) in [
            ("成功标准", &self.definition.success_criteria),
            ("约束", &self.definition.constraints),
            ("验证方式", &self.definition.verification),
        ] {
            if !values.is_empty() {
                out.push_str(&format!("\n## {heading}\n\n"));
                for value in values {
                    out.push_str(&format!("- {value}\n"));
                }
            }
        }
        out.push_str("\n## 步骤\n\n");
        for step in &self.definition.steps {
            let marker = match step.status {
                PlanStatus::Pending => " ",
                PlanStatus::InProgress => ">",
                PlanStatus::Done => "x",
            };
            out.push_str(&format!(
                "- [{marker}] {} · {}\n",
                step.id, step.description
            ));
        }
        out
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanExecution {
    pub plan_id: String,
    pub revision: u64,
    pub content_digest: String,
    pub operation_id: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanDecision {
    Execute,
    Discard,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlanDecisionReceipt {
    pub session_key: crate::SessionKey,
    pub lifetime: crate::SessionLifetimeId,
    pub identity: PlanExecution,
    pub decision: PlanDecision,
    pub run_id: Option<crate::RunId>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanReadback {
    pub session_key: crate::SessionKey,
    pub lifetime: crate::SessionLifetimeId,
    pub snapshot_revision: crate::SnapshotRevision,
    pub plan: Option<PlanDocument>,
    pub legacy_plan: Option<serde_json::Value>,
    pub markdown: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn digest_is_definition_only_and_order_sensitive() {
        let mut definition = PlanDefinition {
            title: "审计".into(),
            steps: vec![PlanStep {
                id: "a".into(),
                description: "验证".into(),
                status: PlanStatus::Pending,
            }],
            ..Default::default()
        };
        let digest = definition.canonical_bytes().unwrap();
        definition.steps[0].status = PlanStatus::Done;
        assert_eq!(digest, definition.canonical_bytes().unwrap());
        definition.verification.push("全量测试".into());
        assert_ne!(digest, definition.canonical_bytes().unwrap());
        definition.steps.push(PlanStep {
            id: "b".into(),
            description: "重启".into(),
            status: PlanStatus::Pending,
        });
        let ordered = definition.canonical_bytes().unwrap();
        definition.steps.reverse();
        assert_ne!(ordered, definition.canonical_bytes().unwrap());
        definition.steps.push(definition.steps[0].clone());
        assert!(definition.canonical_bytes().is_err());
    }
}
