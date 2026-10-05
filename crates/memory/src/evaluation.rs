//! 可重复的离线质量门禁；样本是人工预期，不用运行成功自举标签。
use super::{MemoryError, rank, rank_with_feedback, render_context};
use agent_core::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Dataset {
    version: u32,
    cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Case {
    id: String,
    query: String,
    entries: Vec<Entry>,
    #[serde(default)]
    feedback: Vec<MemoryAssessment>,
    expected: Vec<String>,
    #[serde(default)]
    injected: Option<Vec<String>>,
    #[serde(default = "default_budget")]
    budget: usize,
    #[serde(default = "default_limit")]
    limit: usize,
}
fn default_budget() -> usize {
    100_000
}
fn default_limit() -> usize {
    20
}
#[derive(Deserialize)]
struct Entry {
    id: String,
    content: String,
    scope: MemoryScope,
    #[serde(default)]
    confirmed: bool,
    #[serde(default)]
    expires_at: Option<u64>,
    #[serde(default)]
    event_time: u64,
    #[serde(default)]
    digest: Option<String>,
    #[serde(default = "one")]
    repeat: usize,
}
fn one() -> usize {
    1
}
#[derive(Debug, Serialize)]
pub struct EvaluationCase {
    pub id: String,
    pub passed: bool,
    pub expected: Vec<String>,
    pub actual: Vec<String>,
    pub injected: Vec<String>,
    pub baseline: Vec<String>,
}
#[derive(Debug, Serialize)]
pub struct EvaluationReport {
    pub dataset_version: u32,
    pub policy: &'static str,
    pub total: usize,
    pub passed: usize,
    pub cases: Vec<EvaluationCase>,
}
impl EvaluationReport {
    pub fn is_passed(&self) -> bool {
        self.total > 0 && self.total == self.passed
    }
}

pub fn evaluate_builtin(
    mut measure: impl FnMut(&str) -> usize,
) -> Result<EvaluationReport, MemoryError> {
    let dataset: Dataset = serde_json::from_str(include_str!("../eval/recall-v1.json"))
        .map_err(|e| MemoryError(e.to_string()))?;
    let visibility = MemoryVisibility {
        lifetime: SessionLifetimeId("current".into()),
        project: "project".into(),
        allow_confirmed_global: true,
    };
    let mut cases = Vec::new();
    for case in dataset.cases {
        let entries = case
            .entries
            .into_iter()
            .map(|e| MemoryRecord {
                content_digest: e.digest.unwrap_or_else(|| e.id.clone()),
                id: e.id,
                content: e.content.repeat(e.repeat),
                scope: e.scope,
                layer: MemoryLayer::Semantic,
                kind: MemoryKind::Explicit,
                confirmed_by_user: e.confirmed,
                expires_at: e.expires_at,
                confidence: 70,
                event_time: e.event_time,
                created_at: 0,
                updated_at: 0,
                source: None,
                source_message_ids: vec![],
                revision: 0,
            })
            .collect::<Vec<_>>();
        let baseline = rank(entries.clone(), &visibility, &case.query, case.limit, 100)
            .into_iter()
            .map(|e| e.id)
            .collect();
        let ranked = rank_with_feedback(
            entries,
            &visibility,
            &case.query,
            case.limit,
            100,
            &case.feedback,
        );
        let actual = ranked.iter().map(|e| e.id.clone()).collect::<Vec<_>>();
        let rendered = render_context(
            &ranked,
            &visibility,
            100,
            case.limit,
            case.budget,
            &mut measure,
        )?;
        let injected = if let Some(rendered) = rendered {
            let records: Vec<MemoryExposure> =
                serde_json::from_str(rendered.split_once('\n').map_or("[]", |(_, json)| json))
                    .map_err(|e| MemoryError(e.to_string()))?;
            records.into_iter().map(|e| e.id).collect::<Vec<_>>()
        } else {
            vec![]
        };
        let passed = actual == case.expected
            && case
                .injected
                .as_ref()
                .is_none_or(|expected| expected == &injected);
        cases.push(EvaluationCase {
            id: case.id,
            passed,
            expected: case.expected,
            actual,
            injected,
            baseline,
        });
    }
    Ok(EvaluationReport {
        dataset_version: dataset.version,
        policy: super::RECALL_POLICY,
        total: cases.len(),
        passed: cases.iter().filter(|c| c.passed).count(),
        cases,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn independent_quality_dataset_is_a_release_gate() {
        let report = super::evaluate_builtin(str::len).unwrap();
        assert!(report.is_passed(), "{report:#?}");
    }
}
