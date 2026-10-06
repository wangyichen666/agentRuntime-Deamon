//! ACP 连接协商与严格扩展身份；只保存连接能力，不拥有业务事实。
use agent_core::{
    ExactOwner, PlanExecution, ProjectionGeneration, SessionLifetimeId, TranscriptSeq,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

pub const ACP_NAMESPACE: &str = "my-agent";
// 明确版本的线协议指纹；改变 schema 必须换指纹。
pub const ACP_V2_FINGERPRINT: &str = "my-agent/acp-v2/control-schema-1";
pub const ACP_V2_CAPABILITIES: &[&str] =
    &["plan", "compact", "interaction", "readback", "management"];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AcpCapabilities {
    pub schema_version: u16,
    pub fingerprint: String,
    pub capabilities: Vec<String>,
}
#[derive(Clone, Debug, Default)]
pub struct AcpNegotiation {
    initialized: bool,
    capabilities: BTreeSet<String>,
}
impl AcpNegotiation {
    pub fn initialize(&mut self, params: &Value) -> Result<AcpCapabilities, String> {
        if self.initialized {
            return Err("ACP 物理连接已初始化，不允许重复或热变更".into());
        }
        validate_acp_fields("initialize", params, 2)?;
        if params["protocolVersion"] != 2 {
            return Err("当前 opt-in 连接只支持 ACP v2".into());
        }
        let capabilities = match namespace(params)? {
            Some(value) => {
                let offered: AcpCapabilities =
                    serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
                check_header(offered.schema_version, &offered.fingerprint)?;
                if offered.capabilities.len() > 16 {
                    return Err("ACP capabilities 超出预算".into());
                }
                let mut result = BTreeSet::new();
                for name in offered.capabilities {
                    if !ACP_V2_CAPABILITIES.contains(&name.as_str()) || !result.insert(name) {
                        return Err("未知或重复 ACP capability".into());
                    }
                }
                result
            }
            None => BTreeSet::new(),
        };
        self.capabilities = capabilities;
        self.initialized = true;
        Ok(self.advertisement())
    }
    pub fn advertisement(&self) -> AcpCapabilities {
        AcpCapabilities {
            schema_version: 1,
            fingerprint: ACP_V2_FINGERPRINT.into(),
            capabilities: self.capabilities.iter().cloned().collect(),
        }
    }
    pub fn require(&self, capability: Option<&str>) -> Result<(), String> {
        if !self.initialized {
            return Err("ACP 必须先初始化物理连接".into());
        }
        if let Some(capability) = capability
            && !self.capabilities.contains(capability)
        {
            return Err(format!("ACP capability 未协商：{capability}"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AcpControl {
    pub schema_version: u16,
    pub fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_lifetime: Option<SessionLifetimeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<ExactOwner>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_execution: Option<PlanExecution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<TranscriptSeq>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_projection_generation: Option<ProjectionGeneration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_revision: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved: Option<bool>,
}
impl AcpControl {
    pub fn parse(method: &str, params: &Value) -> Result<Self, String> {
        let raw = namespace(params)?.ok_or("ACP v2 操作缺少 my-agent 身份 metadata")?;
        let control: Self = serde_json::from_value(raw.clone()).map_err(|e| e.to_string())?;
        check_header(control.schema_version, &control.fingerprint)?;
        let allowed = match method {
            "session/new" => vec!["operation_id"],
            "session/close" | "session/delete" => vec!["expected_lifetime", "operation_id"],
            "session/prompt" => vec!["expected_lifetime", "plan_execution"],
            "session/resume" | "_my_agent/session/read" | "_my_agent/plan/read" => {
                vec!["expected_lifetime"]
            }
            "_my_agent/plan/execute" | "_my_agent/plan/discard" => {
                vec!["expected_lifetime", "plan_execution"]
            }
            "_my_agent/compact/start" => vec![
                "expected_lifetime",
                "operation_id",
                "expected_revision",
                "expected_projection_generation",
            ],
            "session/cancel" | "_my_agent/run/cancel" => vec!["expected_lifetime", "owner"],
            "_my_agent/interaction/respond" => vec![
                "expected_lifetime",
                "owner",
                "interaction_id",
                "interaction_revision",
                "approved",
            ],
            _ => return Err("未声明的 ACP 控制方法".into()),
        };
        let object = raw.as_object().ok_or("ACP metadata 必须为对象")?;
        for key in object.keys() {
            if !matches!(key.as_str(), "schema_version" | "fingerprint")
                && !allowed.contains(&key.as_str())
            {
                return Err(format!("该 ACP 方法不接受字段：{key}"));
            }
        }
        if method != "session/new"
            && control
                .expected_lifetime
                .as_ref()
                .is_none_or(|life| life.0.is_empty())
        {
            return Err("ACP 缺少 expected_lifetime".into());
        }
        if allowed.contains(&"operation_id")
            && control
                .operation_id
                .as_ref()
                .is_none_or(|id| id.is_empty() || id.len() > 256)
        {
            return Err("ACP 缺少有效 operation_id".into());
        }
        if method.starts_with("_my_agent/plan/")
            && method != "_my_agent/plan/read"
            && control.plan_execution.is_none()
        {
            return Err("ACP 缺少 exact plan identity".into());
        }
        if method == "_my_agent/compact/start"
            && (control.expected_revision.is_none()
                || control.expected_projection_generation.is_none())
        {
            return Err("ACP 缺少 compact source CAS".into());
        }
        if allowed.contains(&"owner") {
            let owner = control.owner.as_ref().ok_or("ACP 缺少 exact owner")?;
            if params["sessionId"].as_str() != Some(owner.session_key.0.as_str())
                || control.expected_lifetime.as_ref() != Some(&owner.session_lifetime_id)
                || owner.run_id.0.is_empty()
                || owner.run_generation.0 == 0
                || owner.turn_id.0.is_empty()
            {
                return Err("ACP exact owner 身份不一致".into());
            }
        }
        if method == "_my_agent/interaction/respond"
            && (control
                .interaction_id
                .as_ref()
                .is_none_or(|id| id.is_empty())
                || control
                    .interaction_revision
                    .is_none_or(|revision| revision < 0)
                || control.approved.is_none())
        {
            return Err("ACP 缺少 interaction revision/response".into());
        }
        Ok(control)
    }
}
fn check_header(schema: u16, fingerprint: &str) -> Result<(), String> {
    if schema != 1 || fingerprint != ACP_V2_FINGERPRINT {
        Err("ACP 控制 schema/fingerprint 不支持".into())
    } else {
        Ok(())
    }
}
fn namespace(params: &Value) -> Result<Option<&Value>, String> {
    match params.get("_meta") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(meta)) => Ok(meta.get(ACP_NAMESPACE)),
        _ => Err("ACP _meta 必须是对象".into()),
    }
}
/// 在 SDK 的容错 DTO 解码之前检查顶层身份，不吞未知字段。
pub fn validate_acp_fields(method: &str, params: &Value, version: u16) -> Result<(), String> {
    let allowed: &[&str] = match method {
        "initialize" if version == 1 => &[
            "protocolVersion",
            "clientCapabilities",
            "clientInfo",
            "_meta",
        ],
        "initialize" => &["protocolVersion", "capabilities", "info", "_meta"],
        "session/new" => &["cwd", "mcpServers", "additionalDirectories", "_meta"],
        "session/load" if version == 1 => &["sessionId", "cwd", "mcpServers", "_meta"],
        "session/resume" => &[
            "sessionId",
            "cwd",
            "mcpServers",
            "replayFrom",
            "additionalDirectories",
            "_meta",
        ],
        "session/prompt" => &["sessionId", "prompt", "_meta"],
        "session/list" => &["cwd", "cursor", "_meta"],
        "session/close" | "session/delete" | "session/cancel" => &["sessionId", "_meta"],
        method if method.starts_with("_my_agent/") => &["sessionId", "_meta"],
        _ => return Err(format!("ACP 方法不支持：{method}")),
    };
    let object = params.as_object().ok_or("ACP params 必须是对象")?;
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        return Err(format!("未知 ACP 字段：{key}"));
    }
    if version == 1 && namespace(params)?.is_some() {
        return Err("ACP v1 不接受 v2 控制 metadata".into());
    }
    Ok(())
}

/// permission 的传输请求 ID 关联已捕获 owner；未知字段不能变成授权。
pub fn decode_acp_permission(value: Value, owner: &ExactOwner) -> Result<Option<bool>, String> {
    decode_permission_reply(value, Some(owner))
}
/// 稳定 v1 的 permission response 不接受 v2 控制 metadata。
pub fn decode_acp_v1_permission(value: Value) -> Result<Option<bool>, String> {
    decode_permission_reply(value, None)
}
fn decode_permission_reply(
    value: Value,
    owner: Option<&ExactOwner>,
) -> Result<Option<bool>, String> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Answer {
        outcome: Outcome,
        #[serde(default, rename = "_meta")]
        meta: Option<Value>,
    }
    #[derive(Deserialize)]
    #[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
    enum Outcome {
        Selected {
            #[serde(rename = "optionId")]
            option_id: String,
            #[serde(default, rename = "_meta")]
            meta: Option<Value>,
        },
        Cancelled,
    }
    let answer: Answer = serde_json::from_value(value).map_err(|error| error.to_string())?;
    let outcome_meta = match &answer.outcome {
        Outcome::Selected { meta, .. } => meta.as_ref(),
        Outcome::Cancelled => None,
    };
    for meta in [answer.meta.as_ref(), outcome_meta].into_iter().flatten() {
        if !meta.is_object() {
            return Err("permission _meta 必须为对象".into());
        }
        if meta.get(ACP_NAMESPACE).is_some() {
            let owner =
                owner.ok_or_else(|| "ACP v1 不接受 v2 permission 控制 metadata".to_string())?;
            let control = AcpControl::parse(
                "session/cancel",
                &serde_json::json!({"sessionId":owner.session_key,"_meta":meta}),
            )?;
            if control.owner.as_ref() != Some(owner) {
                return Err("permission answer owner 冲突".into());
            }
        }
    }
    match answer.outcome {
        Outcome::Cancelled => Ok(None),
        Outcome::Selected { option_id, .. } => match option_id.as_str() {
            "allow_once" => Ok(Some(true)),
            "reject_once" => Ok(Some(false)),
            _ => Err("未知 permission option".into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn negotiation_is_connection_scoped_strict_and_immutable() {
        let mut state = AcpNegotiation::default();
        assert!(state.require(None).is_err());
        let params = json!({"protocolVersion":2,"info":{"name":"test","version":"1"},"_meta":{"my-agent":{"schema_version":1,"fingerprint":ACP_V2_FINGERPRINT,"capabilities":["plan","compact"]}}});
        assert_eq!(
            state.initialize(&params).unwrap().capabilities,
            vec!["compact", "plan"]
        );
        assert!(state.require(Some("plan")).is_ok());
        assert!(state.require(Some("interaction")).is_err());
        assert!(state.initialize(&params).is_err());
        let mut hot = params;
        hot["protocolVersion"] = json!(1);
        assert!(state.initialize(&hot).is_err());
        let mut fresh = AcpNegotiation::default();
        assert!(fresh.initialize(&hot).is_err());
        assert!(fresh.require(None).is_err());
        assert!(
            validate_acp_fields(
                "session/prompt",
                &json!({"sessionId":"s","prompt":[],"owner":"bad"}),
                2
            )
            .is_err()
        );
        assert!(
            validate_acp_fields(
                "session/prompt",
                &json!({"sessionId":"s","prompt":[],"_meta":{"my-agent":{}}}),
                1
            )
            .is_err()
        );
    }
    #[test]
    fn permission_answer_is_strict_and_popup_cancellation_is_no_decision() {
        assert_eq!(decode_acp_v1_permission(json!({"outcome":{"outcome":"selected","optionId":"allow_once","_meta":{"editor":{"display":true}}}})).unwrap(),Some(true));
        assert!(decode_acp_v1_permission(json!({"outcome":{"outcome":"selected","optionId":"allow_once","_meta":{"my-agent":{"owner":{}}}}})).is_err());
        assert!(decode_acp_v1_permission(json!({"outcome":{"outcome":"selected","optionId":"allow_once"},"_meta":{"my-agent":{}}})).is_err());

        let owner:ExactOwner=serde_json::from_value(json!({"session_key":"s","session_lifetime_id":"life","run_id":"r","run_generation":1,"turn_id":"t"})).unwrap();
        assert_eq!(
            decode_acp_permission(
                json!({"outcome":{"outcome":"selected","optionId":"allow_once"}}),
                &owner
            )
            .unwrap(),
            Some(true)
        );
        assert_eq!(
            decode_acp_permission(json!({"outcome":{"outcome":"cancelled"}}), &owner).unwrap(),
            None
        );
        assert!(
            decode_acp_permission(
                json!({"outcome":{"outcome":"selected","optionId":"allow_once","owner":"foreign"}}),
                &owner
            )
            .is_err()
        );
        assert!(
            decode_acp_permission(
                json!({"outcome":{"outcome":"selected","optionId":"future"}}),
                &owner
            )
            .is_err()
        );
        assert!(
            decode_acp_permission(
                json!({"outcome":{"outcome":"selected","optionId":"allow_once"},"approved":true}),
                &owner
            )
            .is_err()
        );
    }
    #[test]
    fn exact_metadata_rejects_unknown_identity_and_wrong_method_fields() {
        let params = json!({"sessionId":"s","_meta":{"my-agent":{"schema_version":1,"fingerprint":ACP_V2_FINGERPRINT,"expected_lifetime":"life"}}});
        assert!(AcpControl::parse("_my_agent/session/read", &params).is_ok());
        assert!(AcpControl::parse("_my_agent/compact/start", &params).is_err());
        let mut wrong = params.clone();
        wrong["_meta"]["my-agent"]["operation_id"] = json!("op");
        assert!(AcpControl::parse("_my_agent/session/read", &wrong).is_err());
        wrong = params;
        wrong["_meta"]["my-agent"]["future_owner"] = json!("foreign");
        assert!(AcpControl::parse("session/prompt", &wrong).is_err());
    }
}
