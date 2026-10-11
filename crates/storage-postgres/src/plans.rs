//! 原计划document/identity/version/decision在PostgreSQL中的同事务实现。
use crate::canonical::{field, unsigned};
use crate::sessions::encode;
use crate::{SCOPE, parameters, scope_values, unavailable};
use agent_core::*;
use tokio_postgres::Transaction;
pub(crate) fn validate_document(doc: &PlanDocument) -> Result<(), CoordinationError> {
    agent_storage_ports::validate_plan_document(doc)
}
pub(crate) async fn document_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    life: &SessionLifetimeId,
) -> Result<PlanDocument, CoordinationError> {
    let mut values = scope_values(scope);
    values.push(life.0.clone());
    let row=db.query_opt(&format!("SELECT revision,CASE WHEN octet_length(data_json)<=1048576 THEN data_json END FROM session_plans WHERE {SCOPE} AND lifetime=$5"),&parameters(&values)).await.map_err(unavailable)?.ok_or(CoordinationError::Conflict)?;
    let doc: PlanDocument =
        serde_json::from_str(&field(&row, 1)?).map_err(|_| CoordinationError::Corrupt)?;
    validate_document(&doc)?;
    if doc.revision != unsigned(row.try_get(0).map_err(unavailable)?)? {
        return Err(CoordinationError::Corrupt);
    }
    Ok(doc)
}
pub(crate) fn identity_matches(
    doc: &PlanDocument,
    id: &PlanExecution,
) -> Result<(), CoordinationError> {
    if id.operation_id.is_empty()
        || id.operation_id.len() > 128
        || id.operation_id.chars().any(char::is_control)
    {
        return Err(CoordinationError::InvalidRequest);
    }
    if doc.plan_id != id.plan_id
        || doc.revision != id.revision
        || doc.content_digest != id.content_digest
    {
        return Err(CoordinationError::Conflict);
    }
    Ok(())
}
pub(crate) async fn write_document(
    db: &Transaction<'_>,
    scope: &TenantContext,
    life: &SessionLifetimeId,
    doc: &PlanDocument,
) -> Result<(), CoordinationError> {
    validate_document(doc)?;
    let raw = encode(doc, 1048576)?;
    let markdown = doc.markdown();
    let mut values = scope_values(scope);
    values.extend([life.0.clone(), doc.revision.to_string()]);
    if let Some(old)=db.query_opt(&format!("SELECT plan_id,content_digest FROM plan_versions WHERE {SCOPE} AND lifetime=$5 AND revision=$6::text::bigint"),&parameters(&values)).await.map_err(unavailable)?{if old.try_get::<_,String>(0).map_err(unavailable)?!=doc.plan_id||old.try_get::<_,String>(1).map_err(unavailable)?!=doc.content_digest{return Err(CoordinationError::Conflict);}}
    values.extend([
        doc.plan_id.clone(),
        doc.content_digest.clone(),
        raw.clone(),
        markdown,
    ]);
    db.execute("INSERT INTO plan_versions VALUES($1,$2,$3,$4,$5,$6::text::bigint,$7,$8,$9,$10) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,lifetime,revision) DO UPDATE SET document_json=excluded.document_json,markdown=excluded.markdown",&parameters(&values)).await.map_err(unavailable)?;
    let mut values = scope_values(scope);
    values.extend([life.0.clone(), doc.revision.to_string(), raw]);
    db.execute("INSERT INTO session_plans VALUES($1,$2,$3,$4,$5,$6::text::bigint,$7) ON CONFLICT(organization_id,tenant_id,project_id,agent_uid,lifetime) DO UPDATE SET revision=excluded.revision,data_json=excluded.data_json",&parameters(&values)).await.map_err(unavailable)?;
    Ok(())
}
pub(crate) async fn receipt_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    meta: &SessionMetadata,
    id: &PlanExecution,
    input: &str,
) -> Result<Option<PlanDecisionReceipt>, CoordinationError> {
    decision_receipt_in(db, scope, meta, id, PlanDecision::Execute, input).await
}

pub(crate) async fn decision_receipt_in(
    db: &Transaction<'_>,
    scope: &TenantContext,
    meta: &SessionMetadata,
    id: &PlanExecution,
    decision: PlanDecision,
    input: &str,
) -> Result<Option<PlanDecisionReceipt>, CoordinationError> {
    let mut values = scope_values(scope);
    values.push(id.operation_id.clone());
    let row=db.query_opt(&format!("SELECT CASE WHEN octet_length(receipt_json)<=65536 THEN receipt_json END,CASE WHEN octet_length(input)<=65536 THEN input END,CASE WHEN octet_length(identity_json)<=16384 THEN identity_json END,decision,CASE WHEN octet_length(session_id) BETWEEN 1 AND 1024 THEN session_id END,CASE WHEN octet_length(lifetime) BETWEEN 1 AND 128 THEN lifetime END,run_id IS NULL,CASE WHEN octet_length(run_id) BETWEEN 1 AND 1024 THEN run_id END FROM plan_decisions WHERE {SCOPE} AND operation_id=$5"),&parameters(&values)).await.map_err(unavailable)?;
    if let Some(row) = row {
        let receipt: PlanDecisionReceipt =
            serde_json::from_str(&field(&row, 0)?).map_err(|_| CoordinationError::Corrupt)?;
        let saved: PlanExecution =
            serde_json::from_str(&field(&row, 2)?).map_err(|_| CoordinationError::Corrupt)?;
        let sql_decision = match row.try_get::<_, String>(3).map_err(unavailable)?.as_str() {
            "execute" => PlanDecision::Execute,
            "discard" => PlanDecision::Discard,
            _ => return Err(CoordinationError::Corrupt),
        };
        if receipt.identity != saved
            || receipt.identity.operation_id != id.operation_id
            || receipt.session_key.0 != field(&row, 4)?
            || receipt.lifetime.0 != field(&row, 5)?
            || receipt.decision != sql_decision
            || (sql_decision == PlanDecision::Discard
                && (!row.try_get::<_, bool>(6).map_err(unavailable)?
                    || !field(&row, 1)?.is_empty()))
            || (sql_decision == PlanDecision::Execute
                && (row.try_get::<_, bool>(6).map_err(unavailable)?
                    || receipt.run_id.as_ref().map(|run| run.0.as_str())
                        != Some(field(&row, 7)?.as_str())))
        {
            return Err(CoordinationError::Corrupt);
        }
        if let Some(run) = &receipt.run_id {
            let owner = crate::identities::exact_owner_in(db, scope, run)
                .await
                .map_err(|error| {
                    if error == CoordinationError::Forbidden {
                        CoordinationError::Corrupt
                    } else {
                        error
                    }
                })?;
            if owner.session_key != receipt.session_key
                || owner.session_lifetime_id != receipt.lifetime
            {
                return Err(CoordinationError::Corrupt);
            }
        }
        if receipt.session_key != meta.key
            || receipt.lifetime != meta.lifetime
            || receipt.identity != *id
            || receipt.decision != decision
            || field(&row, 1)? != input
        {
            return Err(CoordinationError::Conflict);
        }
        Ok(Some(receipt))
    } else {
        Ok(None)
    }
}
pub(crate) async fn admit_execution(
    db: &Transaction<'_>,
    env: &EnterpriseOwnerEnvelope,
    meta: &SessionMetadata,
    id: &PlanExecution,
    input: &str,
) -> Result<(), CoordinationError> {
    let mut doc = document_in(db, &env.scope, &meta.lifetime).await?;
    identity_matches(&doc, id)?;
    if !matches!(
        doc.review,
        PlanReviewStatus::PendingReview | PlanReviewStatus::PendingExecution
    ) {
        return Err(CoordinationError::Conflict);
    }
    doc.review = PlanReviewStatus::PendingExecution;
    write_document(db, &env.scope, &meta.lifetime, &doc).await?;
    let receipt = PlanDecisionReceipt {
        session_key: meta.key.clone(),
        lifetime: meta.lifetime.clone(),
        identity: id.clone(),
        decision: PlanDecision::Execute,
        run_id: Some(env.owner.run_id.clone()),
    };
    let mut values = scope_values(&env.scope);
    values.extend([
        id.operation_id.clone(),
        meta.key.0.clone(),
        meta.lifetime.0.clone(),
        encode(id, 16384)?,
        input.into(),
        env.owner.run_id.0.clone(),
        encode(&receipt, 65536)?,
    ]);
    db.execute(
        "INSERT INTO plan_decisions VALUES($1,$2,$3,$4,$5,$6,$7,$8,'execute',$9,$10,$11)",
        &parameters(&values),
    )
    .await
    .map_err(unavailable)?;
    Ok(())
}
