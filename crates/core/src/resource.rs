use crate::{ExactOwner, ResourceId};
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceRecord {
    pub id: ResourceId,
    pub owner: ExactOwner,
    pub state: String,
    pub cwd: String,
    pub sandbox_requested: String,
    pub sandbox_effective: String,
    pub process_identity: Option<String>,
    pub log_cursor: u64,
    pub terminal_reason: Option<String>,
}
