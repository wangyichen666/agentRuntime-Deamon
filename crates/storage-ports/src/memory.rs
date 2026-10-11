//! 两后端共用的保守摘录和凭据过滤，不改写 canonical transcript。
use agent_core::{Message, Role};

const EPISODE_BYTES: usize = 16 * 1024;
const OMITTED: &str = "\n[会话摘录已截断；完整证据保留在 transcript 中]\n";

/// 保守过滤自动摘录中的常见凭据行；不是全面 DLP，不改写原始证据。
fn redact_episode(content: &str) -> String {
    redact_memory_evidence(content, 1001)
}
pub fn redact_memory_evidence(content: &str, char_limit: usize) -> String {
    let mut output = String::new();
    let mut remaining_chars = char_limit;
    let mut removed = false;
    let mut private_block = false;
    for line in content.lines() {
        let lower = line.to_ascii_lowercase();
        if lower.contains("-----begin ") && lower.contains("private key-----") {
            private_block = true;
        }
        let credential = private_block
            || [
                "api_key",
                "api key:",
                "authorization:",
                "bearer ",
                "password=",
                "password:",
                "password =",
                "passwd=",
                "client_secret",
                "secret=",
                "secret:",
                "token=",
                "token:",
                "token =",
                "sk-",
                "ghp_",
                "github_pat_",
                "akia",
                "aws_secret_access_key",
            ]
            .iter()
            .any(|marker| lower.contains(marker))
            || (lower.contains("://") && lower.contains('@'));
        if credential {
            removed = true;
        } else {
            if !output.is_empty() && remaining_chars > 0 {
                output.push('\n');
                remaining_chars -= 1;
            }
            for character in line.chars().take(remaining_chars) {
                output.push(character);
                remaining_chars -= 1;
            }
            if remaining_chars == 0 {
                break;
            }
        }
        if private_block && lower.contains("-----end ") && lower.contains("private key-----") {
            private_block = false;
        }
    }
    if removed && !output.trim().is_empty() {
        output.push_str("\n[凭据行已过滤]");
    }
    output
}

/// 摘录不是模型提炼。字节与单消息字符双重上限，永不产生无标记的截断。
#[derive(Default)]
pub struct MemoryEpisodeExcerpt {
    pub text: String,
    pub source_message_ids: Vec<String>,
    pub full: bool,
}
impl MemoryEpisodeExcerpt {
    pub fn push(&mut self, message: &Message, source: String) {
        if self.full
            || !matches!(message.role, Role::User | Role::Assistant)
            || !message.tool_calls.is_empty()
        {
            return;
        }
        let Some(content) = message.content.as_deref().filter(|s| !s.trim().is_empty()) else {
            return;
        };
        let content = redact_episode(content);
        if content.trim().is_empty() {
            return;
        }
        let mut chars = content.chars();
        let excerpt: String = chars.by_ref().take(1000).collect();
        let clipped = chars.next().is_some();
        let line = format!(
            "{:?}: {excerpt}{}\n",
            message.role,
            if clipped { " [该消息已截断]" } else { "" }
        );
        let remaining = EPISODE_BYTES.saturating_sub(OMITTED.len() + self.text.len());
        if line.len() <= remaining {
            self.text.push_str(&line);
            self.source_message_ids.push(source);
        } else {
            let mut end = remaining.min(line.len());
            while !line.is_char_boundary(end) {
                end -= 1;
            }
            if end > 0 {
                self.text.push_str(&line[..end]);
                self.source_message_ids.push(source);
            }
            self.text.push_str(OMITTED);
            self.full = true;
        }
    }
}
