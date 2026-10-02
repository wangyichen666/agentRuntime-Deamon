//! 配置仅保存环境引用或系统 Keychain 引用，密钥解析限于组合边界。
use anyhow::{Result, bail};
pub(crate) fn environment_secret(name: &str) -> Result<String> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        bail!("secret 环境变量引用无效");
    }
    std::env::var(name)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("secret 环境变量引用未配置"))
}
pub(crate) trait SecretStore: std::fmt::Debug + Send + Sync {
    fn put(&self, account: &str, value: &str) -> Result<()>;
    fn get(&self, account: &str) -> Result<String>;
}
#[derive(Debug)]
pub(crate) struct SystemSecretStore;
impl SecretStore for SystemSecretStore {
    fn put(&self, account: &str, value: &str) -> Result<()> {
        #[cfg(test)]
        {
            test_store().put(account, value)
        }
        #[cfg(all(not(test), target_os = "macos"))]
        {
            keyring::Entry::new("my-agent", account)
                .map_err(|_| anyhow::anyhow!("系统 Keychain 引用创建失败"))?
                .set_password(value)
                .map_err(|_| anyhow::anyhow!("系统 Keychain 写入失败"))
        }
        #[cfg(all(not(test), not(target_os = "macos")))]
        {
            let _ = (account, value);
            bail!("此平台未配置系统 secret store，请使用 env:NAME 引用");
        }
    }
    fn get(&self, account: &str) -> Result<String> {
        #[cfg(test)]
        {
            test_store().get(account)
        }
        #[cfg(all(not(test), target_os = "macos"))]
        {
            keyring::Entry::new("my-agent", account)
                .map_err(|_| anyhow::anyhow!("系统 Keychain 引用创建失败"))?
                .get_password()
                .map_err(|_| anyhow::anyhow!("系统 Keychain 读回失败"))
        }
        #[cfg(all(not(test), not(target_os = "macos")))]
        {
            let _ = account;
            bail!("系统 secret store 不可用，请使用 env:NAME 引用");
        }
    }
}
pub(crate) fn resolve(reference: &str) -> Result<String> {
    if let Some(name) = reference.strip_prefix("env:") {
        return environment_secret(name);
    }
    if let Some(account) = reference.strip_prefix("keychain:") {
        return SystemSecretStore.get(account);
    }
    // 仅用于未完成安全迁移的旧配置读取；新持久写入必须先迁移和验证。
    Ok(reference.into())
}
pub(crate) fn is_reference(value: &str) -> bool {
    value.starts_with("env:") || value.starts_with("keychain:")
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct TestSecretStore(std::sync::Mutex<std::collections::HashMap<String, String>>);
#[cfg(test)]
impl SecretStore for TestSecretStore {
    fn put(&self, account: &str, value: &str) -> Result<()> {
        self.0.lock().unwrap().insert(account.into(), value.into());
        Ok(())
    }
    fn get(&self, account: &str) -> Result<String> {
        self.0
            .lock()
            .unwrap()
            .get(account)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("未找到测试 secret"))
    }
}

#[cfg(test)]
fn test_store() -> &'static TestSecretStore {
    static STORE: std::sync::OnceLock<TestSecretStore> = std::sync::OnceLock::new();
    STORE.get_or_init(TestSecretStore::default)
}
