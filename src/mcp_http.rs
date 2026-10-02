//! Streamable HTTP：每个请求重新审查 DNS 并 pin 地址，禁用代理与重定向。
use crate::mcp::{RemoteTool, parse_remote_tools};
use anyhow::{Context, Result, bail};
use reqwest::{Client, Url};
use serde_json::{Value, json};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;
const PROTOCOL: &str = "2025-03-26";
const MAX_RESPONSE: usize = 256 * 1024;

pub(crate) struct HttpMcpClient {
    endpoint: Url,
    bearer: Option<String>,
    allow_private: bool,
    session: Mutex<Option<String>>,
    next_id: AtomicU64,
    pending: tokio::sync::Semaphore,
    #[cfg(test)]
    root_ca: Option<reqwest::Certificate>,
}
fn validate_endpoint(url: &Url) -> Result<()> {
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
    {
        bail!("远程 MCP 要求无凭据、无 query 的 HTTPS URL");
    }
    Ok(())
}
fn public_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_broadcast()
                && ip.octets()[0] != 0
                && ip.octets()[0] < 224
                && !ip.is_documentation()
                && !(ip.octets()[0] == 198 && (18..=19).contains(&ip.octets()[1]))
                && !(ip.octets()[0] == 192 && ip.octets()[1] == 0 && ip.octets()[2] == 0)
                && !(ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1]))
        }
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map_or_else(
            || {
                !ip.is_loopback()
                    && !ip.is_unspecified()
                    && !ip.is_multicast()
                    && !ip.is_unique_local()
                    && !ip.is_unicast_link_local()
                    && ip.segments()[0] & 0xe000 == 0x2000
                    && !(ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8)
            },
            |ip| public_address(IpAddr::V4(ip)),
        ),
    }
}
impl HttpMcpClient {
    pub(crate) async fn connect(
        url: &str,
        bearer_env: Option<&str>,
        allow_private: bool,
    ) -> Result<(Arc<Self>, Vec<RemoteTool>)> {
        let endpoint = Url::parse(url).context("MCP endpoint URL 无效")?;
        validate_endpoint(&endpoint)?;
        let bearer = bearer_env
            .map(crate::secrets::environment_secret)
            .transpose()?;
        let client = Arc::new(Self {
            endpoint,
            bearer,
            allow_private,
            session: Mutex::new(None),
            next_id: AtomicU64::new(1),
            pending: tokio::sync::Semaphore::new(64),
            #[cfg(test)]
            root_ca: None,
        });
        let initialized=client.request("initialize",json!({"protocolVersion":PROTOCOL,"capabilities":{},"clientInfo":{"name":"my-agent","version":env!("CARGO_PKG_VERSION")}})).await?;
        if initialized.get("protocolVersion").and_then(Value::as_str) != Some(PROTOCOL) {
            bail!("远程 MCP protocolVersion 不兼容");
        }
        client
            .post(
                json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                None,
            )
            .await?;
        let tools = parse_remote_tools(&client.request("tools/list", json!({})).await?)?;
        Ok((client, tools))
    }
    async fn pinned_client(&self) -> Result<Client> {
        validate_endpoint(&self.endpoint)?;
        let host = self.endpoint.host_str().context("缺少 MCP host")?;
        let port = self
            .endpoint
            .port_or_known_default()
            .context("缺少 MCP port")?;
        let addresses = tokio::time::timeout(
            Duration::from_secs(3),
            tokio::net::lookup_host((host, port)),
        )
        .await
        .context("MCP DNS 超时")?
        .context("MCP DNS 解析失败")?
        .take(65)
        .collect::<Vec<SocketAddr>>();
        if addresses.len() > 64
            || addresses.is_empty()
            || (!self.allow_private && addresses.iter().any(|a| !public_address(a.ip())))
        {
            bail!("MCP DNS 包含禁止的非公网地址；如需私网必须在该 server 配置显式授权");
        }
        let builder = Client::builder()
            .no_proxy()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .resolve_to_addrs(host, &addresses);
        #[cfg(test)]
        let builder = match &self.root_ca {
            Some(cert) => builder.add_root_certificate(cert.clone()),
            None => builder,
        };
        builder.build().context("创建 MCP HTTPS client 失败")
    }
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.post(
            json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
            Some(id),
        )
        .await
    }
    async fn post(&self, message: Value, id: Option<u64>) -> Result<Value> {
        let _permit = self
            .pending
            .try_acquire()
            .context("MCP HTTP pending 请求预算已满")?;
        let client = self.pinned_client().await?;
        let mut request = client
            .post(self.endpoint.clone())
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", PROTOCOL)
            .json(&message);
        if let Some(bearer) = &self.bearer {
            request = request.bearer_auth(bearer);
        }
        if let Some(session) = self.session.lock().await.clone() {
            request = request.header("Mcp-Session-Id", session);
        }
        let mut response = request
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("MCP HTTP 传输失败，已隐藏 URL 和凭据"))?;
        if response.status().is_redirection() {
            bail!("MCP redirect 被拒绝");
        }
        if !response.status().is_success() {
            bail!("MCP HTTP 返回状态 {}", response.status().as_u16());
        }
        if let Some(session) = response.headers().get("Mcp-Session-Id") {
            let session = session.to_str().context("MCP session header 非法")?;
            if session.len() > 256 {
                bail!("MCP session header 超出预算");
            }
            *self.session.lock().await = Some(session.into());
        }
        if id.is_none()
            && matches!(
                response.status(),
                reqwest::StatusCode::ACCEPTED | reqwest::StatusCode::NO_CONTENT
            )
        {
            return Ok(Value::Null);
        }
        let sse = response
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .is_some_and(|t| t.starts_with("text/event-stream"));
        if response
            .content_length()
            .is_some_and(|n| n > MAX_RESPONSE as u64)
        {
            bail!("MCP 响应超过预算");
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| anyhow::anyhow!("MCP 响应流中断"))?
        {
            if bytes.len() + chunk.len() > MAX_RESPONSE {
                bail!("MCP 响应超过预算");
            }
            bytes.extend_from_slice(&chunk);
            if sse {
                if let Some(value) = sse_result(&bytes, id)? {
                    return Ok(value);
                }
            }
        }
        if sse {
            bail!("MCP SSE 未取得完整匹配回执");
        }
        rpc_result(
            serde_json::from_slice(&bytes).context("MCP 响应不是完整 JSON")?,
            id,
        )
    }
    pub(crate) async fn call_tool(&self, name: &str, args: Value) -> Result<String> {
        let result = self
            .request("tools/call", json!({"name":name,"arguments":args}))
            .await
            .map_err(|_| agent_core::ToolOutcomeUnknown("MCP HTTP 调用未取得可信回执".into()))?;
        let content = result
            .get("content")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|item| {
                        item.get("text")
                            .and_then(Value::as_str)
                            .map_or_else(|| item.to_string(), str::to_owned)
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_else(|| result.to_string());
        if result.get("isError").and_then(Value::as_bool) == Some(true) {
            bail!("MCP 工具已确认返回错误：{content}");
        }
        Ok(content)
    }
    pub(crate) async fn shutdown(&self) {
        if let Ok(client) = self.pinned_client().await {
            if let Some(session) = self.session.lock().await.take() {
                let mut request = client
                    .delete(self.endpoint.clone())
                    .header("Mcp-Session-Id", session)
                    .header("MCP-Protocol-Version", PROTOCOL);
                if let Some(bearer) = &self.bearer {
                    request = request.bearer_auth(bearer);
                }
                let _ = request.send().await;
            }
        }
    }
}
fn sse_result(bytes: &[u8], id: Option<u64>) -> Result<Option<Value>> {
    let valid = match std::str::from_utf8(bytes) {
        Ok(text) => text,
        Err(e) if e.error_len().is_none() => std::str::from_utf8(&bytes[..e.valid_up_to()])?,
        Err(_) => bail!("MCP SSE UTF-8 无效"),
    };
    let text = valid.replace("\r\n", "\n");
    let Some(last) = text.rfind("\n\n") else {
        return Ok(None);
    };
    for event in text[..last].split("\n\n") {
        let data = event
            .lines()
            .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
            .collect::<Vec<_>>()
            .join("\n");
        if data.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&data).context("MCP SSE event 非法")?;
        if value.get("id").and_then(Value::as_u64) == id {
            return rpc_result(value, id).map(Some);
        }
    }
    Ok(None)
}
fn rpc_result(value: Value, id: Option<u64>) -> Result<Value> {
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || value.get("id").and_then(Value::as_u64) != id
    {
        bail!("MCP JSON-RPC 响应身份不匹配");
    }
    if value.get("error").is_some() {
        bail!("MCP JSON-RPC 返回错误，已隐藏上游正文");
    }
    value.get("result").cloned().context("MCP 响应缺少 result")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_tls_downgrade_credentials_redirect_destinations_and_private_dns() {
        for url in [
            "http://example.com/mcp",
            "https://user:secret@example.com/mcp",
            "https://example.com/mcp?key=secret",
        ] {
            assert!(validate_endpoint(&Url::parse(url).unwrap()).is_err());
        }
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.169.254",
            "100.64.1.1",
            "::1",
            "::ffff:127.0.0.1",
            "fe80::1",
        ] {
            assert!(!public_address(ip.parse().unwrap()));
        }
        assert!(public_address("8.8.8.8".parse().unwrap()));
        assert!(rpc_result(json!({"jsonrpc":"2.0","id":2,"result":{}}), Some(1)).is_err());
    }
    async fn tls_fixture(
        body: String,
        status: &str,
        content_type: &str,
    ) -> (HttpMcpClient, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio_rustls::rustls::{ServerConfig, pki_types::PrivatePkcs8KeyDer};
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into(),
            )
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nMcp-Session-Id: tls-session\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(socket).await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut chunk = [0; 1024];
                let n = stream.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..n]);
                if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let header = String::from_utf8_lossy(&request[..end]).to_lowercase();
                    let length = header
                        .lines()
                        .find_map(|l| {
                            l.strip_prefix("content-length:")
                                .and_then(|s| s.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
                assert!(request.len() <= MAX_RESPONSE);
            }
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
            String::from_utf8(request).unwrap()
        });
        (
            HttpMcpClient {
                endpoint: Url::parse(&format!("https://localhost:{port}/mcp")).unwrap(),
                bearer: None,
                allow_private: true,
                session: Mutex::new(None),
                next_id: AtomicU64::new(1),
                pending: tokio::sync::Semaphore::new(64),
                root_ca: Some(reqwest::Certificate::from_der(cert.cert.der()).unwrap()),
            },
            server,
        )
    }
    #[tokio::test]
    async fn tls_json_sse_receipt_and_budget_contract() {
        let (client, server) = tls_fixture(
            r#"{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"确认"}]}}"#
                .into(),
            "200 OK",
            "application/json",
        )
        .await;
        assert_eq!(client.call_tool("read", json!({})).await.unwrap(), "确认");
        let request = server.await.unwrap();
        assert!(request.contains("tools/call"));
        assert_eq!(client.session.lock().await.as_deref(), Some("tls-session"));
        let (client, server) = tls_fixture(
            "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\r\n\r\n".into(),
            "200 OK",
            "text/event-stream",
        )
        .await;
        assert_eq!(
            client.request("tools/list", json!({})).await.unwrap(),
            json!({"ok":true})
        );
        server.await.unwrap();
        for (body, status) in [
            ("x".repeat(MAX_RESPONSE + 1), "200 OK"),
            ("{}".into(), "302 Found"),
            (r#"{"jsonrpc":"2.0","id":2,"result":{}}"#.into(), "200 OK"),
        ] {
            let (client, server) = tls_fixture(body, status, "application/json").await;
            let error = client
                .call_tool("side_effect", json!({}))
                .await
                .unwrap_err();
            assert!(
                error
                    .downcast_ref::<agent_core::ToolOutcomeUnknown>()
                    .is_some()
            );
            server.await.unwrap();
        }
        assert!(
            sse_result(
                b"data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}",
                Some(1)
            )
            .unwrap()
            .is_none()
        );
    }
}
