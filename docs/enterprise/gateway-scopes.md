# 多企业 Gateway 配置

`AGENT_OIDC_CONFIG`、`AGENT_GATEWAY_TOKEN`和启动workspace形成default profile。`AGENT_GATEWAY_SCOPES`指定受保护绝对路径JSON，格式见gateway-scopes.example.json；配置文件必须普通文件、0600、不跟随symlink。每profile绑定一个完整organization/tenant/project/agent scope、固定gateway_subject及独立服务凭据。最多16个profile，每profile最多16运行端点；端点RPC最终核验scope和runtime_endpoint。

用户JWT的scope仅作选择已配置verifier的提示，随后完整验签和数据库最终授权。API key必须在非默认scope传`X-Agent-Scope: tenant_b`；WebSocket首帧connect可带`scope: tenant_b`。显式profile与token不匹配统一401，客户端不能传可信身份或宿主目录。

额外profile的OAuth redirect_uri必须包含`?scope=tenant_b`，访问`/auth/login?scope=tenant_b`；登录cookie按别名隔离，state/nonce/PKCE消费仍由该scope的原事务决定。配置变更需重启Gateway，运行端点表和服务token每operation受保护读取；目录在canonical数据库永久绑定端点，不随Gateway重启改变。

本机两tenant/三daemon/单Gateway HTTP+APIkey+重启冒烟通过。真实远程IdP、multi-Gateway压力及WebSocket多profile专项待验收；没有生产部署。
