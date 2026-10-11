BEGIN IMMEDIATE;
-- 保留Local后台主体，只供服务端Cron准入，不作为可交互登录账号。
INSERT INTO principals VALUES('local-tenant','local-cron-workload','workload',0);
INSERT INTO role_bindings VALUES('local-tenant','local-project','local-agent','local-cron-workload','developer');
INSERT INTO schema_migrations VALUES(27,CAST(strftime('%s','now') AS INTEGER)*1000);
COMMIT;
