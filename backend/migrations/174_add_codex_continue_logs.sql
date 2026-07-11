-- db: postgresql
-- evidence: deploy/docker-compose.yml DATABASE_HOST=postgres
-- purpose: persist Codex continuation decisions for administrator diagnostics

CREATE TABLE IF NOT EXISTS codex_continue_logs (
    id BIGSERIAL PRIMARY KEY,
    request_id VARCHAR(64) NOT NULL,
    user_id BIGINT NOT NULL,
    api_key_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    model VARCHAR(100) NOT NULL,
    status VARCHAR(32) NOT NULL,
    reason VARCHAR(100),
    rounds JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE codex_continue_logs IS 'Codex 连续推理续写审计日志';
COMMENT ON COLUMN codex_continue_logs.request_id IS '网关请求 ID';
COMMENT ON COLUMN codex_continue_logs.user_id IS '用户 ID';
COMMENT ON COLUMN codex_continue_logs.api_key_id IS 'API Key ID';
COMMENT ON COLUMN codex_continue_logs.account_id IS '上游账号 ID';
COMMENT ON COLUMN codex_continue_logs.model IS '客户端请求模型';
COMMENT ON COLUMN codex_continue_logs.status IS '续写状态：continued/not_needed/failed';
COMMENT ON COLUMN codex_continue_logs.reason IS '续写判定或停止原因';
COMMENT ON COLUMN codex_continue_logs.rounds IS '每轮 reasoning token 审计明细';
COMMENT ON COLUMN codex_continue_logs.created_at IS '创建时间';

CREATE INDEX IF NOT EXISTS idx_codex_continue_logs_status_created_at
    ON codex_continue_logs (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_codex_continue_logs_user_created_at
    ON codex_continue_logs (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_codex_continue_logs_account_created_at
    ON codex_continue_logs (account_id, created_at DESC);
