ALTER TABLE users
    ADD COLUMN IF NOT EXISTS codex_continue_max_continue INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS codex_continue_retry_max INTEGER NOT NULL DEFAULT 2,
    ADD COLUMN IF NOT EXISTS codex_continue_low_reasoning_floor INTEGER NOT NULL DEFAULT 150;

COMMENT ON COLUMN users.codex_continue_max_continue IS 'Codex截断续写轮数上限（0=不限制）';
COMMENT ON COLUMN users.codex_continue_retry_max IS 'Codex低推理重试次数上限（0=不限制）';
COMMENT ON COLUMN users.codex_continue_low_reasoning_floor IS 'Codex低推理重试下限阈值，单位reasoning_tokens（0=不设下限）';
