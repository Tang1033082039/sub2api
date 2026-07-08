ALTER TABLE users
    ADD COLUMN IF NOT EXISTS codex_continue_enabled BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN users.codex_continue_enabled IS '是否为该用户启用 Codex 连续推理续写灰度功能';
