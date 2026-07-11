package service

import (
	"context"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
)

type CodexContinueLog struct {
	ID        int64                     `json:"id"`
	RequestID string                    `json:"request_id"`
	UserID    int64                     `json:"user_id"`
	APIKeyID  int64                     `json:"api_key_id"`
	AccountID int64                     `json:"account_id"`
	Model     string                    `json:"model"`
	Status    string                    `json:"status"`
	Reason    string                    `json:"reason"`
	Rounds    []CodexContinueTraceRound `json:"rounds"`
	CreatedAt time.Time                 `json:"created_at"`
}

type CodexContinueLogFilters struct {
	UserID    int64
	AccountID int64
	Status    string
	RequestID string
	StartTime *time.Time
	EndTime   *time.Time
}

type CodexContinueLogRepository interface {
	CreateCodexContinueLog(context.Context, *CodexContinueLog) error
	ListCodexContinueLogs(context.Context, pagination.PaginationParams, CodexContinueLogFilters) ([]CodexContinueLog, *pagination.PaginationResult, error)
}
