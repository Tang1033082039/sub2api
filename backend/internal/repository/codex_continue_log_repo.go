package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

func (r *usageLogRepository) CreateCodexContinueLog(ctx context.Context, log *service.CodexContinueLog) error {
	if log == nil || strings.TrimSpace(log.RequestID) == "" {
		return nil
	}
	rounds, err := json.Marshal(log.Rounds)
	if err != nil {
		return err
	}
	_, err = r.sql.ExecContext(ctx, `INSERT INTO codex_continue_logs (request_id, user_id, api_key_id, account_id, model, status, reason, rounds)
		VALUES ($1,$2,$3,$4,$5,$6,NULLIF($7,''),$8::jsonb)`, log.RequestID, log.UserID, log.APIKeyID, log.AccountID, log.Model, log.Status, log.Reason, rounds)
	return err
}

func (r *usageLogRepository) ListCodexContinueLogs(ctx context.Context, params pagination.PaginationParams, filters service.CodexContinueLogFilters) ([]service.CodexContinueLog, *pagination.PaginationResult, error) {
	page := params.Page
	if page < 1 {
		page = 1
	}
	pageSize := params.Limit()
	conditions := make([]string, 0, 6)
	args := make([]any, 0, 6)
	if filters.UserID > 0 {
		conditions = append(conditions, fmt.Sprintf("l.user_id = $%d", len(args)+1))
		args = append(args, filters.UserID)
	}
	if filters.AccountID > 0 {
		conditions = append(conditions, fmt.Sprintf("l.account_id = $%d", len(args)+1))
		args = append(args, filters.AccountID)
	}
	if value := strings.TrimSpace(filters.Status); value != "" {
		conditions = append(conditions, fmt.Sprintf("l.status = $%d", len(args)+1))
		args = append(args, value)
	}
	if value := strings.TrimSpace(filters.RequestID); value != "" {
		conditions = append(conditions, fmt.Sprintf("l.request_id = $%d", len(args)+1))
		args = append(args, value)
	}
	if filters.StartTime != nil {
		conditions = append(conditions, fmt.Sprintf("l.created_at >= $%d", len(args)+1))
		args = append(args, *filters.StartTime)
	}
	if filters.EndTime != nil {
		conditions = append(conditions, fmt.Sprintf("l.created_at < $%d", len(args)+1))
		args = append(args, *filters.EndTime)
	}
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	var total int64
	if err := r.sql.QueryRowContext(ctx, "SELECT COUNT(*) FROM codex_continue_logs l"+where, args...).Scan(&total); err != nil {
		return nil, nil, err
	}
	queryArgs := append(append([]any{}, args...), pageSize, (page-1)*pageSize)
	query := fmt.Sprintf("SELECT l.id,l.request_id,l.user_id,l.api_key_id,l.account_id,l.model,l.status,COALESCE(l.reason,''),l.rounds,l.created_at FROM codex_continue_logs l%s ORDER BY l.created_at DESC,l.id DESC LIMIT $%d OFFSET $%d", where, len(args)+1, len(args)+2)
	rows, err := r.sql.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	items := make([]service.CodexContinueLog, 0)
	for rows.Next() {
		var item service.CodexContinueLog
		var rounds []byte
		if err := rows.Scan(&item.ID, &item.RequestID, &item.UserID, &item.APIKeyID, &item.AccountID, &item.Model, &item.Status, &item.Reason, &rounds, &item.CreatedAt); err != nil {
			return nil, nil, err
		}
		if err := json.Unmarshal(rounds, &item.Rounds); err != nil {
			return nil, nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return items, &pagination.PaginationResult{Total: total, Page: page, PageSize: pageSize, Pages: int((total + int64(pageSize) - 1) / int64(pageSize))}, nil
}

var _ service.CodexContinueLogRepository = (*usageLogRepository)(nil)
