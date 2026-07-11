package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const (
	codexContinueTruncationStep = 518
	codexContinueMaxContinue    = 3
	codexContinueMinN           = 1
	codexContinueFirstRoundMin  = codexContinueTruncationStep - 2
	codexContinueMarkerText     = "Continue thinking..."
	openAISSEDone               = "[DONE]"
)

type codexContinueFoldResult struct {
	usage            *OpenAIUsage
	firstTokenMs     *int
	responseID       string
	imageCount       int
	imageOutputSizes []string
}

type codexContinueRound struct {
	roundNo        int
	reasoningItems []map[string]any
	bufferedEvents []codexContinueBufferedItem
	terminal       map[string]any
	usage          *OpenAIUsage
	rawUsage       map[string]any
	sawDone        bool
	sawTerminal    bool
}

type codexContinueBufferedItem struct {
	upstreamOutputIndex any
	itemType            string
	events              []map[string]any
	item                map[string]any
}

type codexContinueSeq struct {
	next int
}

func (s *codexContinueSeq) Take() int {
	n := s.next
	s.next++
	return n
}

type codexContinueUsageSum struct {
	InputTokens     int
	OutputTokens    int
	TotalTokens     int
	CachedTokens    int
	HasCachedTokens bool
	ReasoningTokens int
}

func shouldUseCodexContinueFold(c *gin.Context, account *Account, reqStream bool, isCodexCLI bool) bool {
	if !reqStream || !isCodexCLI || account == nil || account.Platform != PlatformOpenAI || (account.Type != AccountTypeOAuth && account.Type != AccountTypeAPIKey) {
		return false
	}
	if c == nil || isOpenAIResponsesCompactPath(c) {
		return false
	}
	apiKey := getAPIKeyFromContext(c)
	return apiKey != nil && apiKey.User != nil && apiKey.User.CodexContinueEnabled
}

func (s *OpenAIGatewayService) handleCodexContinueStreamingResponse(
	ctx context.Context,
	resp *http.Response,
	c *gin.Context,
	account *Account,
	baseBody []byte,
	token string,
	promptCacheKey string,
	isCodexCLI bool,
	startTime time.Time,
	originalModel string,
	upstreamModel string,
) (*openaiStreamingResult, error) {
	result, err := s.foldCodexContinueStream(ctx, resp, c, account, baseBody, token, promptCacheKey, isCodexCLI, startTime, originalModel, upstreamModel)
	if result == nil {
		return nil, err
	}
	return &openaiStreamingResult{
		usage:            result.usage,
		firstTokenMs:     result.firstTokenMs,
		responseID:       result.responseID,
		imageCount:       result.imageCount,
		imageOutputSizes: result.imageOutputSizes,
	}, err
}

func (s *OpenAIGatewayService) foldCodexContinueStream(
	ctx context.Context,
	resp *http.Response,
	c *gin.Context,
	account *Account,
	baseBody []byte,
	token string,
	promptCacheKey string,
	isCodexCLI bool,
	startTime time.Time,
	originalModel string,
	upstreamModel string,
) (*codexContinueFoldResult, error) {
	if s.responseHeaderFilter != nil {
		responseheaders.WriteFilteredHeaders(c.Writer.Header(), resp.Header, s.responseHeaderFilter)
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	if v := resp.Header.Get("x-request-id"); v != "" {
		c.Header("x-request-id", v)
	}

	w := c.Writer
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, errors.New("streaming not supported")
	}
	bufferedWriter := bufio.NewWriterSize(w, 4*1024)
	clientDisconnected := false
	flush := func() {
		if clientDisconnected {
			return
		}
		if err := bufferedWriter.Flush(); err != nil {
			clientDisconnected = true
			return
		}
		flusher.Flush()
	}
	writeEvent := func(ev map[string]any) {
		if clientDisconnected {
			return
		}
		payload, err := marshalOpenAIUpstreamJSON(ev)
		if err != nil {
			return
		}
		if _, err := bufferedWriter.WriteString("data: "); err != nil {
			clientDisconnected = true
			return
		}
		if _, err := bufferedWriter.Write(payload); err != nil {
			clientDisconnected = true
			return
		}
		if _, err := bufferedWriter.WriteString("\n\n"); err != nil {
			clientDisconnected = true
			return
		}
		flush()
	}
	writeDone := func() {
		if clientDisconnected {
			return
		}
		if _, err := bufferedWriter.WriteString("data: [DONE]\n\n"); err != nil {
			clientDisconnected = true
			return
		}
		flush()
	}

	base, err := decodeCodexContinueBaseBody(baseBody)
	if err != nil {
		return nil, err
	}
	originalInput := cloneCodexContinueInput(base["input"])

	seq := &codexContinueSeq{}
	downstreamOutputIndex := 0
	var firstTokenMs *int
	firstVisibleResponseID := ""
	var baseResponse map[string]any
	finalOutput := make([]any, 0)
	replayTail := make([]any, 0)
	roundsInfo := make([]map[string]any, 0, codexContinueMaxContinue+1)
	usageSum := &codexContinueUsageSum{}
	var firstRawUsage map[string]any
	var finalRawUsage map[string]any
	totalUsage := &OpenAIUsage{}
	imageCounter := newOpenAIImageOutputCounter()
	currentResp := resp

	for roundNo := 1; ; roundNo++ {
		round, readErr := s.readCodexContinueRound(ctx, currentResp, account, roundNo, seq, &downstreamOutputIndex, &baseResponse, &firstVisibleResponseID, writeEvent, imageCounter, originalModel, upstreamModel, startTime, &firstTokenMs)
		_ = currentResp.Body.Close()
		if round.usage != nil {
			addOpenAIUsage(totalUsage, round.usage)
		}
		usageSum.Add(round.rawUsage)
		if roundNo == 1 {
			firstRawUsage = cloneJSONMap(round.rawUsage)
		}
		if readErr != nil {
			if !round.sawTerminal {
				incompleteUsage := codexContinueAgentUsage(firstRawUsage, usageSum, nil, false)
				writeEvent(codexContinueSyntheticIncomplete(baseResponse, finalOutput, incompleteUsage, seq.Take(), "upstream_error", roundsInfo, usageSum.Raw()))
				return &codexContinueFoldResult{usage: totalUsage, firstTokenMs: firstTokenMs, responseID: firstVisibleResponseID, imageCount: imageCounter.Count(), imageOutputSizes: imageCounter.Sizes()}, nil
			}
			return &codexContinueFoldResult{usage: totalUsage, firstTokenMs: firstTokenMs, responseID: firstVisibleResponseID, imageCount: imageCounter.Count(), imageOutputSizes: imageCounter.Sizes()}, readErr
		}
		for _, item := range round.reasoningItems {
			finalOutput = append(finalOutput, cloneJSONMap(item))
		}

		reasoningTokens := codexContinueReasoningTokens(round.rawUsage)
		n := codexContinueTierN(reasoningTokens)
		roundsInfo = append(roundsInfo, map[string]any{"round": roundNo, "reasoning_tokens": reasoningTokens, "n": n})

		hasEncrypted := false
		if len(round.reasoningItems) > 0 {
			last := round.reasoningItems[len(round.reasoningItems)-1]
			_, hasEncrypted = last["encrypted_content"].(string)
		}
		doContinue := round.sawTerminal &&
			codexContinueShouldContinueRound(roundNo, reasoningTokens) &&
			hasEncrypted &&
			roundNo <= codexContinueMaxContinue

		stopReason := ""
		if !doContinue && codexContinueIsTruncationPattern(reasoningTokens) {
			switch {
			case !hasEncrypted:
				stopReason = "no_encrypted_content"
			case roundNo > codexContinueMaxContinue:
				stopReason = "max_continue"
			default:
				stopReason = "tier_out_of_window"
			}
		}

		if doContinue {
			for _, item := range round.reasoningItems {
				replayTail = append(replayTail, cloneJSONMap(item))
			}
			replayTail = append(replayTail, codexContinueCommentaryMessage(codexContinueMarkerText))

			nextBody, buildErr := buildCodexContinueRoundBody(base, originalInput, replayTail)
			if buildErr != nil {
				incompleteUsage := codexContinueAgentUsage(firstRawUsage, usageSum, round.rawUsage, false)
				writeEvent(codexContinueSyntheticIncomplete(baseResponse, finalOutput, incompleteUsage, seq.Take(), "build_continuation_failed", roundsInfo, usageSum.Raw()))
				return &codexContinueFoldResult{usage: totalUsage, firstTokenMs: firstTokenMs, responseID: firstVisibleResponseID, imageCount: imageCounter.Count(), imageOutputSizes: imageCounter.Sizes()}, nil
			}
			nextReq, buildReqErr := s.buildUpstreamRequest(ctx, c, account, nextBody, token, true, promptCacheKey, isCodexCLI)
			if buildReqErr != nil {
				incompleteUsage := codexContinueAgentUsage(firstRawUsage, usageSum, round.rawUsage, false)
				writeEvent(codexContinueSyntheticIncomplete(baseResponse, finalOutput, incompleteUsage, seq.Take(), "build_continuation_failed", roundsInfo, usageSum.Raw()))
				return &codexContinueFoldResult{usage: totalUsage, firstTokenMs: firstTokenMs, responseID: firstVisibleResponseID, imageCount: imageCounter.Count(), imageOutputSizes: imageCounter.Sizes()}, nil
			}
			proxyURL := ""
			if account.ProxyID != nil && account.Proxy != nil {
				proxyURL = account.Proxy.URL()
			}
			nextResp, doErr := s.httpUpstream.Do(nextReq, proxyURL, account.ID, account.Concurrency)
			if doErr != nil {
				incompleteUsage := codexContinueAgentUsage(firstRawUsage, usageSum, round.rawUsage, false)
				writeEvent(codexContinueSyntheticIncomplete(baseResponse, finalOutput, incompleteUsage, seq.Take(), "upstream_error", roundsInfo, usageSum.Raw()))
				return &codexContinueFoldResult{usage: totalUsage, firstTokenMs: firstTokenMs, responseID: firstVisibleResponseID, imageCount: imageCounter.Count(), imageOutputSizes: imageCounter.Sizes()}, nil
			}
			if nextResp.StatusCode >= 400 {
				body, _ := io.ReadAll(io.LimitReader(nextResp.Body, openAIUpstreamErrorBodyReadLimitForConfig(s.cfg)))
				_ = nextResp.Body.Close()
				logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Codex continuation failed account=%d status=%d body=%s", account.ID, nextResp.StatusCode, truncateString(string(body), 1024))
				incompleteUsage := codexContinueAgentUsage(firstRawUsage, usageSum, round.rawUsage, false)
				writeEvent(codexContinueSyntheticIncomplete(baseResponse, finalOutput, incompleteUsage, seq.Take(), "upstream_error", roundsInfo, usageSum.Raw()))
				return &codexContinueFoldResult{usage: totalUsage, firstTokenMs: firstTokenMs, responseID: firstVisibleResponseID, imageCount: imageCounter.Count(), imageOutputSizes: imageCounter.Sizes()}, nil
			}
			currentResp = nextResp
			continue
		}

		if !round.sawTerminal {
			incompleteUsage := codexContinueAgentUsage(firstRawUsage, usageSum, round.rawUsage, false)
			writeEvent(codexContinueSyntheticIncomplete(baseResponse, finalOutput, incompleteUsage, seq.Take(), "upstream_eof", roundsInfo, usageSum.Raw()))
			return &codexContinueFoldResult{usage: totalUsage, firstTokenMs: firstTokenMs, responseID: firstVisibleResponseID, imageCount: imageCounter.Count(), imageOutputSizes: imageCounter.Sizes()}, nil
		}

		for _, entry := range round.bufferedEvents {
			for _, ev := range entry.events {
				if _, ok := ev["output_index"]; ok {
					ev["output_index"] = downstreamOutputIndex
				}
				ev["sequence_number"] = seq.Take()
				if firstTokenMs == nil {
					ms := int(time.Since(startTime).Milliseconds())
					firstTokenMs = &ms
				}
				writeEvent(ev)
			}
			downstreamOutputIndex++
			finalOutput = append(finalOutput, cloneJSONMap(entry.item))
		}
		finalRawUsage = round.rawUsage
		agentUsage := codexContinueAgentUsage(firstRawUsage, usageSum, finalRawUsage, true)
		writeEvent(codexContinueTerminal(round.terminal, baseResponse, finalOutput, agentUsage, seq.Take(), roundsInfo, stopReason, usageSum.Raw()))
		if round.sawDone {
			writeDone()
		}
		if terminalType, _ := round.terminal["type"].(string); terminalType == "response.failed" {
			msg := extractOpenAISSEErrorMessage(mustMarshalJSON(round.terminal))
			if msg == "" {
				msg = "upstream response failed"
			}
			return &codexContinueFoldResult{usage: totalUsage, firstTokenMs: firstTokenMs, responseID: firstVisibleResponseID, imageCount: imageCounter.Count(), imageOutputSizes: imageCounter.Sizes()}, fmt.Errorf("upstream response failed: %s", msg)
		}
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Codex continuation completed account=%d response_id=%s rounds=%d usage_in=%d usage_out=%d", account.ID, firstVisibleResponseID, roundNo, totalUsage.InputTokens, totalUsage.OutputTokens)
		return &codexContinueFoldResult{usage: totalUsage, firstTokenMs: firstTokenMs, responseID: firstVisibleResponseID, imageCount: imageCounter.Count(), imageOutputSizes: imageCounter.Sizes()}, nil
	}
}

func (s *OpenAIGatewayService) readCodexContinueRound(
	ctx context.Context,
	resp *http.Response,
	account *Account,
	roundNo int,
	seq *codexContinueSeq,
	downstreamOutputIndex *int,
	baseResponse *map[string]any,
	firstVisibleResponseID *string,
	writeEvent func(map[string]any),
	imageCounter *openAIImageOutputCounter,
	originalModel string,
	upstreamModel string,
	startTime time.Time,
	firstTokenMs **int,
) (*codexContinueRound, error) {
	round := &codexContinueRound{roundNo: roundNo}
	scanner := bufio.NewScanner(resp.Body)
	maxLineSize := defaultMaxLineSize
	if s.cfg != nil && s.cfg.Gateway.MaxLineSize > 0 {
		maxLineSize = s.cfg.Gateway.MaxLineSize
	}
	scanBuf := getSSEScannerBuf64K()
	defer putSSEScannerBuf64K(scanBuf)
	scanner.Buffer(scanBuf[:0], maxLineSize)

	itemKind := map[any]string{}
	outputIndexMap := map[any]int{}
	needModelReplace := originalModel != upstreamModel
	for scanner.Scan() {
		data, ok := extractOpenAISSEDataLine(scanner.Text())
		if !ok {
			continue
		}
		if strings.TrimSpace(data) == openAISSEDone {
			round.sawDone = true
			continue
		}
		if !gjson.Valid(data) {
			continue
		}
		dataBytes := []byte(data)
		if s.toolCorrector != nil {
			if corrected, changed := s.toolCorrector.CorrectToolCallsInSSEBytes(dataBytes); changed {
				dataBytes = corrected
			}
		}
		if needModelReplace && upstreamModel != "" && bytes.Contains(dataBytes, []byte(upstreamModel)) {
			dataBytes = []byte(s.replaceModelInSSELine("data: "+string(dataBytes), upstreamModel, originalModel)[len("data: "):])
		}
		imageCounter.AddSSEData(dataBytes)
		var ev map[string]any
		if err := json.Unmarshal(dataBytes, &ev); err != nil {
			continue
		}
		eventType, _ := ev["type"].(string)
		if *firstVisibleResponseID == "" {
			*firstVisibleResponseID = extractOpenAIResponseIDFromJSONBytes(dataBytes)
		}
		if roundNo == 1 && (eventType == "response.created" || eventType == "response.in_progress") {
			if eventType == "response.created" {
				if respMap, ok := ev["response"].(map[string]any); ok {
					*baseResponse = cloneJSONMap(respMap)
				}
			}
			ev["sequence_number"] = seq.Take()
			writeEvent(ev)
			continue
		}
		if eventType == "response.completed" || eventType == "response.done" || eventType == "response.failed" || eventType == "response.incomplete" || eventType == "response.cancelled" || eventType == "response.canceled" {
			round.sawTerminal = true
			round.terminal = ev
			if usage, ok := extractOpenAIUsageFromJSONBytes(dataBytes); ok {
				round.usage = &usage
			}
			round.rawUsage = extractCodexContinueRawUsage(ev)
			if eventType == "response.failed" {
				if sanitized, changed := sanitizeOpenAIResponseFailedEventForClient(dataBytes, eventType, false); changed {
					_ = json.Unmarshal(sanitized, &round.terminal)
				}
			}
			break
		}

		upstreamOutputIndex := ev["output_index"]
		switch eventType {
		case "response.output_item.added":
			item, _ := ev["item"].(map[string]any)
			if itemType, _ := item["type"].(string); itemType == "reasoning" {
				itemKind[upstreamOutputIndex] = "reasoning"
				outputIndexMap[upstreamOutputIndex] = *downstreamOutputIndex
				ev["output_index"] = *downstreamOutputIndex
				*downstreamOutputIndex = *downstreamOutputIndex + 1
				ev["sequence_number"] = seq.Take()
				writeEvent(ev)
				if *firstTokenMs == nil {
					ms := int(time.Since(startTime).Milliseconds())
					*firstTokenMs = &ms
				}
			} else {
				itemKind[upstreamOutputIndex] = "buffered"
				round.bufferedEvents = append(round.bufferedEvents, codexContinueBufferedItem{
					upstreamOutputIndex: upstreamOutputIndex,
					itemType:            itemType,
					events:              []map[string]any{ev},
					item:                cloneJSONMap(item),
				})
			}
		default:
			switch itemKind[upstreamOutputIndex] {
			case "reasoning":
				if idx, ok := outputIndexMap[upstreamOutputIndex]; ok {
					ev["output_index"] = idx
				}
				ev["sequence_number"] = seq.Take()
				writeEvent(ev)
				if eventType == "response.output_item.done" {
					if item, ok := ev["item"].(map[string]any); ok {
						cloned := cloneJSONMap(item)
						round.reasoningItems = append(round.reasoningItems, cloned)
					}
				}
			case "buffered":
				if entry := round.findBuffer(upstreamOutputIndex); entry != nil {
					entry.events = append(entry.events, ev)
					if eventType == "response.output_item.done" {
						if item, ok := ev["item"].(map[string]any); ok {
							entry.item = cloneJSONMap(item)
						}
					}
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return round, err
		}
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI] Codex continuation scan error account=%d round=%d err=%v", account.ID, roundNo, err)
		return round, err
	}
	return round, nil
}

func (r *codexContinueRound) findBuffer(upstreamOutputIndex any) *codexContinueBufferedItem {
	for i := range r.bufferedEvents {
		if fmt.Sprint(r.bufferedEvents[i].upstreamOutputIndex) == fmt.Sprint(upstreamOutputIndex) {
			return &r.bufferedEvents[i]
		}
	}
	return nil
}

func decodeCodexContinueBaseBody(body []byte) (map[string]any, error) {
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	out["stream"] = true
	out["include"] = mergeCodexContinueInclude(out["include"])
	return out, nil
}

func buildCodexContinueRoundBody(base map[string]any, originalInput []any, replayTail []any) ([]byte, error) {
	body := cloneJSONMap(base)
	input := make([]any, 0, len(originalInput)+len(replayTail))
	input = append(input, cloneJSONSlice(originalInput)...)
	input = append(input, cloneJSONSlice(replayTail)...)
	body["input"] = input
	body["stream"] = true
	body["include"] = mergeCodexContinueInclude(body["include"])
	delete(body, "previous_response_id")
	return marshalOpenAIUpstreamJSON(body)
}

func mergeCodexContinueInclude(v any) []any {
	items := make([]any, 0)
	seen := false
	if arr, ok := v.([]any); ok {
		for _, item := range arr {
			if s, ok := item.(string); ok && s == "reasoning.encrypted_content" {
				seen = true
			}
			items = append(items, item)
		}
	}
	if !seen {
		items = append(items, "reasoning.encrypted_content")
	}
	return items
}

func cloneCodexContinueInput(v any) []any {
	if arr, ok := v.([]any); ok {
		return cloneJSONSlice(arr)
	}
	if v == nil {
		return []any{}
	}
	return []any{cloneJSONValue(v)}
}

func codexContinueCommentaryMessage(text string) map[string]any {
	return map[string]any{
		"type":  "message",
		"role":  "assistant",
		"phase": "commentary",
		"content": []any{
			map[string]any{"type": "output_text", "text": text},
		},
	}
}

func codexContinueIsTruncationPattern(tokens int) bool {
	return tokens >= codexContinueTruncationStep-2 && (tokens+2)%codexContinueTruncationStep == 0
}

func codexContinueTierN(tokens int) int {
	if !codexContinueIsTruncationPattern(tokens) {
		return 0
	}
	return (tokens + 2) / codexContinueTruncationStep
}

func codexContinueShouldContinue(tokens int) bool {
	n := codexContinueTierN(tokens)
	return n >= codexContinueMinN
}

func codexContinueShouldContinueRound(roundNo, tokens int) bool {
	if codexContinueShouldContinue(tokens) {
		return true
	}
	return roundNo == 1 && tokens > 0 && tokens < codexContinueFirstRoundMin
}

func codexContinueReasoningTokens(usage map[string]any) int {
	if usage == nil {
		return 0
	}
	if details, ok := usage["output_tokens_details"].(map[string]any); ok {
		return intFromAny(details["reasoning_tokens"])
	}
	return 0
}

func extractCodexContinueRawUsage(ev map[string]any) map[string]any {
	if ev == nil {
		return nil
	}
	if usage, ok := ev["usage"].(map[string]any); ok {
		return cloneJSONMap(usage)
	}
	if resp, ok := ev["response"].(map[string]any); ok {
		if usage, ok := resp["usage"].(map[string]any); ok {
			return cloneJSONMap(usage)
		}
	}
	return nil
}

func (u *codexContinueUsageSum) Add(usage map[string]any) {
	if u == nil || usage == nil {
		return
	}
	u.InputTokens += intFromAny(usage["input_tokens"])
	u.OutputTokens += intFromAny(usage["output_tokens"])
	u.TotalTokens += intFromAny(usage["total_tokens"])
	if details, ok := usage["input_tokens_details"].(map[string]any); ok {
		if v, exists := details["cached_tokens"]; exists {
			u.CachedTokens += intFromAny(v)
			u.HasCachedTokens = true
		}
	}
	if details, ok := usage["output_tokens_details"].(map[string]any); ok {
		u.ReasoningTokens += intFromAny(details["reasoning_tokens"])
	}
}

func (u *codexContinueUsageSum) Raw() map[string]any {
	if u == nil {
		return nil
	}
	out := map[string]any{
		"input_tokens":  u.InputTokens,
		"output_tokens": u.OutputTokens,
		"total_tokens":  u.TotalTokens,
		"output_tokens_details": map[string]any{
			"reasoning_tokens": u.ReasoningTokens,
		},
	}
	if u.HasCachedTokens {
		out["input_tokens_details"] = map[string]any{"cached_tokens": u.CachedTokens}
	}
	return out
}

func codexContinueAgentUsage(first map[string]any, total *codexContinueUsageSum, final map[string]any, flushedFinal bool) map[string]any {
	if total == nil {
		return nil
	}
	inputTokens := intFromAny(first["input_tokens"])
	cachedTokens := 0
	hasCached := false
	if details, ok := first["input_tokens_details"].(map[string]any); ok {
		if v, exists := details["cached_tokens"]; exists {
			cachedTokens = intFromAny(v)
			hasCached = true
		}
	}
	finalNonReasoning := 0
	if flushedFinal && final != nil {
		finalNonReasoning = intFromAny(final["output_tokens"]) - codexContinueReasoningTokens(final)
		if finalNonReasoning < 0 {
			finalNonReasoning = 0
		}
	}
	outputTokens := total.ReasoningTokens + finalNonReasoning
	out := map[string]any{
		"input_tokens":  inputTokens,
		"output_tokens": outputTokens,
		"total_tokens":  inputTokens + outputTokens,
		"output_tokens_details": map[string]any{
			"reasoning_tokens": total.ReasoningTokens,
		},
	}
	if hasCached {
		out["input_tokens_details"] = map[string]any{"cached_tokens": cachedTokens}
	}
	return out
}

func codexContinueSyntheticIncomplete(baseResponse map[string]any, output []any, usage map[string]any, seq int, reason string, rounds []map[string]any, billedUsage map[string]any) map[string]any {
	resp := cloneJSONMap(baseResponse)
	if resp == nil {
		resp = map[string]any{}
	}
	resp["status"] = "incomplete"
	resp["output"] = cloneJSONSlice(output)
	resp["usage"] = usage
	resp["incomplete_details"] = map[string]any{"reason": reason}
	addCodexContinueMetadata(resp, rounds, reason, billedUsage)
	return map[string]any{"type": "response.incomplete", "response": resp, "sequence_number": seq}
}

func codexContinueTerminal(terminal map[string]any, baseResponse map[string]any, output []any, usage map[string]any, seq int, rounds []map[string]any, stopReason string, billedUsage map[string]any) map[string]any {
	terminalResp, _ := terminal["response"].(map[string]any)
	resp := cloneJSONMap(baseResponse)
	if len(resp) == 0 {
		resp = cloneJSONMap(terminalResp)
	}
	if resp == nil {
		resp = map[string]any{}
	}
	resp["output"] = cloneJSONSlice(output)
	resp["usage"] = usage
	if status, ok := terminalResp["status"].(string); ok && status != "" {
		resp["status"] = status
	}
	if details, ok := terminalResp["incomplete_details"]; ok {
		resp["incomplete_details"] = cloneJSONValue(details)
	}
	addCodexContinueMetadata(resp, rounds, stopReason, billedUsage)
	eventType, _ := terminal["type"].(string)
	if eventType == "" {
		eventType = "response.completed"
	}
	return map[string]any{"type": eventType, "response": resp, "sequence_number": seq}
}

func addCodexContinueMetadata(resp map[string]any, rounds []map[string]any, stoppedReason string, billedUsage map[string]any) {
	metadata := map[string]any{}
	if existing, ok := resp["metadata"].(map[string]any); ok {
		metadata = cloneJSONMap(existing)
	}
	metadata["proxy_rounds"] = cloneJSONSlice(mapsToAnySlice(rounds))
	metadata["proxy_billed_usage"] = cloneJSONMap(billedUsage)
	if stoppedReason != "" {
		metadata["proxy_stopped_reason"] = stoppedReason
	}
	resp["metadata"] = metadata
}

func addOpenAIUsage(dst *OpenAIUsage, src *OpenAIUsage) {
	if dst == nil || src == nil {
		return
	}
	dst.InputTokens += src.InputTokens
	dst.ImageInputTokens += src.ImageInputTokens
	dst.OutputTokens += src.OutputTokens
	dst.CacheCreationInputTokens += src.CacheCreationInputTokens
	dst.CacheReadInputTokens += src.CacheReadInputTokens
	dst.ImageOutputTokens += src.ImageOutputTokens
}

func cloneJSONMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = cloneJSONValue(v)
	}
	return out
}

func cloneJSONSlice(in []any) []any {
	if in == nil {
		return nil
	}
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = cloneJSONValue(v)
	}
	return out
}

func cloneJSONValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneJSONMap(t)
	case []any:
		return cloneJSONSlice(t)
	default:
		return t
	}
}

func mapsToAnySlice(in []map[string]any) []any {
	out := make([]any, 0, len(in))
	for _, item := range in {
		out = append(out, cloneJSONMap(item))
	}
	return out
}

func intFromAny(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case json.Number:
		i, _ := strconv.Atoi(n.String())
		return i
	default:
		return 0
	}
}

func mustMarshalJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
