package enrichment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/karma-234/sol-whisperer/core/internal/detector"
	"github.com/karma-234/sol-whisperer/core/internal/processor"
	"github.com/karma-234/sol-whisperer/core/internal/ws"
)

// AlchemyTxEnricher fetches transaction details from Alchemy and extracts swap info
type AlchemyTxEnricher struct {
	rpcURL          string
	httpClient      *http.Client
	maxWorkers      int
	requestTimeout  time.Duration
	retries         int
	engine          *processor.Engine
	stablecoinMints map[string]bool
	programNames    map[string]string
	logger          *slog.Logger
	metricsOnce     sync.Once
	successCount    int64
	failureCount    int64
	timeoutCount    int64
	dedupCount      int64
	processedSigs   sync.Map // map[string]bool for dedup
}

// AlchemyTxEnricherConfig holds configuration for AlchemyTxEnricher
type AlchemyTxEnricherConfig struct {
	RPCURL          string
	MaxWorkers      int
	RequestTimeout  time.Duration
	Retries         int
	Engine          *processor.Engine
	StablecoinMints map[string]bool
	ProgramNames    map[string]string
	Logger          *slog.Logger
}

// NewAlchemyTxEnricher creates a new enricher
func NewAlchemyTxEnricher(cfg AlchemyTxEnricherConfig) *AlchemyTxEnricher {
	if cfg.MaxWorkers == 0 {
		cfg.MaxWorkers = 5
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.Retries == 0 {
		cfg.Retries = 3
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.StablecoinMints == nil {
		cfg.StablecoinMints = make(map[string]bool)
	}
	if cfg.ProgramNames == nil {
		cfg.ProgramNames = make(map[string]string)
	}

	return &AlchemyTxEnricher{
		rpcURL:          cfg.RPCURL,
		maxWorkers:      cfg.MaxWorkers,
		requestTimeout:  cfg.RequestTimeout,
		retries:         cfg.Retries,
		engine:          cfg.Engine,
		stablecoinMints: cfg.StablecoinMints,
		programNames:    cfg.ProgramNames,
		logger:          cfg.Logger,
		httpClient: &http.Client{
			Timeout: cfg.RequestTimeout,
		},
	}
}

// EnrichLoop starts worker goroutines to process enrichment tasks
func (ate *AlchemyTxEnricher) EnrichLoop(ctx context.Context, taskChan <-chan ws.EnrichmentTask) error {
	var wg sync.WaitGroup
	for i := 0; i < ate.maxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ate.worker(ctx, workerID, taskChan)
		}(i)
	}

	wg.Wait()
	return ctx.Err()
}

// worker processes enrichment tasks
func (ate *AlchemyTxEnricher) worker(ctx context.Context, workerID int, taskChan <-chan ws.EnrichmentTask) {
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-taskChan:
			if !ok {
				return
			}

			// Dedup check: have we already processed this signature?
			if _, ok := ate.processedSigs.Load(task.Signature); ok {
				atomic.AddInt64(&ate.dedupCount, 1)
				continue
			}

			ate.processTask(ctx, task)
			ate.processedSigs.Store(task.Signature, true)
		}
	}
}

// processTask fetches transaction details and extracts swap info
func (ate *AlchemyTxEnricher) processTask(ctx context.Context, task ws.EnrichmentTask) {
	txResp, err := ate.getTransactionWithRetry(ctx, task.Signature)
	if err != nil {
		atomic.AddInt64(&ate.failureCount, 1)
		ate.logger.Warn("failed to get transaction", slog.String("signature", task.Signature), slog.String("error", err.Error()))
		return
	}

	if txResp == nil || txResp.Meta == nil {
		atomic.AddInt64(&ate.failureCount, 1)
		ate.logger.Warn("no transaction metadata", slog.String("signature", task.Signature))
		return
	}

	// Extract swap info from token balance deltas
	swapInfo := ate.extractSwapInfo(task.Signature, txResp.Meta, txResp.Transaction)
	if swapInfo == nil {
		atomic.AddInt64(&ate.failureCount, 1)
		return
	}

	// Ingest into processor
	ate.engine.IngestSwapInfo(swapInfo)
	atomic.AddInt64(&ate.successCount, 1)
	ate.logger.Debug("enriched transaction",
		slog.String("signature", swapInfo.Signature),
		slog.String("output_mint", swapInfo.OutputMint),
		slog.String("amount", swapInfo.OutputAmount))
}

// getTransactionWithRetry fetches a transaction with retry logic
func (ate *AlchemyTxEnricher) getTransactionWithRetry(ctx context.Context, signature string) (*ws.TransactionResponse, error) {
	var lastErr error
	for attempt := 0; attempt < ate.retries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resp, err := ate.getTransaction(ctx, signature)
		if err == nil {
			return resp, nil
		}

		lastErr = err
		if attempt < ate.retries-1 {
			backoff := time.Duration((attempt+1)*100) * time.Millisecond
			time.Sleep(backoff)
		}
	}

	if lastErr == context.DeadlineExceeded {
		atomic.AddInt64(&ate.timeoutCount, 1)
	}
	return nil, lastErr
}

// getTransaction fetches a single transaction from Alchemy RPC
func (ate *AlchemyTxEnricher) getTransaction(ctx context.Context, signature string) (*ws.TransactionResponse, error) {
	req := ws.JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "getTransaction",
		Params: []interface{}{
			signature,
			map[string]interface{}{
				"encoding":                       "jsonParsed",
				"maxSupportedTransactionVersion": 0,
			},
		},
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", ate.rpcURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := ate.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status %d", httpResp.StatusCode)
	}

	var jsonResp ws.JSONRPCResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&jsonResp); err != nil {
		return nil, err
	}

	if jsonResp.Error != nil {
		return nil, fmt.Errorf("rpc error: %s", jsonResp.Error.Message)
	}

	// Extract transaction response
	resultBytes, err := json.Marshal(jsonResp.Result)
	if err != nil {
		return nil, err
	}

	var txResp ws.TransactionResponse
	if err := json.Unmarshal(resultBytes, &txResp); err != nil {
		return nil, err
	}

	return &txResp, nil
}

// extractSwapInfo extracts swap info from transaction metadata
// Uses token balance deltas to identify swapper and output mint/amount
func (ate *AlchemyTxEnricher) extractSwapInfo(signature string, meta *ws.TransactionMeta, tx *ws.FullTransaction) *detector.SwapInfo {
	if meta == nil || len(meta.PreTokenBalances) == 0 {
		return nil
	}

	// Group pre and post balances by (owner, mint)
	preBalances := make(map[string]map[string]*ws.TokenBalance)  // owner -> (mint -> balance)
	postBalances := make(map[string]map[string]*ws.TokenBalance) // owner -> (mint -> balance)

	for i := range meta.PreTokenBalances {
		balance := &meta.PreTokenBalances[i]
		if _, ok := preBalances[balance.Owner]; !ok {
			preBalances[balance.Owner] = make(map[string]*ws.TokenBalance)
		}
		preBalances[balance.Owner][balance.Mint] = balance
	}

	for i := range meta.PostTokenBalances {
		balance := &meta.PostTokenBalances[i]
		if _, ok := postBalances[balance.Owner]; !ok {
			postBalances[balance.Owner] = make(map[string]*ws.TokenBalance)
		}
		postBalances[balance.Owner][balance.Mint] = balance
	}

	// Find swapper: account with greatest total volume change
	type ownerDelta struct {
		owner      string
		totalDelta float64
		outputs    map[string]float64 // mint -> delta
		inputs     map[string]float64 // mint -> delta
	}

	ownerDeltas := make(map[string]*ownerDelta)
	for owner, mints := range postBalances {
		od := &ownerDelta{
			owner:   owner,
			outputs: make(map[string]float64),
			inputs:  make(map[string]float64),
		}

		for mint, postBal := range mints {
			preBal, exists := preBalances[owner][mint]
			var preAmount float64
			if exists {
				preAmount = preBal.UiTokenAmount.UIAmount
			}

			postAmount := postBal.UiTokenAmount.UIAmount
			delta := postAmount - preAmount

			if delta > 0 {
				od.outputs[mint] = delta
			} else if delta < 0 {
				od.inputs[mint] = -delta
			}

			if delta > 0 {
				od.totalDelta += delta
			} else {
				od.totalDelta -= delta
			}
		}

		if od.totalDelta > 0 {
			ownerDeltas[owner] = od
		}
	}

	if len(ownerDeltas) == 0 {
		return nil
	}

	// Pick swapper with greatest total delta
	var swapper *ownerDelta
	var maxDelta float64
	for _, od := range ownerDeltas {
		if od.totalDelta > maxDelta {
			maxDelta = od.totalDelta
			swapper = od
		}
	}

	if swapper == nil || len(swapper.outputs) == 0 {
		return nil
	}

	// Find output mint (prefer non-stablecoin)
	var outputMint string
	var outputAmount float64

	// First pass: find non-stablecoin output
	for mint, amount := range swapper.outputs {
		if !ate.stablecoinMints[mint] {
			if amount > outputAmount {
				outputMint = mint
				outputAmount = amount
			}
		}
	}

	// If no non-stablecoin output, take largest stablecoin output
	if outputMint == "" {
		for mint, amount := range swapper.outputs {
			if amount > outputAmount {
				outputMint = mint
				outputAmount = amount
			}
		}
	}

	if outputMint == "" || outputAmount == 0 {
		return nil
	}

	// Convert output amount to string
	outputAmountStr := strconv.FormatFloat(outputAmount, 'f', -1, 64)

	return &detector.SwapInfo{
		Signature:    signature,
		Swapper:      swapper.owner,
		OutputMint:   outputMint,
		OutputAmount: outputAmountStr,
		Source:       "alchemy_ws",
	}
}

// Metrics returns current metrics
func (ate *AlchemyTxEnricher) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"success":  atomic.LoadInt64(&ate.successCount),
		"failures": atomic.LoadInt64(&ate.failureCount),
		"timeouts": atomic.LoadInt64(&ate.timeoutCount),
		"dedup":    atomic.LoadInt64(&ate.dedupCount),
		"workers":  ate.maxWorkers,
	}
}
