package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type TokenMetadata struct {
	Symbol   string
	Name     string
	MarketCap uint64 // in USD
}

type Fetcher struct {
	jupiterCache  atomic.Pointer[map[string]*TokenMetadata]
	dexCache      sync.Map // mint -> *cacheEntry
	decoderPool   *sync.Pool
	refreshTicker *time.Ticker
	ctx           context.Context
	cancel        context.CancelFunc
	refreshWg     sync.WaitGroup
	httpClient    *http.Client
	jupiterAPIKey string
}

type cacheEntry struct {
	meta      *TokenMetadata
	timestamp time.Time
}

// NewFetcher initializes the token metadata fetcher with pre-allocated pools.
func NewFetcher() *Fetcher {
	ctx, cancel := context.WithCancel(context.Background())

	// Pre-allocate Jupiter cache with capacity 2000
	initialCache := make(map[string]*TokenMetadata, 2000)

	f := &Fetcher{
		decoderPool: &sync.Pool{
			New: func() any {
				return json.NewDecoder(nil)
			},
		},
		ctx:           ctx,
		cancel:        cancel,
		jupiterAPIKey: os.Getenv("JUPITER_API_KEY"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	f.jupiterCache.Store(&initialCache)

	return f
}

// LoadJupiterList is kept for compatibility; token metadata now uses on-demand Jupiter Tokens V2 lookups.
func (f *Fetcher) LoadJupiterList(ctx context.Context) error {
	slog.Info("Jupiter token preload skipped; using on-demand tokens/v2/search lookups")
	return nil
}

// StartAutoRefresh begins the background 30-minute refresh loop.
func (f *Fetcher) StartAutoRefresh() {
	slog.Info("Jupiter auto-refresh disabled; metadata fetched on-demand")
}

// FetchTokenMetadata returns cached metadata or fetches from Jupiter Tokens V2 with timeout.
// Blocks up to 2 seconds for Jupiter search; DexScreener lookup continues async for cache population.
func (f *Fetcher) FetchTokenMetadata(mint string) *TokenMetadata {
	// Check Jupiter cache (read-only, no lock)
	if cache := f.jupiterCache.Load(); cache != nil {
		if meta, ok := (*cache)[mint]; ok {
			return meta
		}
	}

	// Check DexScreener cache
	if entry, ok := f.dexCache.Load(mint); ok {
		e := entry.(*cacheEntry)
		// Return if not expired (24h TTL)
		if time.Since(e.timestamp) < 24*time.Hour {
			return e.meta
		}
		// Expired, remove from cache
		f.dexCache.Delete(mint)
	}

	// Blocking Jupiter fetch with timeout (ensures token names are resolved for alerts)
	meta := f.fetchFromJupiterSearchBlocking(mint)
	if meta != nil {
		return meta
	}

	// Spawn async DexScreener fetch for fallback cache population
	go f.fetchFromDexScreener(mint)

	return nil
}

// fetchFromJupiterSearch queries Jupiter Tokens V2 search endpoint and caches result.
func (f *Fetcher) fetchFromJupiterSearch(mint string) {
	ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
	defer cancel()

	url := fmt.Sprintf("https://api.jup.ag/tokens/v2/search?query=%s", mint)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	if f.jupiterAPIKey != "" {
		req.Header.Set("x-api-key", f.jupiterAPIKey)
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		slog.Debug("Jupiter search fetch failed", slog.String("mint", mint), slog.String("error", err.Error()))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		slog.Debug("Jupiter search returned non-200", slog.String("mint", mint), slog.Int("status", resp.StatusCode))
		return
	}

	body := io.LimitReader(resp.Body, 128*1024)

	var tokens []struct {
		ID     string `json:"id"`
		Symbol string `json:"symbol"`
		Name   string `json:"name"`
	}

	if err := json.NewDecoder(body).Decode(&tokens); err != nil {
		slog.Debug("Jupiter search parse failed", slog.String("mint", mint), slog.String("error", err.Error()))
		return
	}

	if len(tokens) == 0 || tokens[0].ID == "" {
		return
	}

	resolvedMint := tokens[0].ID
	meta := &TokenMetadata{Symbol: tokens[0].Symbol, Name: tokens[0].Name}
	f.upsertJupiterCache(resolvedMint, meta)

	slog.Debug("Cached token metadata from Jupiter", slog.String("mint", resolvedMint), slog.String("symbol", meta.Symbol))
}

// fetchFromJupiterSearchBlocking fetches and returns metadata directly (blocking, ~2s timeout).
func (f *Fetcher) fetchFromJupiterSearchBlocking(mint string) *TokenMetadata {
	ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
	defer cancel()

	url := fmt.Sprintf("https://api.jup.ag/tokens/v2/search?query=%s", mint)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	if f.jupiterAPIKey != "" {
		req.Header.Set("x-api-key", f.jupiterAPIKey)
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil
	}

	body := io.LimitReader(resp.Body, 128*1024)

	var tokens []struct {
		ID        string `json:"id"`
		Symbol    string `json:"symbol"`
		Name      string `json:"name"`
		MarketCap uint64 `json:"marketCap"`
	}

	if err := json.NewDecoder(body).Decode(&tokens); err != nil {
		return nil
	}

	if len(tokens) == 0 || tokens[0].ID == "" {
		return nil
	}

	resolvedMint := tokens[0].ID
	meta := &TokenMetadata{Symbol: tokens[0].Symbol, Name: tokens[0].Name, MarketCap: tokens[0].MarketCap}
	f.upsertJupiterCache(resolvedMint, meta)
	return meta
}

func (f *Fetcher) upsertJupiterCache(mint string, meta *TokenMetadata) {
	current := f.jupiterCache.Load()
	if current == nil {
		newCache := map[string]*TokenMetadata{mint: meta}
		f.jupiterCache.Store(&newCache)
		return
	}

	newCache := make(map[string]*TokenMetadata, len(*current)+1)
	for k, v := range *current {
		newCache[k] = v
	}
	newCache[mint] = meta
	f.jupiterCache.Store(&newCache)
}

// fetchFromDexScreener queries DexScreener API and caches result.
func (f *Fetcher) fetchFromDexScreener(mint string) {
	ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
	defer cancel()

	url := fmt.Sprintf("https://api.dexscreener.com/latest/dex/tokens/%s", mint)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)

	resp, err := f.httpClient.Do(req)
	if err != nil {
		slog.Debug("DexScreener fetch failed", slog.String("mint", mint), slog.String("error", err.Error()))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		slog.Debug("DexScreener returned non-200", slog.String("mint", mint), slog.Int("status", resp.StatusCode))
		return
	}

	// Read body with size limit
	body := io.LimitReader(resp.Body, 100*1024) // 100KB limit

	var result struct {
		Pairs []struct {
			BaseToken struct {
				Symbol    string `json:"symbol"`
				Name      string `json:"name"`
			} `json:"baseToken"`
			FDV float64 `json:"fdv"` // fully diluted valuation (market cap proxy)
		} `json:"pairs"`
	}

	if err := json.NewDecoder(body).Decode(&result); err != nil {
		slog.Debug("DexScreener parse failed", slog.String("mint", mint), slog.String("error", err.Error()))
		return
	}

	if len(result.Pairs) == 0 || result.Pairs[0].BaseToken.Symbol == "" {
		slog.Debug("DexScreener returned empty result", slog.String("mint", mint))
		return
	}

	meta := &TokenMetadata{
		Symbol:    result.Pairs[0].BaseToken.Symbol,
		Name:      result.Pairs[0].BaseToken.Name,
		MarketCap: uint64(result.Pairs[0].FDV),
	}

	f.dexCache.Store(mint, &cacheEntry{
		meta:      meta,
		timestamp: time.Now(),
	})

	slog.Debug("Cached token metadata from DexScreener", slog.String("mint", mint), slog.String("symbol", meta.Symbol), slog.Uint64("cap", meta.MarketCap))
}

// Stop gracefully shuts down the fetcher and background goroutines.
func (f *Fetcher) Stop() {
	if f.refreshTicker != nil {
		f.refreshTicker.Stop()
	}
	f.cancel()
	f.refreshWg.Wait()
	slog.Info("Metadata fetcher stopped")
}
