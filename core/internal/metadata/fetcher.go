package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type TokenMetadata struct {
	Symbol string
	Name   string
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
		ctx:    ctx,
		cancel: cancel,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	f.jupiterCache.Store(&initialCache)

	return f
}

// LoadJupiterList fetches and loads the Jupiter token list at startup (fail-fast).
func (f *Fetcher) LoadJupiterList(ctx context.Context) error {
	slog.Info("Loading Jupiter token list...")

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(reqCtx, "GET", "https://token.jup.ag/all", nil)
	resp, err := f.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch Jupiter token list: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Jupiter API returned %d", resp.StatusCode)
	}

	var tokens []struct {
		Address  string `json:"address"`
		Symbol   string `json:"symbol"`
		Name     string `json:"name"`
		Decimals int    `json:"decimals"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tokens); err != nil {
		return fmt.Errorf("failed to parse Jupiter token list: %w", err)
	}

	cache := make(map[string]*TokenMetadata, len(tokens))
	for _, t := range tokens {
		cache[t.Address] = &TokenMetadata{
			Symbol: t.Symbol,
			Name:   t.Name,
		}
	}

	f.jupiterCache.Store(&cache)
	slog.Info("Loaded Jupiter token list", slog.Int("count", len(tokens)))

	return nil
}

// StartAutoRefresh begins the background 30-minute refresh loop.
func (f *Fetcher) StartAutoRefresh() {
	f.refreshTicker = time.NewTicker(30 * time.Minute)

	f.refreshWg.Go(func() {
		for {
			select {
			case <-f.refreshTicker.C:
				if err := f.LoadJupiterList(f.ctx); err != nil {
					slog.Error("Jupiter list auto-refresh failed", slog.String("error", err.Error()))
				}
			case <-f.ctx.Done():
				return
			}
		}
	})
}

// FetchTokenMetadata returns cached metadata or nil if not found (non-blocking hot path).
// New tokens are fetched async from DexScreener in background.
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

	// Spawn async fetch from DexScreener (non-blocking return)
	go f.fetchFromDexScreener(mint)

	return nil
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
				Symbol string `json:"symbol"`
				Name   string `json:"name"`
			} `json:"baseToken"`
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
		Symbol: result.Pairs[0].BaseToken.Symbol,
		Name:   result.Pairs[0].BaseToken.Name,
	}

	f.dexCache.Store(mint, &cacheEntry{
		meta:      meta,
		timestamp: time.Now(),
	})

	slog.Debug("Cached token metadata from DexScreener", slog.String("mint", mint), slog.String("symbol", meta.Symbol))
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
