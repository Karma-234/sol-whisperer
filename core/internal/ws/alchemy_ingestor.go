package ws

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// DedupCacheEntry represents a cached entry in the dedup cache
type DedupCacheEntry struct {
	Signature string
	ExpiresAt time.Time
}

// DedupCache is a simple LRU cache with TTL for deduplicating notifications
type DedupCache struct {
	entries sync.Map // map[string]DedupCacheEntry
	mu      sync.Mutex
	maxSize int
	ttl     time.Duration
	logger  *slog.Logger
}

// NewDedupCache creates a new dedup cache
func NewDedupCache(maxSize int, ttl time.Duration, logger *slog.Logger) *DedupCache {
	return &DedupCache{
		maxSize: maxSize,
		ttl:     ttl,
		logger:  logger,
	}
}

// IsDuplicate checks if a signature is already in cache (dedup) and adds it if not
func (dc *DedupCache) IsDuplicate(signature string) bool {
	now := time.Now()

	// Check if exists and not expired
	if val, ok := dc.entries.Load(signature); ok {
		entry := val.(DedupCacheEntry)
		if now.Before(entry.ExpiresAt) {
			return true // duplicate
		}
	}

	// Add or update entry
	dc.entries.Store(signature, DedupCacheEntry{
		Signature: signature,
		ExpiresAt: now.Add(dc.ttl),
	})

	// Periodic cleanup of expired entries (simple approach)
	// In high-throughput scenarios, this can be optimized with a background cleanup goroutine
	dc.mu.Lock()
	defer dc.mu.Unlock()

	// Count entries and clean if over size
	count := 0
	var toDelete []string
	dc.entries.Range(func(key, value interface{}) bool {
		count++
		entry := value.(DedupCacheEntry)
		if now.After(entry.ExpiresAt) {
			toDelete = append(toDelete, key.(string))
		}
		return true
	})

	for _, key := range toDelete {
		dc.entries.Delete(key)
	}

	// If still over size, we could evict oldest, but for now just log
	if count > dc.maxSize {
		dc.logger.Warn("dedup cache size exceeded", slog.Int("size", count), slog.Int("max", dc.maxSize))
	}

	return false
}

// EnrichmentTask represents a task queued for enrichment
type EnrichmentTask struct {
	Signature string
	Slot      uint64
	ProgramID string
	Timestamp time.Time
}

// Ingestor ingests Alchemy WebSocket notifications, deduplicates, and queues for enrichment
type Ingestor struct {
	client          *AlchemyClient
	dedupCache      *DedupCache
	enrichmentQueue chan EnrichmentTask
	stablecoinMints map[string]bool
	dexPrograms     []string
	logger          *slog.Logger
	metricsOnce     sync.Once
	connectedCount  int64
	reconnectCount  int64
	queueDropCount  int64
	dedupDropCount  int64
}

// IngestorConfig holds configuration for the Ingestor
type IngestorConfig struct {
	AlchemyClient       *AlchemyClient
	DedupMaxSize        int
	DedupTTL            time.Duration
	EnrichmentQueueSize int
	StablecoinMints     map[string]bool
	DEXPrograms         []string
	Logger              *slog.Logger
}

// NewIngestor creates a new Ingestor
func NewIngestor(cfg IngestorConfig) *Ingestor {
	if cfg.DedupMaxSize == 0 {
		cfg.DedupMaxSize = 10000
	}
	if cfg.DedupTTL == 0 {
		cfg.DedupTTL = 2 * time.Hour
	}
	if cfg.EnrichmentQueueSize == 0 {
		cfg.EnrichmentQueueSize = 2048
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Ingestor{
		client:          cfg.AlchemyClient,
		dedupCache:      NewDedupCache(cfg.DedupMaxSize, cfg.DedupTTL, cfg.Logger),
		enrichmentQueue: make(chan EnrichmentTask, cfg.EnrichmentQueueSize),
		stablecoinMints: cfg.StablecoinMints,
		dexPrograms:     cfg.DEXPrograms,
		logger:          cfg.Logger,
	}
}

// EnrichmentChan returns the channel for enrichment tasks
func (i *Ingestor) EnrichmentChan() <-chan EnrichmentTask {
	return i.enrichmentQueue
}

// Close closes the ingestor
func (i *Ingestor) Close() error {
	close(i.enrichmentQueue)
	return nil
}

// ProcessNotification processes a single notification from the WebSocket
// Returns a SwapInfo if successfully parsed and not a duplicate, or nil otherwise
func (i *Ingestor) ProcessNotification(notif *ProgramNotification) *EnrichmentTask {
	if notif == nil || notif.Params == nil || notif.Params.Result == nil || notif.Params.Result.Value == nil {
		return nil
	}

	value := notif.Params.Result.Value
	if value.Signature == "" {
		return nil
	}

	// Check for duplicates
	if i.dedupCache.IsDuplicate(value.Signature) {
		i.dedupDropCount++
		return nil
	}

	var slot uint64
	if notif.Params.Result.Context != nil {
		slot = notif.Params.Result.Context.Slot
	}

	// Create enrichment task
	task := EnrichmentTask{
		Signature: value.Signature,
		Slot:      slot,
		Timestamp: time.Now(),
	}

	// Try to enqueue; drop if queue is full
	select {
	case i.enrichmentQueue <- task:
		// Successfully queued
	default:
		i.logger.Warn("enrichment queue full, dropping task", slog.String("signature", value.Signature))
		i.queueDropCount++
		return nil
	}

	return &task
}

// IngestLoop runs the main ingestion loop
func (i *Ingestor) IngestLoop(ctx context.Context) error {
	// Subscribe to all DEX programs
	for _, progID := range i.dexPrograms {
		if err := i.client.Subscribe(progID); err != nil {
			i.logger.Error("failed to subscribe to program", slog.String("program", progID), slog.String("error", err.Error()))
			return fmt.Errorf("subscribe to program %s: %w", progID, err)
		}
	}

	// Start read loop in background
	go i.client.ReadLoop(ctx)

	// Process notifications
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case notif, ok := <-i.client.NotificationChan():
			if !ok {
				return fmt.Errorf("notification channel closed")
			}

			if notif == nil {
				continue
			}

			i.ProcessNotification(notif)
		}
	}
}

// Metrics returns current metrics
func (i *Ingestor) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"queue_drops": i.queueDropCount,
		"dedup_drops": i.dedupDropCount,
		"queue_depth": len(i.enrichmentQueue),
		"queue_max":   cap(i.enrichmentQueue),
	}
}
