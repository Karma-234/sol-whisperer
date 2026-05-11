package handler

import (
	"errors"
	"log/slog"

	"github.com/gofiber/fiber/v2"
	"github.com/karma-234/sol-whisperer/core/internal/detector"
	"github.com/karma-234/sol-whisperer/core/internal/filter"
	"github.com/karma-234/sol-whisperer/core/internal/metadata"
	"github.com/karma-234/sol-whisperer/core/internal/processor"
	"github.com/karma-234/sol-whisperer/core/internal/types"
)

type WebhookHandler struct {
	secret      string
	engine      *processor.Engine
	capFilter   *filter.MarketCapFilter
	metaFetcher *metadata.Fetcher
}

func NewWebhookHandler(secret string, engine *processor.Engine, capFilter *filter.MarketCapFilter, metaFetcher *metadata.Fetcher) *WebhookHandler {
	return &WebhookHandler{
		secret:      secret,
		engine:      engine,
		capFilter:   capFilter,
		metaFetcher: metaFetcher,
	}
}

func (h *WebhookHandler) WebHookHandler(c *fiber.Ctx) error {
	if auth := c.Get("Authorization"); auth != h.secret && auth != "Bearer "+h.secret {
		slog.Warn("webhook_auth_failed", slog.Bool("auth_present", auth != ""))
		return c.Status(401).SendString("Unauthorized")
	}
	var payload types.HeliusEnhancedWebhookPayload
	if err := c.BodyParser(&payload); err != nil {
		slog.Warn("webhook_parse_failed", slog.String("error", err.Error()))
		return c.Status(400).SendString("Invalid payload")
	}
	acceptedCount := 0
	for i := range payload {
		info := detector.ExtractSwapInfoWithOptions(payload[i], false, h.capFilter, h.metaFetcher)
		if info == nil {
			continue
		}
		if err := h.engine.IngestSwapInfo(info); err != nil {
			if errors.Is(err, processor.ErrShardQueueFull) {
				slog.Warn("webhook_ingest_queue_full", slog.String("signature", info.Signature))
				// return c.Status(503).SendString("Queue full")
				continue
			}
			slog.Warn("webhook_ingest_failed", slog.String("signature", info.Signature), slog.String("error", err.Error()))
			continue
			// return c.Status(500).SendString("Processing failed")
		}
		acceptedCount++
	}
	if acceptedCount > 0 {
		slog.Info("webhook_accepted", slog.Int("count", acceptedCount))
	}
	return c.SendStatus(200)
}
