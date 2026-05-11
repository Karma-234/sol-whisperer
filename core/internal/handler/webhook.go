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
	secret       string
	engine       *processor.Engine
	capFilter    *filter.MarketCapFilter
	metaFetcher  *metadata.Fetcher
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
		return c.Status(401).SendString("Unauthorized")
	}
	var payload types.HeliusEnhancedWebhookPayload
	if err := c.BodyParser(&payload); err != nil {
		slog.Info("Failed to parse webhook payload", slog.String("error", err.Error()))
		return c.Status(400).SendString("Invalid payload")
	}
	for i := range payload {
		info := detector.ExtractSwapInfoWithOptions(payload[i], false, h.capFilter, h.metaFetcher)
		if info == nil {
			continue
		}
		if err := h.engine.IngestSwapInfo(info); err != nil {
			if errors.Is(err, processor.ErrShardQueueFull) {
				return c.Status(503).SendString("Queue full")
			}
			return c.Status(500).SendString("Processing failed")
		}
	}
	return c.SendStatus(200)
}
