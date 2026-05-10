package handler

import (
	"errors"

	"github.com/gofiber/fiber/v2"
	"github.com/karma-234/sol-whisperer/core/internal/detector"
	"github.com/karma-234/sol-whisperer/core/internal/processor"
	"github.com/karma-234/sol-whisperer/core/internal/types"
)

type WebhookHandler struct {
	secret string
	engine *processor.Engine
}

func NewWebhookHandler(secret string, engine *processor.Engine) *WebhookHandler {
	return &WebhookHandler{
		secret: secret,
		engine: engine,
	}
}

func (h *WebhookHandler) WebHookHandler(c *fiber.Ctx) error {
	if auth := c.Get("Authorization"); auth != "Bearer "+h.secret {
		return c.Status(401).SendString("Unauthorized")
	}
	var payload types.HeliusEnhancedWebhookPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(400).SendString("Invalid payload")
	}
	for i := range payload {
		info := detector.ExtractSwapInfoWithOptions(payload[i], false)
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
