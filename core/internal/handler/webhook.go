package handler

import (
	"os"

	"github.com/gofiber/fiber/v2"
	"github.com/karma-234/sol-whisperer/core/internal/detector"
	"github.com/karma-234/sol-whisperer/core/internal/types"
)

func WebHookHandler(c *fiber.Ctx) error {
	if auth := c.Get("Authorization"); auth != "Bearer "+os.Getenv("WEBHOOK_SECRET") {
		return c.Status(401).SendString("Unauthorized")
	}
	var payload types.HeliusEnhancedWebhookPayload
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(400).SendString("Invalid payload")
	}
	go func() {
		for _, tx := range payload {
			_ = detector.ExtractSwapInfo(tx)
		}
	}()
	return c.SendStatus(200)
}
