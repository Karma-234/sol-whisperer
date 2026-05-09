package handler

import (
	"github.com/gofiber/fiber/v2"
)

func WebHookHandler(c *fiber.Ctx) error {
	return c.SendStatus(200)
}
