package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v2"
	"github.com/karma-234/sol-whisperer/core/internal/handler"
)

func main() {
	app := fiber.New()
	app.Post("/webhook", handler.WebHookHandler)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		if err := app.Listen(":8080"); err != nil {
			slog.Info("Server encountered an error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	<-quit
	slog.Info("Shutting down server...")
}
