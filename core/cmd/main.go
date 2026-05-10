package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v2"
	"github.com/karma-234/sol-whisperer/core/internal/alert"
	"github.com/karma-234/sol-whisperer/core/internal/handler"
	"github.com/karma-234/sol-whisperer/core/internal/processor"
)

func main() {
	telegramSink := alert.NewTelegramSink(os.Getenv("TELEGRAM_BOT_TOKEN"), os.Getenv("TELEGRAM_CHAT_ID"))

	engine := processor.New(processor.Config{
		Shards:           16,
		QueuePerShard:    2048,
		AlertQueue:       1024,
		WindowSec:        60,
		MinVolumeRaw:     1_000_000_000,
		MinTrades:        8,
		SpikeMultiple:    3.0,
		EWMAAlpha:        0.2,
		AlertCooldownSec: 30,
	}, telegramSink)
	h := handler.NewWebhookHandler(os.Getenv("WEBHOOK_SECRET"), engine)
	app := fiber.New()
	app.Post("/webhook", h.WebHookHandler)

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
