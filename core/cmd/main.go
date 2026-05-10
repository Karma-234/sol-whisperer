package main

import (
	"log/slog"
	"net/http"
	"net/http/pprof"
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
		slog.Info("Server is starting on port 8080")
		if err := app.Listen(":8080"); err != nil {
			slog.Info("Server encountered an error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	// Real pprof server on :6060
	pprofMux := http.NewServeMux()
	pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
	pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	pprofMux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	pprofMux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	pprofMux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	pprofMux.Handle("/debug/pprof/block", pprof.Handler("block"))
	pprofMux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))

	go func() {
		slog.Info("Pprof server is starting on port 6060")
		if err := http.ListenAndServe(":6060", pprofMux); err != nil {
			slog.Info("Pprof server encountered an error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	<-quit
	slog.Info("Shutting down server...")
}
