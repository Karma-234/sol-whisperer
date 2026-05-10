package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/karma-234/sol-whisperer/core"
	"github.com/karma-234/sol-whisperer/core/internal/alert"
	"github.com/karma-234/sol-whisperer/core/internal/enrichment"
	"github.com/karma-234/sol-whisperer/core/internal/handler"
	"github.com/karma-234/sol-whisperer/core/internal/metadata"
	"github.com/karma-234/sol-whisperer/core/internal/processor"
	"github.com/karma-234/sol-whisperer/core/internal/ws"
	"golang.org/x/sync/errgroup"
)

func main() {
	telegramSink := alert.NewTelegramSink(os.Getenv("TELEGRAM_BOT_TOKEN"), os.Getenv("TELEGRAM_CHAT_ID"))
	metadataFetcher := metadata.NewFetcher()
	if err := metadataFetcher.LoadJupiterList(context.Background()); err != nil {
		slog.Error("Failed to load Jupiter token list", slog.String("error", err.Error()))
	}
	metadataFetcher.StartAutoRefresh()

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
	}, telegramSink, metadataFetcher)
	h := handler.NewWebhookHandler(os.Getenv("WEBHOOK_SECRET"), engine)
	app := fiber.New()
	app.Post("/webhook", h.WebHookHandler)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	var cancelFuncs []context.CancelFunc
	var eg *errgroup.Group
	var egCtx context.Context

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

	// Optional: Alchemy WebSocket ingestion
	alchemyWSURL := os.Getenv("ALCHEMY_WS_URL")
	alchemyRPCURL := os.Getenv("ALCHEMY_RPC_URL")
	effectiveAlchemyWSURL := alchemyWSURL
	derivedWSURL := ""
	if alchemyRPCURL != "" {
		if candidateWSURL, err := deriveWSURLFromRPC(alchemyRPCURL); err != nil {
			slog.Warn("Unable to derive Alchemy WS URL from RPC URL", slog.String("error", err.Error()))
		} else if alchemyWSURL == "" {
			derivedWSURL = candidateWSURL
			effectiveAlchemyWSURL = candidateWSURL
			slog.Info("Derived Alchemy WS URL from RPC URL")
		} else if !sameEndpoint(alchemyWSURL, candidateWSURL) {
			derivedWSURL = candidateWSURL
			slog.Warn("ALCHEMY_WS_URL differs from RPC-derived endpoint; using configured ALCHEMY_WS_URL")
		}
	}

	if effectiveAlchemyWSURL != "" && alchemyRPCURL != "" {
		if parsedWSURL, err := url.Parse(effectiveAlchemyWSURL); err != nil {
			slog.Warn("Unable to parse Alchemy WS URL", slog.String("error", err.Error()))
		} else {
			slog.Info("Resolved Alchemy WS endpoint",
				slog.String("scheme", parsedWSURL.Scheme),
				slog.String("host", parsedWSURL.Host))
		}
		slog.Info("Initializing Alchemy WebSocket ingestion")

		// Initialize errgroup for Alchemy components
		eg, egCtx = errgroup.WithContext(context.Background())
		dexPrograms := make([]string, 0)
		for progID := range core.ProgramNames {
			dexPrograms = append(dexPrograms, progID)
		}

		// Create WebSocket client
		alchemyClient := ws.NewAlchemyClient(ws.AlchemyClientConfig{
			WSURL:                   effectiveAlchemyWSURL,
			RequestTimeout:          5 * time.Second,
			ReconnectMinBackoff:     100 * time.Millisecond,
			ReconnectMaxBackoff:     30 * time.Second,
			CircuitBreakerThreshold: 10,
			CircuitBreakerTimeout:   5 * time.Minute,
			NotificationBufferSize:  1024,
			Logger:                  slog.Default(),
		})

		// Connect to Alchemy
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := alchemyClient.Connect(ctx); err != nil {
			slog.Error("Failed to connect to Alchemy WebSocket", slog.String("error", err.Error()))
			cancel()
		} else {
			cancel()

			if err := alchemyClient.VerifySolanaRPC(); err != nil {
				slog.Warn("Configured Alchemy WS endpoint failed Solana probe", slog.String("error", err.Error()))
				if derivedWSURL != "" && !sameEndpoint(effectiveAlchemyWSURL, derivedWSURL) {
					slog.Warn("Retrying Alchemy WS with RPC-derived endpoint")
					_ = alchemyClient.Disconnect()
					alchemyClient = ws.NewAlchemyClient(ws.AlchemyClientConfig{
						WSURL:                   derivedWSURL,
						RequestTimeout:          5 * time.Second,
						ReconnectMinBackoff:     100 * time.Millisecond,
						ReconnectMaxBackoff:     30 * time.Second,
						CircuitBreakerThreshold: 10,
						CircuitBreakerTimeout:   5 * time.Minute,
						NotificationBufferSize:  1024,
						Logger:                  slog.Default(),
					})

					ctxRetry, cancelRetry := context.WithTimeout(context.Background(), 30*time.Second)
					if err := alchemyClient.Connect(ctxRetry); err != nil {
						slog.Error("Failed to connect to RPC-derived Alchemy WebSocket", slog.String("error", err.Error()))
						cancelRetry()
					} else {
						cancelRetry()
						if err := alchemyClient.VerifySolanaRPC(); err != nil {
							slog.Error("RPC-derived Alchemy WS endpoint failed Solana probe", slog.String("error", err.Error()))
						} else {
							effectiveAlchemyWSURL = derivedWSURL
							slog.Info("Using RPC-derived Alchemy WS endpoint after successful probe")
						}
					}
				}
			}

			if !alchemyClient.IsConnected() {
				slog.Error("Alchemy WebSocket unavailable after probe/fallback")
			} else {

				// Create ingestor
				ingestor := ws.NewIngestor(ws.IngestorConfig{
					AlchemyClient:       alchemyClient,
					DedupMaxSize:        10000,
					DedupTTL:            2 * time.Hour,
					EnrichmentQueueSize: 2048,
					StablecoinMints:     core.StablecoinMints,
					DEXPrograms:         dexPrograms,
					Logger:              slog.Default(),
				})

				// Create enricher
				enricher := enrichment.NewAlchemyTxEnricher(enrichment.AlchemyTxEnricherConfig{
					RPCURL:          alchemyRPCURL,
					MaxWorkers:      5,
					RequestTimeout:  5 * time.Second,
					Retries:         3,
					Engine:          engine,
					StablecoinMints: core.StablecoinMints,
					Logger:          slog.Default(),
				})

				// Start ingestion goroutine
				ingestCtx, ingestCancel := context.WithCancel(egCtx)
				cancelFuncs = append(cancelFuncs, ingestCancel)
				eg.Go(func() error {
					slog.Info("Starting Alchemy ingest loop")
					return ingestor.IngestLoop(ingestCtx)
				})

				// Start enrichment workers
				enrichCtx, enrichCancel := context.WithCancel(egCtx)
				cancelFuncs = append(cancelFuncs, enrichCancel)
				eg.Go(func() error {
					slog.Info("Starting Alchemy enrichment workers", slog.Int("workers", 5))
					return enricher.EnrichLoop(enrichCtx, ingestor.EnrichmentChan())
				})

				// Periodically log metrics
				metricsCtx, metricsCancel := context.WithCancel(egCtx)
				cancelFuncs = append(cancelFuncs, metricsCancel)
				eg.Go(func() error {
					ticker := time.NewTicker(30 * time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-metricsCtx.Done():
							return metricsCtx.Err()
						case <-ticker.C:
							ingestMetrics := ingestor.Metrics()
							enrichMetrics := enricher.Metrics()
							slog.Info("Alchemy metrics",
								slog.Any("ingest", ingestMetrics),
								slog.Any("enrich", enrichMetrics))
						}
					}
				})
			}
		}
	} else {
		slog.Info("Alchemy WebSocket not configured (ALCHEMY_WS_URL or ALCHEMY_RPC_URL not set)")
	}

	// Wait for interrupt
	<-quit
	slog.Info("Shutting down server...")

	// Cancel all contexts
	for _, cancel := range cancelFuncs {
		cancel()
	}

	// Wait for all goroutines to finish (if Alchemy is configured)
	if eg != nil {
		if err := eg.Wait(); err != nil {
			slog.Error("Alchemy goroutines error", slog.String("error", err.Error()))
		}
	}

	metadataFetcher.Stop()
	if err := app.Shutdown(); err != nil {
		slog.Error("Server shutdown error", slog.String("error", err.Error()))
	}
}

func deriveWSURLFromRPC(rpcURL string) (string, error) {
	u, err := url.Parse(rpcURL)
	if err != nil {
		return "", err
	}

	switch strings.ToLower(u.Scheme) {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	case "wss", "ws":
		// Keep as-is
	default:
		return "", &url.Error{Op: "parse", URL: rpcURL, Err: errUnsupportedURLScheme(u.Scheme)}
	}

	return u.String(), nil
}

func sameEndpoint(a, b string) bool {
	au, errA := url.Parse(a)
	bu, errB := url.Parse(b)
	if errA != nil || errB != nil {
		return a == b
	}

	return strings.EqualFold(au.Host, bu.Host) && au.EscapedPath() == bu.EscapedPath()
}

type errUnsupportedURLScheme string

func (e errUnsupportedURLScheme) Error() string {
	return "unsupported URL scheme: " + string(e)
}
