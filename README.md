# sol-whisperer

Real-time Solana swap-spike detector for meme-token discovery. The service ingests swap activity, applies token and volume filters, computes short-window anomaly signals, and emits Telegram alerts.

Status: Beta — production-usable for real-time monitoring, with known operational caveats and evolving interfaces.

## What This Service Does

- Ingests swap candidates from:
  - Helius Enhanced Webhook (`POST /webhook`) as the baseline ingestion path.
  - Optional Alchemy WebSocket + RPC enrichment path.
- Normalizes and filters swaps:
  - Stablecoin output mints are ignored.
  - Minimum SOL input threshold is enforced when the input is native SOL.
  - Market-cap threshold filter is applied for meme-token focus.
- Detects volume spikes using sharded rolling windows and EWMA baselines.
- Sends HTML-formatted alerts to Telegram.

## High-Level Architecture

```text
              +------------------------+
              | Helius Webhook (POST) |
              +-----------+------------+
                          |
                          v
                    +-----------+
                    | detector  |
                    | filters   |
                    +-----+-----+
                          |
                          v
                    +-----------+
                    | processor |
                    | shards +  |
                    | EWMA      |
                    +-----+-----+
                          |
                          v
                    +-----------+
                    | Telegram  |
                    | alerts    |
                    +-----------+

Optional side path:
Alchemy WS logsSubscribe -> ingestor (dedup/queue) -> RPC enrichment -> detector -> processor
```

## Repository Layout

```text
core/cmd/main.go                         # service entrypoint, wiring, servers, shutdown
core/internal/handler/webhook.go         # webhook auth + payload handling
core/internal/detector/detector.go       # swap extraction + filtering
core/internal/processor/processor.go     # sharded spike detection + alert queue
core/internal/alert/telegram.go          # Telegram sink and message formatting
core/internal/metadata/fetcher.go        # Jupiter + DexScreener token metadata
core/internal/filter/marketcap.go        # market-cap filter
core/internal/ws/*                       # Alchemy WebSocket client + ingestor
core/internal/enrichment/*               # Alchemy transaction enrichment workers
core/internal/types/*                    # Helius payload types
core/helpers.go                          # DEX program IDs + stablecoin mints
Dockerfile                               # multi-stage production image
.github/workflows/deploy-ec2.yml         # CI deploy pipeline to EC2
```

## Runtime Requirements

- Go `1.25.x` (module currently targets `go 1.25.0`).
- Network egress to:
  - Telegram Bot API
  - Jupiter Tokens V2 API
  - DexScreener API
  - Optional: Alchemy WS/RPC endpoints
- A configured Helius webhook sender (external to this service).

## Configuration

### Full `.env` Template

Copy and set values appropriate to your environment:

```bash
# -----------------------------
# Required for baseline operation
# -----------------------------
WEBHOOK_SECRET=replace-with-shared-secret
TELEGRAM_BOT_TOKEN=123456789:replace-with-bot-token
TELEGRAM_CHAT_ID=-1001234567890

# -----------------------------
# Optional filters and metadata
# -----------------------------
# Max allowed token market cap (USD) for alerts.
# If omitted or invalid, service defaults to 200000.
MAX_MARKET_CAP_USD=200000

# Optional Jupiter API key for tokens/v2/search.
JUPITER_API_KEY=

# -----------------------------
# Optional Alchemy ingestion path
# Both must be set together to enable WS ingestion.
# -----------------------------
ALCHEMY_WS_URL=wss://solana-mainnet.g.alchemy.com/v2/replace-with-key
ALCHEMY_RPC_URL=https://solana-mainnet.g.alchemy.com/v2/replace-with-key
```

### Environment Variable Semantics

| Variable             | Required                | Default  | Purpose                                                           |
| -------------------- | ----------------------- | -------- | ----------------------------------------------------------------- |
| `WEBHOOK_SECRET`     | Yes                     | none     | Authorization shared secret for `/webhook`.                       |
| `TELEGRAM_BOT_TOKEN` | Yes (if alerts enabled) | none     | Bot token for Telegram sendMessage API.                           |
| `TELEGRAM_CHAT_ID`   | Yes (if alerts enabled) | none     | Destination chat/channel ID.                                      |
| `MAX_MARKET_CAP_USD` | No                      | `200000` | Max cap allowed by market-cap filter.                             |
| `JUPITER_API_KEY`    | No                      | empty    | Optional API key for Jupiter token search requests.               |
| `ALCHEMY_WS_URL`     | No                      | empty    | Enables optional WS ingestion when paired with `ALCHEMY_RPC_URL`. |
| `ALCHEMY_RPC_URL`    | No                      | empty    | RPC endpoint used by enrichment workers.                          |

## Processing Defaults (Current)

Configured in `main.go` and engine internals:

- Processor config:
  - `Shards=16`
  - `QueuePerShard=2048`
  - `AlertQueue=1024`
  - `WindowSec=60`
  - `MinVolumeRaw=1_000_000_000`
  - `MinTrades=8`
  - `SpikeMultiple=3.0`
  - `EWMAAlpha=0.2`
  - `AlertCooldownSec=30`
- Detector filters:
  - Minimum SOL input threshold: `0.3 SOL` (`3e8` lamports) when input is native SOL.
  - Stablecoin output mint suppression enabled.
  - Market-cap filter enabled using `MAX_MARKET_CAP_USD`.

## Local Development

### 1. Install Dependencies

```bash
go mod download
```

### 2. Run Locally

```bash
# Option A: export env vars directly
export WEBHOOK_SECRET="replace-with-secret"
export TELEGRAM_BOT_TOKEN="replace-with-bot-token"
export TELEGRAM_CHAT_ID="replace-with-chat-id"

# Optional
export MAX_MARKET_CAP_USD="200000"
export JUPITER_API_KEY=""
export ALCHEMY_WS_URL=""
export ALCHEMY_RPC_URL=""

go run ./core/cmd/main.go
```

### 3. Build Binary

```bash
go build -o sol-whisperer ./core/cmd/main.go
```

## Docker

### Build

```bash
docker build -t sol-whisperer .
```

### Run

```bash
docker run -d \
  --name sol-whisperer \
  --restart unless-stopped \
  -p 8080:8080 \
  -p 127.0.0.1:6060:6060 \
  --env-file .env \
  sol-whisperer
```

## API and Health Checks

### Webhook Endpoint

- Path: `POST /webhook`
- Authorization header accepted values:
  - `Authorization: <WEBHOOK_SECRET>`
  - `Authorization: Bearer <WEBHOOK_SECRET>`

### Quick Liveness Checks

```bash
# Expected: 405 Method Not Allowed (if using GET)
# This still proves HTTP server is alive.
curl -i http://localhost:8080/webhook

# Expected: 401 Unauthorized (POST without valid Authorization)
curl -i -X POST http://localhost:8080/webhook
```

### pprof

Exposed on `:6060` with standard endpoints:

- `/debug/pprof/`
- `/debug/pprof/profile`
- `/debug/pprof/heap`
- `/debug/pprof/goroutine`
- `/debug/pprof/trace`

Example:

```bash
go tool pprof http://localhost:6060/debug/pprof/profile
```

## Optional Alchemy Ingestion

When both `ALCHEMY_WS_URL` and `ALCHEMY_RPC_URL` are present:

1. Service opens WS connection.
2. Subscribes to tracked DEX programs via `logsSubscribe` with `processed` commitment.
3. Ingestor deduplicates signatures and queues enrichment tasks.
4. Enrichment workers call `getTransaction` over RPC.
5. Extracted swaps re-enter the same detector/processor pipeline.

If either variable is missing, service remains in webhook-only mode.

### Important Operational Note

If logs show:

`subscription error: Method 'logsSubscribe' not found (code=-32601)`

this typically indicates Alchemy app/key capability mismatch for Solana PubSub methods, not necessarily a process crash. In this codebase, the Alchemy ingestion goroutine exits for that path, while the HTTP webhook server can continue running.

## Alert Semantics

Telegram alert includes:

- Token symbol (if metadata resolved)
- SOL amount (if input was SOL)
- Mint
- Swapper
- Source
- Window/trade count/raw volume
- Baseline EWMA and spike ratio
- Signature

If metadata lookup fails, alert still sends without symbol enrichment.

## Deployment

### GitHub Actions -> EC2

Workflow file: `.github/workflows/deploy-ec2.yml`

Pipeline behavior:

1. Build Docker image.
2. Save and copy image tarball to EC2.
3. Load image on EC2.
4. Replace running container with:
   - `-p 8080:8080`
   - `-p 127.0.0.1:6060:6060`
   - `--env-file /etc/environment`

### Railway

`railway.toml` builds and runs Go binary from `core/cmd`.

## Troubleshooting Playbook

### 1. `405 Method Not Allowed` on `/webhook`

Cause: Route is defined as POST-only.

Action:

```bash
curl -i -X POST http://localhost:8080/webhook
```

### 2. `401 Unauthorized` on `/webhook`

Cause: Missing/invalid `Authorization` header.

Action: send exact secret or `Bearer <secret>`.

### 3. `logsSubscribe not found (-32601)`

Cause: Provider/app capability issue for PubSub method.

Action:

- Verify Alchemy app tier/capabilities and key.
- Keep webhook ingestion active as baseline path.

### 4. Error log on shutdown (`context canceled` / errgroup wait)

Cause: expected context cancellation during graceful shutdown.

Action: treat as normal if it appears immediately after shutdown signal.

### 5. Missing token symbol in alert

Cause: metadata lookup timeout or no upstream metadata.

Action:

- Verify network egress to Jupiter and DexScreener.
- Optionally set `JUPITER_API_KEY`.

## Contributor Workflow

### Recommended Dev Loop

```bash
# 1) Pull deps
go mod download

# 2) Build fast
go build ./core/cmd

# 3) Run all tests (repository may currently have no *_test.go files)
go test ./...

# 4) Vet
go vet ./...
```

### Making Safe Changes

- Keep webhook compatibility stable (`/webhook`, auth behavior, response codes).
- Preserve detector filter semantics unless intentionally changing alert policy.
- Treat queue and timeout changes as production-impacting; verify under load.
- For Alchemy path changes, validate behavior with and without WS/RPC env vars.

### Suggested Regression Checklist

- Webhook route responds and auth works.
- Baseline webhook-only mode works with Alchemy vars unset.
- Optional Alchemy mode starts when both vars are set.
- Alerts still send and include expected fields.
- Graceful shutdown completes without hangs.

## Security Notes

- Never commit real tokens/secrets.
- Restrict inbound access to `:8080` to trusted webhook source(s).
- Keep `:6060` bound to localhost or behind restricted network controls.
- Use TLS termination and authentication at ingress/reverse proxy.

## Known Limitations

- No persistence layer: processing state and dedup are in-memory.
- No built-in structured metrics exporter (currently log-based metrics snapshots).
- No unit/integration test suite is currently committed.

## License

MIT
