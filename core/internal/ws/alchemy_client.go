package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// AlchemyClient manages a persistent WebSocket connection to Alchemy
type AlchemyClient struct {
	wsURL                   string
	conn                    *websocket.Conn
	connMu                  sync.Mutex
	subscriptions           map[string]int // program ID -> subscription ID
	subscriptionMu          sync.RWMutex
	nextID                  int
	nextIDMu                sync.Mutex
	requestTimeout          time.Duration
	reconnectMinBackoff     time.Duration
	reconnectMaxBackoff     time.Duration
	circuitBreakerFailures  int
	circuitBreakerThreshold int
	circuitBreakerTimeout   time.Duration
	lastFailureTime         *time.Time
	failureMu               sync.Mutex
	isConnected             atomic.Bool
	stopCh                  chan struct{}
	notificationCh          chan *ProgramNotification
	logger                  *slog.Logger
}

// AlchemyClientConfig holds configuration for AlchemyClient
type AlchemyClientConfig struct {
	WSURL                   string
	RequestTimeout          time.Duration
	ReconnectMinBackoff     time.Duration
	ReconnectMaxBackoff     time.Duration
	CircuitBreakerThreshold int
	CircuitBreakerTimeout   time.Duration
	NotificationBufferSize  int
	Logger                  *slog.Logger
}

// NewAlchemyClient creates a new Alchemy WebSocket client
func NewAlchemyClient(cfg AlchemyClientConfig) *AlchemyClient {
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.ReconnectMinBackoff == 0 {
		cfg.ReconnectMinBackoff = 100 * time.Millisecond
	}
	if cfg.ReconnectMaxBackoff == 0 {
		cfg.ReconnectMaxBackoff = 30 * time.Second
	}
	if cfg.CircuitBreakerThreshold == 0 {
		cfg.CircuitBreakerThreshold = 10
	}
	if cfg.CircuitBreakerTimeout == 0 {
		cfg.CircuitBreakerTimeout = 5 * time.Minute
	}
	if cfg.NotificationBufferSize == 0 {
		cfg.NotificationBufferSize = 1024
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &AlchemyClient{
		wsURL:                   cfg.WSURL,
		subscriptions:           make(map[string]int),
		requestTimeout:          cfg.RequestTimeout,
		reconnectMinBackoff:     cfg.ReconnectMinBackoff,
		reconnectMaxBackoff:     cfg.ReconnectMaxBackoff,
		circuitBreakerThreshold: cfg.CircuitBreakerThreshold,
		circuitBreakerTimeout:   cfg.CircuitBreakerTimeout,
		stopCh:                  make(chan struct{}),
		notificationCh:          make(chan *ProgramNotification, cfg.NotificationBufferSize),
		logger:                  cfg.Logger,
	}
}

// Connect establishes a WebSocket connection to Alchemy
func (ac *AlchemyClient) Connect(ctx context.Context) error {
	ac.connMu.Lock()
	defer ac.connMu.Unlock()

	if ac.conn != nil {
		ac.conn.Close()
		ac.conn = nil
	}

	// Parse URL to allow auth parameters in connection string
	u, err := url.Parse(ac.wsURL)
	if err != nil {
		ac.logger.Error("failed to parse WebSocket URL", slog.String("error", err.Error()))
		return fmt.Errorf("parse ws url: %w", err)
	}

	dialer := *websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		ac.logger.Error("failed to connect to WebSocket", slog.String("error", err.Error()))
		return fmt.Errorf("dial ws: %w", err)
	}

	ac.conn = conn
	ac.isConnected.Store(true)
	ac.logger.Info("WebSocket connected")
	return nil
}

// Disconnect closes the WebSocket connection
func (ac *AlchemyClient) Disconnect() error {
	ac.connMu.Lock()
	defer ac.connMu.Unlock()

	if ac.conn != nil {
		err := ac.conn.Close()
		ac.conn = nil
		ac.isConnected.Store(false)
		return err
	}
	ac.isConnected.Store(false)
	return nil
}

// Subscribe subscribes to program updates for a given program ID
func (ac *AlchemyClient) Subscribe(programID string) error {
	ac.subscriptionMu.RLock()
	if _, exists := ac.subscriptions[programID]; exists {
		ac.subscriptionMu.RUnlock()
		return nil // already subscribed
	}
	ac.subscriptionMu.RUnlock()

	id := ac.getNextID()
	req := JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  "logsSubscribe",
		Params: []any{
			map[string]any{
				"mentions": []string{programID},
			},
			map[string]any{
				"commitment": "processed",
			},
		},
	}

	if err := ac.sendRequest(req); err != nil {
		return fmt.Errorf("subscribe %s: %w", programID, err)
	}

	// Read subscription response
	resp, err := ac.readResponse()
	if err != nil {
		return fmt.Errorf("read subscription response: %w", err)
	}

	if resp.Error != nil {
		return fmt.Errorf("subscription error: %s (code=%d, data=%v)", resp.Error.Message, resp.Error.Code, resp.Error.Data)
	}

	// Extract subscription ID from result (should be a float64)
	subID, ok := resp.Result.(float64)
	if !ok {
		return fmt.Errorf("invalid subscription ID type")
	}

	ac.subscriptionMu.Lock()
	ac.subscriptions[programID] = int(subID)
	ac.subscriptionMu.Unlock()

	ac.logger.Info("subscribed to program", slog.String("program_id", programID), slog.Int("subscription_id", int(subID)))
	return nil
}

// ReadLoop continuously reads notifications from the WebSocket
func (ac *AlchemyClient) ReadLoop(ctx context.Context) {
	defer close(ac.notificationCh)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			ac.Disconnect()
			return
		case <-ac.stopCh:
			ac.Disconnect()
			return
		case <-ticker.C:
			// Periodic ping to keep connection alive
			ac.connMu.Lock()
			if ac.conn != nil {
				ac.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				ac.conn.WriteMessage(websocket.PingMessage, []byte{})
				ac.conn.SetWriteDeadline(time.Time{})
			}
			ac.connMu.Unlock()
		default:
			ac.connMu.Lock()
			conn := ac.conn
			ac.connMu.Unlock()

			if conn == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			_, msg, err := conn.ReadMessage()
			if err != nil {
				ac.connMu.Lock()
				if ac.conn == conn {
					ac.conn = nil
					ac.isConnected.Store(false)
				}
				ac.connMu.Unlock()
				ac.logger.Error("read from WebSocket failed", slog.String("error", err.Error()))
				return
			}

			var notif ProgramNotification
			if err := json.Unmarshal(msg, &notif); err != nil {
				ac.logger.Warn("failed to parse notification", slog.String("error", err.Error()))
				continue
			}

			select {
			case ac.notificationCh <- &notif:
			case <-ctx.Done():
				ac.Disconnect()
				return
			case <-ac.stopCh:
				ac.Disconnect()
				return
			}
		}
	}
}

// NotificationChan returns the channel for receiving notifications
func (ac *AlchemyClient) NotificationChan() <-chan *ProgramNotification {
	return ac.notificationCh
}

// IsConnected returns true if the WebSocket is connected
func (ac *AlchemyClient) IsConnected() bool {
	return ac.isConnected.Load()
}

// ReconnectWithBackoff reconnects with exponential backoff
func (ac *AlchemyClient) ReconnectWithBackoff(ctx context.Context) error {
	ac.failureMu.Lock()
	now := time.Now()
	if ac.lastFailureTime != nil && now.Sub(*ac.lastFailureTime) < ac.circuitBreakerTimeout {
		ac.circuitBreakerFailures++
		if ac.circuitBreakerFailures >= ac.circuitBreakerThreshold {
			ac.failureMu.Unlock()
			ac.logger.Warn("circuit breaker triggered, entering cooldown", slog.Duration("cooldown", ac.circuitBreakerTimeout))
			time.Sleep(ac.circuitBreakerTimeout)
			ac.circuitBreakerFailures = 0
			ac.lastFailureTime = nil
			ac.failureMu.Unlock()
			return fmt.Errorf("circuit breaker triggered")
		}
	} else {
		ac.circuitBreakerFailures = 1
	}
	ac.lastFailureTime = &now
	ac.failureMu.Unlock()

	backoff := ac.exponentialBackoff(ac.circuitBreakerFailures)
	ac.logger.Info("reconnecting with backoff", slog.Duration("backoff", backoff), slog.Int("attempt", ac.circuitBreakerFailures))
	time.Sleep(backoff)

	err := ac.Connect(ctx)
	if err == nil {
		ac.failureMu.Lock()
		ac.circuitBreakerFailures = 0
		ac.lastFailureTime = nil
		ac.failureMu.Unlock()

		// Resubscribe to all programs
		ac.subscriptionMu.RLock()
		programs := make([]string, 0, len(ac.subscriptions))
		for prog := range ac.subscriptions {
			programs = append(programs, prog)
		}
		ac.subscriptionMu.RUnlock()

		for _, prog := range programs {
			if err := ac.Subscribe(prog); err != nil {
				ac.logger.Error("failed to resubscribe after reconnect", slog.String("program", prog), slog.String("error", err.Error()))
			}
		}
	}
	return err
}

// Stop stops the read loop
func (ac *AlchemyClient) Stop() {
	close(ac.stopCh)
}

// Private helpers

func (ac *AlchemyClient) getNextID() int {
	ac.nextIDMu.Lock()
	defer ac.nextIDMu.Unlock()
	ac.nextID++
	return ac.nextID
}

func (ac *AlchemyClient) sendRequest(req JSONRPCRequest) error {
	ac.connMu.Lock()
	defer ac.connMu.Unlock()

	if ac.conn == nil {
		return fmt.Errorf("not connected")
	}

	ac.conn.SetWriteDeadline(time.Now().Add(ac.requestTimeout))
	defer ac.conn.SetWriteDeadline(time.Time{})

	return ac.conn.WriteJSON(req)
}

func (ac *AlchemyClient) readResponse() (*JSONRPCResponse, error) {
	ac.connMu.Lock()
	defer ac.connMu.Unlock()

	if ac.conn == nil {
		return nil, fmt.Errorf("not connected")
	}

	ac.conn.SetReadDeadline(time.Now().Add(ac.requestTimeout))
	defer ac.conn.SetReadDeadline(time.Time{})

	var resp JSONRPCResponse
	if err := ac.conn.ReadJSON(&resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (ac *AlchemyClient) exponentialBackoff(attempt int) time.Duration {
	expBackoff := ac.reconnectMinBackoff.Seconds() * math.Pow(2, float64(attempt-1))
	if d := time.Duration(expBackoff * float64(time.Second)); d > ac.reconnectMaxBackoff {
		expBackoff = ac.reconnectMaxBackoff.Seconds()
	}

	// Add jitter: +/- 10% randomization
	jitterAmount := expBackoff * 0.1
	jitter := (rand.Float64() - 0.5) * 2 * jitterAmount
	return time.Duration((expBackoff+jitter)*1000) * time.Millisecond
}
