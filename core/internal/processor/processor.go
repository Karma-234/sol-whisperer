package processor

import (
	"context"
	"errors"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/karma-234/sol-whisperer/core/internal/detector"
	"github.com/karma-234/sol-whisperer/core/internal/metadata"
)

var ErrShardQueueFull = errors.New("shard queue full")

type Event struct {
	Signature   string
	Timestamp   int64
	Swapper     string
	OutputMint  string
	OutputAmt   uint64
	AmountInSOL uint64 // lamports (populated when input is SOL)
	Source      string
}

type Alert struct {
	Mint         string
	TokenName    string
	Swapper      string
	Signature    string
	Source       string
	VolumeRaw    uint64
	TradeCount   uint32
	BaselineEWMA float64
	SpikeRatio   float64
	WindowSec    int64
	DetectedAt   int64
	AmountInSOL  uint64 // lamports (if input was SOL)
}

type AlertSink interface {
	Send(context.Context, Alert) error
}

type Counters struct {
	Enqueued     uint64
	Dropped      uint64
	Handled      uint64
	AlertQueued  uint64
	AlertDropped uint64
	AlertSent    uint64
}

type Config struct {
	Shards           int
	QueuePerShard    int
	AlertQueue       int
	WindowSec        int64
	MinVolumeRaw     uint64
	MinTrades        uint32
	SpikeMultiple    float64
	EWMAAlpha        float64
	AlertCooldownSec int64
}

type Engine struct {
	cfg             Config
	shards          []chan Event
	alerts          chan Alert
	sink            AlertSink
	metadataFetcher *metadata.Fetcher

	enqueued     uint64
	dropped      uint64
	handled      uint64
	alertQueued  uint64
	alertDropped uint64
	alertSent    uint64
}

func New(cfg Config, sink AlertSink, fetcher *metadata.Fetcher) *Engine {
	if cfg.Shards <= 0 {
		cfg.Shards = max(2*runtime.NumCPU(), 16)
	}
	if cfg.QueuePerShard <= 0 {
		cfg.QueuePerShard = 2048
	}
	if cfg.AlertQueue <= 0 {
		cfg.AlertQueue = 1024
	}
	if cfg.WindowSec <= 0 {
		cfg.WindowSec = 60
	}
	if cfg.SpikeMultiple <= 0 {
		cfg.SpikeMultiple = 3.0
	}
	if cfg.EWMAAlpha <= 0 || cfg.EWMAAlpha >= 1 {
		cfg.EWMAAlpha = 0.2
	}
	if cfg.AlertCooldownSec <= 0 {
		cfg.AlertCooldownSec = 30
	}

	e := &Engine{
		cfg:             cfg,
		shards:          make([]chan Event, cfg.Shards),
		alerts:          make(chan Alert, cfg.AlertQueue),
		sink:            sink,
		metadataFetcher: fetcher,
	}

	for i := 0; i < cfg.Shards; i++ {
		ch := make(chan Event, cfg.QueuePerShard)
		e.shards[i] = ch
		go e.runShard(ch)
	}

	if sink != nil {
		go e.runAlertWorker()
	}

	return e
}

func (e *Engine) IngestSwapInfo(info *detector.SwapInfo) error {
	if info == nil || info.OutputMint == "" || info.OutputAmount == "" || info.Timestamp <= 0 {
		return nil
	}

	amt, err := strconv.ParseUint(info.OutputAmount, 10, 64)
	if err != nil || amt == 0 {
		return nil
	}

	ev := Event{
		Signature:   info.Signature,
		Timestamp:   info.Timestamp,
		Swapper:     info.Swapper,
		OutputMint:  info.OutputMint,
		OutputAmt:   amt,
		AmountInSOL: info.AmountInSOL,
		Source:      info.Source,
	}

	sh := shardForMint(ev.OutputMint, len(e.shards))
	select {
	case e.shards[sh] <- ev:
		atomic.AddUint64(&e.enqueued, 1)
		return nil
	default:
		atomic.AddUint64(&e.dropped, 1)
		return ErrShardQueueFull
	}
}

func (e *Engine) Stats() Counters {
	return Counters{
		Enqueued:     atomic.LoadUint64(&e.enqueued),
		Dropped:      atomic.LoadUint64(&e.dropped),
		Handled:      atomic.LoadUint64(&e.handled),
		AlertQueued:  atomic.LoadUint64(&e.alertQueued),
		AlertDropped: atomic.LoadUint64(&e.alertDropped),
		AlertSent:    atomic.LoadUint64(&e.alertSent),
	}
}

func (e *Engine) runShard(ch <-chan Event) {
	state := newShardState(e.cfg)
	for ev := range ch {
		if alert, ok := state.process(ev); ok {
			e.enqueueAlert(alert)
		}
		atomic.AddUint64(&e.handled, 1)
	}
}

func (e *Engine) enqueueAlert(alert Alert) {
	if e.sink == nil || alert.Mint == "" {
		return
	}

	select {
	case e.alerts <- alert:
		atomic.AddUint64(&e.alertQueued, 1)
	default:
		atomic.AddUint64(&e.alertDropped, 1)
	}
}

func (e *Engine) runAlertWorker() {
	for alert := range e.alerts {
		// Fetch token metadata asynchronously (non-blocking lookup)
		if e.metadataFetcher != nil {
			if meta := e.metadataFetcher.FetchTokenMetadata(alert.Mint); meta != nil {
				alert.TokenName = meta.Symbol
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := e.sink.Send(ctx, alert)
		cancel()
		if err == nil {
			atomic.AddUint64(&e.alertSent, 1)
		} else {
			slog.Debug("Failed to send alert", slog.String("error", err.Error()), slog.String("mint", alert.Mint))
		}
	}
}

func shardForMint(m string, mod int) int {
	h := fnv.New32a()
	_, _ = io.WriteString(h, m)
	return int(h.Sum32() % uint32(mod))
}

type mintWindow struct {
	volSlots []uint64
	cntSlots []uint32

	totalVol uint64
	totalCnt uint32
	lastSec  int64
	ewma     float64

	lastAlertSec int64
}

type shardState struct {
	cfg    Config
	byMint map[string]*mintWindow
}

func newShardState(cfg Config) *shardState {
	return &shardState{
		cfg:    cfg,
		byMint: make(map[string]*mintWindow, 1024),
	}
}

func (s *shardState) process(ev Event) (Alert, bool) {
	w, ok := s.byMint[ev.OutputMint]
	if !ok {
		w = &mintWindow{
			volSlots: make([]uint64, s.cfg.WindowSec),
			cntSlots: make([]uint32, s.cfg.WindowSec),
			lastSec:  ev.Timestamp,
		}
		s.byMint[ev.OutputMint] = w
	}

	s.advance(w, ev.Timestamp)

	idx := ev.Timestamp % s.cfg.WindowSec
	w.volSlots[idx] += ev.OutputAmt
	w.cntSlots[idx]++
	w.totalVol += ev.OutputAmt
	w.totalCnt++

	cur := float64(w.totalVol)
	if w.ewma == 0 {
		w.ewma = cur
	} else {
		w.ewma = s.cfg.EWMAAlpha*cur + (1.0-s.cfg.EWMAAlpha)*w.ewma
	}

	if w.totalVol < s.cfg.MinVolumeRaw || w.totalCnt < s.cfg.MinTrades || w.ewma <= 0 || cur < s.cfg.SpikeMultiple*math.Max(w.ewma, 1) {
		return Alert{}, false
	}

	if ev.Timestamp-w.lastAlertSec < s.cfg.AlertCooldownSec {
		return Alert{}, false
	}
	w.lastAlertSec = ev.Timestamp

	return Alert{
		Mint:         ev.OutputMint,
		Swapper:      ev.Swapper,
		Signature:    ev.Signature,
		Source:       ev.Source,
		VolumeRaw:    w.totalVol,
		TradeCount:   w.totalCnt,
		BaselineEWMA: w.ewma,
		SpikeRatio:   cur / math.Max(w.ewma, 1),
		WindowSec:    s.cfg.WindowSec,
		DetectedAt:   ev.Timestamp,
		AmountInSOL:  ev.AmountInSOL,
	}, true
}

func (s *shardState) advance(w *mintWindow, toSec int64) {
	if toSec <= w.lastSec {
		return
	}

	steps := toSec - w.lastSec
	if steps > s.cfg.WindowSec {
		steps = s.cfg.WindowSec
	}

	for i := int64(1); i <= steps; i++ {
		sec := w.lastSec + i
		idx := sec % s.cfg.WindowSec

		w.totalVol -= w.volSlots[idx]
		w.totalCnt -= w.cntSlots[idx]
		w.volSlots[idx] = 0
		w.cntSlots[idx] = 0
	}

	w.lastSec = toSec
}
