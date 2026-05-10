package processor

import (
	"hash/fnv"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/karma-234/sol-whisperer/core/internal/detector"
)

type Event struct {
	Signature  string
	Timestamp  int64
	Swapper    string
	OutputMint string
	OutputAmt  uint64
	Source     string
}

type Counters struct {
	Enqueued uint64
	Dropped  uint64
	Handled  uint64
}

type Config struct {
	Shards        int
	QueuePerShard int

	WindowSec     int64
	MinVolumeRaw  uint64
	MinTrades     uint32
	SpikeMultiple float64
	EWMAAlpha     float64
}

type Engine struct {
	cfg      Config
	shards   []chan Event
	stats    Counters
	detector *SpikeDetector
}

func New(cfg Config) *Engine {
	if cfg.Shards <= 0 {
		cfg.Shards = 16
	}
	if cfg.QueuePerShard <= 0 {
		cfg.QueuePerShard = 2048
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

	e := &Engine{
		cfg:      cfg,
		shards:   make([]chan Event, cfg.Shards),
		detector: NewSpikeDetector(cfg),
	}

	for i := 0; i < cfg.Shards; i++ {
		ch := make(chan Event, cfg.QueuePerShard)
		e.shards[i] = ch
		go e.worker(ch)
	}
	return e
}

func (e *Engine) worker(ch <-chan Event) {
	for ev := range ch {
		e.detector.Process(ev)
		atomic.AddUint64(&e.stats.Handled, 1)
	}
}

func (e *Engine) IngestSwapInfo(info *detector.SwapInfo) {
	if info == nil || info.OutputMint == "" || info.OutputAmount == "" || info.Timestamp <= 0 {
		return
	}

	amt, err := strconv.ParseUint(info.OutputAmount, 10, 64)
	if err != nil || amt == 0 {
		return
	}

	ev := Event{
		Signature:  info.Signature,
		Timestamp:  info.Timestamp,
		Swapper:    info.Swapper,
		OutputMint: info.OutputMint,
		OutputAmt:  amt,
		Source:     info.Source,
	}

	sh := shardForMint(ev.OutputMint, len(e.shards))

	// Latency-first overload policy: drop newest when full.
	select {
	case e.shards[sh] <- ev:
		atomic.AddUint64(&e.stats.Enqueued, 1)
	default:
		atomic.AddUint64(&e.stats.Dropped, 1)
	}
}

func (e *Engine) Stats() Counters {
	return Counters{
		Enqueued: atomic.LoadUint64(&e.stats.Enqueued),
		Dropped:  atomic.LoadUint64(&e.stats.Dropped),
		Handled:  atomic.LoadUint64(&e.stats.Handled),
	}
}

func shardForMint(m string, mod int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(m))
	return int(h.Sum32() % uint32(mod))
}

type mintState struct {
	volSlots []uint64
	cntSlots []uint32

	totalVol uint64
	totalCnt uint32
	lastSec  int64
	ewma     float64
}

type shardState struct {
	byMint map[string]*mintState
}

type SpikeDetector struct {
	windowSec int64
	minVol    uint64
	minTrades uint32
	multiple  float64
	alpha     float64

	// sharded by same shard function to avoid cross-locking
	states []shardState
}

func NewSpikeDetector(cfg Config) *SpikeDetector {
	d := &SpikeDetector{
		windowSec: cfg.WindowSec,
		minVol:    cfg.MinVolumeRaw,
		minTrades: cfg.MinTrades,
		multiple:  cfg.SpikeMultiple,
		alpha:     cfg.EWMAAlpha,
		states:    make([]shardState, cfg.Shards),
	}
	for i := range d.states {
		d.states[i].byMint = make(map[string]*mintState, 1024)
	}
	return d
}

func (d *SpikeDetector) Process(ev Event) {
	sh := shardForMint(ev.OutputMint, len(d.states))
	ss := &d.states[sh]

	ms, ok := ss.byMint[ev.OutputMint]
	if !ok {
		ms = &mintState{
			volSlots: make([]uint64, d.windowSec),
			cntSlots: make([]uint32, d.windowSec),
			lastSec:  ev.Timestamp,
		}
		ss.byMint[ev.OutputMint] = ms
	}

	d.advance(ms, ev.Timestamp)

	idx := ev.Timestamp % d.windowSec
	ms.volSlots[idx] += ev.OutputAmt
	ms.cntSlots[idx]++
	ms.totalVol += ev.OutputAmt
	ms.totalCnt++

	cur := float64(ms.totalVol)
	if ms.ewma == 0 {
		ms.ewma = cur
	} else {
		ms.ewma = d.alpha*cur + (1.0-d.alpha)*ms.ewma
	}

	if ms.totalVol >= d.minVol && ms.totalCnt >= d.minTrades && ms.ewma > 0 && cur >= d.multiple*ms.ewma {
		// Put alert callback here (publish, log, webhook, etc.)
		_ = time.Now()
	}
}

func (d *SpikeDetector) advance(ms *mintState, toSec int64) {
	if toSec <= ms.lastSec {
		return
	}
	step := min(toSec-ms.lastSec, d.windowSec)
	for i := int64(1); i <= step; i++ {
		sec := ms.lastSec + i
		idx := sec % d.windowSec

		ms.totalVol -= ms.volSlots[idx]
		ms.totalCnt -= ms.cntSlots[idx]
		ms.volSlots[idx] = 0
		ms.cntSlots[idx] = 0
	}
	ms.lastSec = toSec
}
