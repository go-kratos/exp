package pipeline

import (
	"context"
	"errors"
	"github.com/go-kratos/exp/internal/metadata"
	"github.com/go-kratos/kratos/v2/metrics"
	"strconv"
	"sync"
	"time"

	xtime "github.com/go-kratos/exp/internal/time"
)

// ErrFull channel full error
var ErrFull = errors.New("channel full")

type message[T any] struct {
	key   string
	value T
}

// Pipeline pipeline struct
type Pipeline[T any] struct {
	Do            func(c context.Context, index int, values map[string][]T)
	Split         func(key string) int
	chans         []chan *message[T]
	mirrorChans   []chan *message[T]
	config        *Config
	wait          sync.WaitGroup
	name          string
	metricCount   metrics.Counter
	metricChanLen metrics.Gauge
}

// Config Pipeline config
type Config struct {
	// MaxSize merge size
	MaxSize int
	// Interval merge interval
	Interval xtime.Duration
	// Buffer channel size
	Buffer int
	// Worker channel number
	Worker int
	// Name use for metrics
	Name string
	// MetricCount use for metrics
	MetricCount metrics.Counter
	// MetricChanLen use for metrics
	MetricChanLen metrics.Gauge
}

func (c *Config) fix() {
	if c.MaxSize <= 0 {
		c.MaxSize = 1000
	}
	if c.Interval <= 0 {
		c.Interval = xtime.Duration(time.Second)
	}
	if c.Buffer <= 0 {
		c.Buffer = 1000
	}
	if c.Worker <= 0 {
		c.Worker = 10
	}
	if c.Name == "" {
		c.Name = "anonymous"
	}
}

// NewPipeline new pipline
func NewPipeline[T any](config *Config) (res *Pipeline[T]) {
	if config == nil {
		config = &Config{}
	}
	config.fix()
	res = &Pipeline[T]{
		chans:         make([]chan *message[T], config.Worker),
		mirrorChans:   make([]chan *message[T], config.Worker),
		config:        config,
		name:          config.Name,
		metricCount:   config.MetricCount,
		metricChanLen: config.MetricChanLen,
	}
	for i := 0; i < config.Worker; i++ {
		res.chans[i] = make(chan *message[T], config.Buffer)
		res.mirrorChans[i] = make(chan *message[T], config.Buffer)
	}
	return
}

// Start start all mergeproc
func (p *Pipeline[T]) Start() {
	if p.Do == nil {
		panic("pipeline: do func is nil")
	}
	if p.Split == nil {
		panic("pipeline: split func is nil")
	}
	var mirror bool
	p.wait.Add(len(p.chans) + len(p.mirrorChans))
	for i, ch := range p.chans {
		go p.mergeproc(mirror, i, ch)
	}
	mirror = true
	for i, ch := range p.mirrorChans {
		go p.mergeproc(mirror, i, ch)
	}
}

// SyncAdd sync add a value to channal, channel shard in split method
func (p *Pipeline[T]) SyncAdd(c context.Context, key string, value T) (err error) {
	ch, msg := p.add(c, key, value)
	select {
	case ch <- msg:
	case <-c.Done():
		err = c.Err()
	}
	return
}

// Add async add a value to channal, channel shard in split method
func (p *Pipeline[T]) Add(c context.Context, key string, value T) (err error) {
	ch, msg := p.add(c, key, value)
	select {
	case ch <- msg:
	default:
		err = ErrFull
	}
	return
}

func (p *Pipeline[T]) add(c context.Context, key string, value T) (ch chan *message[T], m *message[T]) {
	shard := p.Split(key) % p.config.Worker
	if metadata.String(c, metadata.Mirror) != "" {
		ch = p.mirrorChans[shard]
	} else {
		ch = p.chans[shard]
	}
	m = &message[T]{key: key, value: value}
	return
}

// Close all goroutinue
func (p *Pipeline[T]) Close() (err error) {
	for _, ch := range p.chans {
		ch <- nil
	}
	for _, ch := range p.mirrorChans {
		ch <- nil
	}
	p.wait.Wait()
	return
}

func (p *Pipeline[T]) mergeProc(mirror bool, index int, ch <-chan *message[T]) {
	defer p.wait.Done()
	var (
		m       *message[T]
		vals    = make(map[string][]T, p.config.MaxSize)
		closed  bool
		count   int
		inteval = p.config.Interval
		timeout = false
	)
	if index > 0 {
		inteval = xtime.Duration(int64(index) * (int64(p.config.Interval) / int64(p.config.Worker)))
	}
	timer := time.NewTimer(time.Duration(inteval))
	defer timer.Stop()
	for {
		select {
		case m = <-ch:
			if m == nil {
				closed = true
				break
			}
			count++
			vals[m.key] = append(vals[m.key], m.value)
			if count >= p.config.MaxSize {
				break
			}
			continue
		case <-timer.C:
			timeout = true
		}
		name := p.name
		process := count
		if len(vals) > 0 {
			ctx := context.Background()
			if mirror {
				ctx = metadata.NewContext(ctx, metadata.MD{metadata.Mirror: "1"})
				name = "mirror_" + name
			}
			p.Do(ctx, index, vals)
			vals = make(map[string][]T, p.config.MaxSize)
			count = 0
		}
		if p.metricChanLen != nil {
			p.metricChanLen.With(name, strconv.Itoa(index)).Set(float64(len(ch)))
		}
		if p.metricCount != nil {
			p.metricCount.With(name, strconv.Itoa(index)).Add(float64(process))
		}
		if closed {
			return
		}
		if !timer.Stop() && !timeout {
			<-timer.C
			timeout = false
		}
		timer.Reset(time.Duration(p.config.Interval))
	}
}
