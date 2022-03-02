package pipeline

import (
	"context"
	"errors"
	"github.com/go-kratos/exp/internal/metadata"
	"sync"
	"time"
)

// ErrFull channel full error
var ErrFull = errors.New("channel full")

// mirrorKey
const mirrorKey = "mirror"

type message[T any] struct {
	key   string
	value T
}

// Pipeline pipeline struct
type Pipeline[T any] struct {
	Do          func(c context.Context, index int, values map[string][]T)
	Split       func(key string) int
	chans       []chan *message[T]
	mirrorChans []chan *message[T]
	wait        sync.WaitGroup
	name        string
	opt         *options
}

// NewPipeline new pipline
func NewPipeline[T any](opts ...Option) (res *Pipeline[T]) {
	o := &options{
		MaxSize:  1000,
		Interval: time.Second,
		Buffer:   1000,
		Worker:   10,
		Name:     "anonymous",
	}
	for _, opt := range opts {
		opt(o)
	}

	res = &Pipeline[T]{
		chans:       make([]chan *message[T], o.Worker),
		mirrorChans: make([]chan *message[T], o.Worker),
		name:        o.Name,
		opt:         o,
	}
	for i := 0; i < o.Worker; i++ {
		res.chans[i] = make(chan *message[T], o.Buffer)
		res.mirrorChans[i] = make(chan *message[T], o.Buffer)
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
		go p.mergeProc(mirror, i, ch)
	}
	mirror = true
	for i, ch := range p.mirrorChans {
		go p.mergeProc(mirror, i, ch)
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
	shard := p.Split(key) % p.opt.Worker
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
		vals    = make(map[string][]T, p.opt.MaxSize)
		closed  bool
		count   int
		inteval = p.opt.Interval
		timeout = false
	)
	if index > 0 {
		inteval = time.Duration(int64(index) * (int64(p.opt.Interval) / int64(p.opt.Worker)))
	}
	timer := time.NewTimer(inteval)
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
			if count >= p.opt.MaxSize {
				break
			}
			continue
		case <-timer.C:
			timeout = true
		}
		name := p.name
		if len(vals) > 0 {
			ctx := context.Background()
			if mirror {
				ctx = metadata.NewContext(ctx, metadata.MD{metadata.Mirror: "1"})
				name = "mirror_" + name
			}
			p.Do(ctx, index, vals)
			vals = make(map[string][]T, p.opt.MaxSize)
			count = 0
		}
		if closed {
			return
		}
		if !timer.Stop() && !timeout {
			<-timer.C
			timeout = false
		}
		timer.Reset(time.Duration(p.opt.Interval))
	}
}
