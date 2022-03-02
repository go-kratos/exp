package pipeline

import "time"

type Option func(o *options)

type options struct {
	// MaxSize merge size
	MaxSize int
	// Interval merge interval
	Interval time.Duration
	// Buffer channel size
	Buffer int
	// Worker channel number
	Worker int
	// Name use for metrics
	Name string
}

// WithMaxSize set max merge size
func WithMaxSize(size int) Option {
	return func(o *options) {
		o.MaxSize = size
	}
}

// WithInterval set merge interval
func WithInterval(interval time.Duration) Option {
	return func(o *options) {
		o.Interval = interval
	}
}

//WithBuffer set channel size
func WithBuffer(buffer int) Option {
	return func(o *options) {
		o.Buffer = buffer
	}
}

//WithWorker set worker number
func WithWorker(worker int) Option {
	return func(o *options) {
		o.Worker = worker
	}
}

//WithName set name
func WithName(name string) Option {
	return func(o *options) {
		o.Name = name
	}
}
