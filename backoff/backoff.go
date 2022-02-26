package backoff

import (
	"math/rand"
	"time"
)

// defaultBackoff is a backoff configuration with the default values.
var defaultBackoff = New()

// Option is backoff option.
type Option func(*Exponential)

// WithBaseDelay with base delay duration.
func WithBaseDelay(d time.Duration) Option {
	return func(o *Exponential) {
		o.baseDelay = d
	}
}

// WithMaxDelay with max delay duratioin.
func WithMaxDelay(d time.Duration) Option {
	return func(o *Exponential) {
		o.maxDelay = d
	}
}

// WithMultiplier with multiplier factor.
func WithMultiplier(m float64) Option {
	return func(o *Exponential) {
		o.multiplier = m
	}
}

// WithJitter with jitter factor.
func WithJitter(j float64) Option {
	return func(o *Exponential) {
		o.jitter = j
	}
}

// Strategy defines the methodology for backing off after a retry failure.
type Strategy interface {
	// Backoff returns the amount of time to wait before the next retry given
	// the number of consecutive failures.
	Backoff(retries int) time.Duration
}

// Exponential implements exponential backoff algorithm as defined in
// https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md.
type Exponential struct {
	// baseDelay is the amount of time to backoff after the first failure.
	baseDelay time.Duration
	// multiplier is the factor with which to multiply backoffs after a
	// failed retry. Should ideally be greater than 1.
	multiplier float64
	// jitter is the factor with which backoffs are randomized.
	jitter float64
	// maxDelay is the upper bound of backoff delay.
	maxDelay time.Duration
}

// New new a Exponential backoff with default options.
func New(opts ...Option) Strategy {
	ex := &Exponential{
		baseDelay:  100 * time.Millisecond,
		maxDelay:   15 * time.Second,
		multiplier: 1.6,
		jitter:     0.2,
	}
	for _, o := range opts {
		o(ex)
	}
	return ex
}

// Backoff returns the amount of time to wait before the next retry given the
// number of retries.
func (bc Exponential) Backoff(retries int) time.Duration {
	if retries == 0 {
		return bc.baseDelay
	}
	backoff, max := float64(bc.baseDelay), float64(bc.maxDelay)
	for backoff < max && retries > 0 {
		backoff *= bc.multiplier
		retries--
	}
	if backoff > max {
		backoff = max
	}
	// Randomize backoff delays so that if a cluster of requests start at
	// the same time, they won't operate in lockstep.
	backoff *= 1 + bc.jitter*(rand.Float64()*2-1)
	if backoff < 0 {
		return 0
	}
	return time.Duration(backoff)
}

// Backoff returns the amount of time to wait before the next retry given the
// number of retries.
func Backoff(retries int) time.Duration {
	return defaultBackoff.Backoff(retries)
}
