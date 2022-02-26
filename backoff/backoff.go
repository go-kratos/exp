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
		o.BaseDelay = d
	}
}

// WithMaxDelay with max delay duratioin.
func WithMaxDelay(d time.Duration) Option {
	return func(o *Exponential) {
		o.MaxDelay = d
	}
}

// WithMultiplier with multiplier factor.
func WithMultiplier(m float64) Option {
	return func(o *Exponential) {
		o.Multiplier = m
	}
}

// WithJitter with jitter factor.
func WithJitter(j float64) Option {
	return func(o *Exponential) {
		o.Jitter = j
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
	// BaseDelay is the amount of time to backoff after the first failure.
	BaseDelay time.Duration
	// Multiplier is the factor with which to multiply backoffs after a
	// failed retry. Should ideally be greater than 1.
	Multiplier float64
	// Jitter is the factor with which backoffs are randomized.
	Jitter float64
	// MaxDelay is the upper bound of backoff delay.
	MaxDelay time.Duration
}

// New new a Exponential backoff with default options.
func New(opts ...Option) Strategy {
	ex := &Exponential{
		BaseDelay:  150 * time.Millisecond,
		MaxDelay:   15 * time.Second,
		Multiplier: 1.6,
		Jitter:     0.2,
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
		return bc.BaseDelay
	}
	backoff, max := float64(bc.BaseDelay), float64(bc.MaxDelay)
	for backoff < max && retries > 0 {
		backoff *= bc.Multiplier
		retries--
	}
	if backoff > max {
		backoff = max
	}
	// Randomize backoff delays so that if a cluster of requests start at
	// the same time, they won't operate in lockstep.
	backoff *= 1 + bc.Jitter*(rand.Float64()*2-1)
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
