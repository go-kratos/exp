package retry

import (
	"context"
	"time"

	"github.com/go-kratos/exp/backoff"
)

// defaultRetry is a backoff configuration with the default values.
var defaultRetry = New()

// Option is retry option.
type Option func(*Retry)

// WithAttempts with attempts.
func WithAttempts(n int) Option {
	return func(o *Retry) {
		o.attempts = n
	}
}

// WithRetryable with retryable.
func WithRetryable(r Retryable) Option {
	return func(o *Retry) {
		o.retryable = r
	}
}

// WithBackoff with backoff.
func WithBackoff(b backoff.Strategy) Option {
	return func(o *Retry) {
		o.backoff = b
	}
}

// Retryable is used to judge whether an error is retryable or not.
type Retryable func(err error) bool

// Retry config.
type Retry struct {
	backoff   backoff.Strategy
	retryable Retryable
	attempts  int
}

// New new a retry with backoff.
func New(opts ...Option) *Retry {
	r := &Retry{
		attempts:  2,
		retryable: func(err error) bool { return true },
		backoff:   backoff.New(),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Do wraps func with a backoff to retry.
func (r *Retry) Do(ctx context.Context, fn func(context.Context) error) error {
	var (
		err     error
		retries int
	)
	for {
		if err = ctx.Err(); err != nil {
			break
		}
		if err = fn(ctx); err == nil {
			break
		}
		if err != nil && !r.retryable(err) {
			break
		}
		retries++
		if r.attempts > 0 && retries >= r.attempts {
			break
		}
		time.Sleep(r.backoff.Backoff(retries))
	}
	return err
}

// Do wraps func with a backoff to retry.
func Do(ctx context.Context, fn func(context.Context) error) error {
	return defaultRetry.Do(ctx, fn)
}

// Infinite wraps func with a backoff to retry.
func Infinite(ctx context.Context, fn func(context.Context) error) error {
	r := New(WithAttempts(-1))
	return r.Do(ctx, fn)
}
