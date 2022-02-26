package retry

import (
	"context"
	"errors"
	"testing"
)

func TestRetry(t *testing.T) {
	r := New()
	{
		if err := r.Do(context.Background(), func(ctx context.Context) error {
			t.Log("retry ok")
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	{
		n := 0
		err := r.Do(context.Background(), func(ctx context.Context) error {
			n++
			t.Log("retry:", n)
			return errors.New("retry")
		})
		if err == nil {
			t.Log("should return an error")
		}
		if n != r.attempts {
			t.Logf("should want %d but got %d", r.attempts, n)
		}
	}
	{
		n := 0
		err := Do(context.Background(), func(ctx context.Context) error {
			n++
			return errors.New("not retry")
		}, WithRetryable(func(err error) bool {
			return false
		}))
		if err == nil {
			t.Log("should got an error")
		}
		if n != 1 {
			t.Log("should not retry")
		}
	}
}
