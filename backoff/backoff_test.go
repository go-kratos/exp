package backoff

import "testing"

func TestBackoff(t *testing.T) {
	b := New()
	for i := 0; i < 10; i++ {
		t.Log(b.Backoff(i))
	}
}
