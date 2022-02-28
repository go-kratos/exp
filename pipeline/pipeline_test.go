package pipeline

import (
	"context"
	"github.com/go-kratos/exp/internal/metadata"
	xtime "github.com/go-kratos/exp/internal/time"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestPipeline(t *testing.T) {
	conf := &Config{
		MaxSize:  3,
		Interval: xtime.Duration(time.Millisecond * 20),
		Buffer:   3,
		Worker:   10,
	}
	type recv[T any] struct {
		mirror string
		ch     int
		values map[string][]T
	}
	var runs []recv[interface{}]
	do := func(c context.Context, ch int, values map[string][]interface{}) {
		runs = append(runs, recv[interface{}]{
			mirror: metadata.String(c, metadata.Mirror),
			values: values,
			ch:     ch,
		})
	}
	split := func(s string) int {
		n, _ := strconv.Atoi(s)
		return n
	}
	p := NewPipeline[interface{}](conf)
	p.Do = do
	p.Split = split
	p.Start()
	p.Add(context.Background(), "1", 1)
	p.Add(context.Background(), "1", 2)
	p.Add(context.Background(), "11", 3)
	p.Add(context.Background(), "2", 3)
	time.Sleep(time.Millisecond * 60)
	mirrorCtx := metadata.NewContext(context.Background(), metadata.MD{metadata.Mirror: "1"})
	p.Add(mirrorCtx, "2", 3)
	time.Sleep(time.Millisecond * 60)
	p.SyncAdd(mirrorCtx, "5", 5)
	time.Sleep(time.Millisecond * 60)
	p.Close()
	expt := []recv[interface{}]{
		{
			mirror: "",
			ch:     1,
			values: map[string][]interface{}{
				"1":  {1, 2},
				"11": {3},
			},
		},
		{
			mirror: "",
			ch:     2,
			values: map[string][]interface{}{
				"2": {3},
			},
		},
		{
			mirror: "1",
			ch:     2,
			values: map[string][]interface{}{
				"2": {3},
			},
		},
		{
			mirror: "1",
			ch:     5,
			values: map[string][]interface{}{
				"5": {5},
			},
		},
	}
	if !reflect.DeepEqual(runs, expt) {
		t.Errorf("expect get %+v,\n got: %+v", expt, runs)
	}
}

func TestPipelineSmooth(t *testing.T) {
	conf := &Config{
		MaxSize:  100,
		Interval: xtime.Duration(time.Second),
		Buffer:   100,
		Worker:   10,
	}
	type result struct {
		index int
		ts    time.Time
	}
	var results []result
	do := func(c context.Context, index int, values map[string][]interface{}) {
		results = append(results, result{
			index: index,
			ts:    time.Now(),
		})
	}
	split := func(s string) int {
		n, _ := strconv.Atoi(s)
		return n
	}
	p := NewPipeline[interface{}](conf)
	p.Do = do
	p.Split = split
	p.Start()
	for i := 0; i < 10; i++ {
		p.Add(context.Background(), strconv.Itoa(i), 1)
	}
	time.Sleep(time.Millisecond * 1500)
	if len(results) != conf.Worker {
		t.Errorf("expect results equal worker")
		t.FailNow()
	}
	for i, r := range results {
		if i > 0 {
			if r.ts.Sub(results[i-1].ts) < time.Millisecond*20 {
				t.Errorf("expect runs be smooth")
				t.FailNow()
			}
		}
	}
}
