package metadata

import (
	"context"
	"reflect"
	"testing"
)

func TestPairsMD(t *testing.T) {
	for _, test := range []struct {
		// input
		kv []interface{}
		// output
		md MD
	}{
		{[]interface{}{}, MD{}},
		{[]interface{}{"k1", "v1", "k1", "v2"}, MD{"k1": "v2"}},
	} {
		md := Pairs(test.kv...)
		if !reflect.DeepEqual(md, test.md) {
			t.Fatalf("Pairs(%v) = %v, want %v", test.kv, md, test.md)
		}
	}
}
func TestCopy(t *testing.T) {
	const key, val = "key", "val"
	orig := Pairs(key, val)
	copy := orig.Copy()
	if !reflect.DeepEqual(orig, copy) {
		t.Errorf("copied value not equal to the original, got %v, want %v", copy, orig)
	}
	orig[key] = "foo"
	if v := copy[key]; v != val {
		t.Errorf("change in original should not affect copy, got %q, want %q", v, val)
	}
}
func TestJoin(t *testing.T) {
	for _, test := range []struct {
		mds  []MD
		want MD
	}{
		{[]MD{}, MD{}},
		{[]MD{Pairs("foo", "bar")}, Pairs("foo", "bar")},
		{[]MD{Pairs("foo", "bar"), Pairs("foo", "baz")}, Pairs("foo", "bar", "foo", "baz")},
		{[]MD{Pairs("foo", "bar"), Pairs("foo", "baz"), Pairs("zip", "zap")}, Pairs("foo", "bar", "foo", "baz", "zip", "zap")},
	} {
		md := Join(test.mds...)
		if !reflect.DeepEqual(md, test.want) {
			t.Errorf("context's metadata is %v, want %v", md, test.want)
		}
	}
}

func TestWithContext(t *testing.T) {
	md := MD(map[string]interface{}{RemoteIP: "127.0.0.1", Color: "red", Mirror: true})
	c := NewContext(context.Background(), md)
	ctx := WithContext(c)
	md1, ok := FromContext(ctx)
	if !ok {
		t.Errorf("expect ok be true")
		t.FailNow()
	}
	if !reflect.DeepEqual(md1, md) {
		t.Errorf("expect md1 equal to md")
		t.FailNow()
	}
}

func TestBool(t *testing.T) {
	md := MD{RemoteIP: "127.0.0.1", Color: "red"}
	mdcontext := NewContext(context.Background(), md)
	if Bool(mdcontext, Mirror) {
		t.Errorf("expect mirror be false")
		t.FailNow()
	}

	mdcontext = NewContext(context.Background(), MD{Mirror: true})
	if !Bool(mdcontext, Mirror) {
		t.Errorf("expect mirror be true")
		t.FailNow()
	}

	mdcontext = NewContext(context.Background(), MD{Mirror: "true"})
	if !Bool(mdcontext, Mirror) {
		t.Errorf("expect mirror be true")
		t.FailNow()
	}

	mdcontext = NewContext(context.Background(), MD{Mirror: "1"})
	if !Bool(mdcontext, Mirror) {
		t.Errorf("expect mirror be true")
		t.FailNow()
	}

	mdcontext = NewContext(context.Background(), MD{Mirror: "0"})
	if Bool(mdcontext, Mirror) {
		t.Errorf("expect mirror be false")
		t.FailNow()
	}
}
func TestInt64(t *testing.T) {
	mdcontext := NewContext(context.Background(), MD{Mid: int64(1)})
	if Int64(mdcontext, Mid) != int64(1) {
		t.Errorf("expect mdcontext.Mid equal to 1")
	}
	mdcontext = NewContext(context.Background(), MD{Mid: int64(2)})
	if Int64(mdcontext, Mid) == int64(1) {
		t.Errorf("expect mdcontext.Mid not equal to 1")
	}
	mdcontext = NewContext(context.Background(), MD{Mid: 10})
	if Int64(mdcontext, Mid) == int64(10) {
		t.Errorf("expect mdcontext.Mid not equal to 10")
	}
}

func TestRange(t *testing.T) {
	for _, test := range []struct {
		filterFunc func(key string) bool
		md         MD
		want       MD
	}{
		{
			nil,
			Pairs("foo", "bar"),
			Pairs("foo", "bar"),
		},
		{
			IsOutgoingKey,
			Pairs("foo", "bar", RemoteIP, "127.0.0.1", Color, "red", Mirror, "false"),
			Pairs(RemoteIP, "127.0.0.1", Color, "red", Mirror, "false"),
		},
		{
			IsOutgoingKey,
			Pairs("foo", "bar", Caller, "app-feed", RemoteIP, "127.0.0.1", Color, "red", Mirror, "true"),
			Pairs(RemoteIP, "127.0.0.1", Color, "red", Mirror, "true"),
		},
		{
			IsIncomingKey,
			Pairs("foo", "bar", Caller, "app-feed", RemoteIP, "127.0.0.1", Color, "red", Mirror, "true"),
			Pairs(Caller, "app-feed", RemoteIP, "127.0.0.1", Color, "red", Mirror, "true"),
		},
	} {
		var mds []MD
		c := NewContext(context.Background(), test.md)
		ctx := WithContext(c)
		Range(ctx,
			func(key string, value interface{}) {
				mds = append(mds, Pairs(key, value))
			},
			test.filterFunc)
		rmd := Join(mds...)
		if !reflect.DeepEqual(rmd, test.want) {
			t.Fatalf("Range(%v) = %v, want %v", test.md, rmd, test.want)
		}
		if test.filterFunc == nil {
			var mds []MD
			Range(ctx,
				func(key string, value interface{}) {
					mds = append(mds, Pairs(key, value))
				})
			rmd := Join(mds...)
			if !reflect.DeepEqual(rmd, test.want) {
				t.Fatalf("Range(%v) = %v, want %v", test.md, rmd, test.want)
			}
		}
	}
}
