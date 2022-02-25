package group

import "fmt"

type Counter struct {
	Value int
}

func (c *Counter) Incr() {
	c.Value++
}

func ExampleGroup_Get() {
	group := NewGroup(func() *Counter {
		fmt.Println("Only Once")
		return &Counter{}
	})

	// Create a new Counter
	group.Get("pass").Incr()

	// Get the created Counter again.
	group.Get("pass").Incr()
	// Output:
	// Only Once
}

func ExampleGroup_Reset() {
	group := NewGroup(func() *Counter {
		return &Counter{}
	})

	// Reset the new function and clear all created objects.
	group.Reset(func() *Counter {
		fmt.Println("reset")
		return &Counter{}
	})

	// Create a new Counter
	group.Get("pass").Incr()
	// Output:reset
}
