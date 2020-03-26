package main

/*
Channel fanout adding reader processes after start

This design pattern could be used in a situation
where an external signal creates a new reader instance
to begin pulling messages off the channel

*/

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Fanner - In this example the "message" is an integer
//	the mutex and "closed" flag are for internal use
type Fanner struct {
	chans  []chan int
	mut    *sync.Mutex
	closed bool
}

// Add - returns a new read channel that will receive
//	messages sent after the call is made
func (f *Fanner) Add() (<-chan int, error) {
	if f.closed {
		return nil, fmt.Errorf("can't call Fanner.Add() on closed channel")
	}

	outchan := make(chan int)

	// Use a mutex to prevent multiple goroutines from editing the
	//	list of channels at the same time
	if f.mut == nil {
		f.mut = &sync.Mutex{}
	}

	f.mut.Lock()
	defer f.mut.Unlock()

	f.chans = append(f.chans, outchan)

	fmt.Println("Added new channel to Fanner")

	return outchan, nil
}

// This should be called with a defer to cleanly close all reader channels
func (f *Fanner) shutdown() {
	fmt.Println("Fanner shutting down")

	for i := 0; i < len(f.chans); i++ {
		close(f.chans[i])
	}

	f.closed = true
}

// Start the fan-out process with a channel to read from
//	will spawn a goroutine to handle the fanout
//	*NB:* the goroutine can be cleanly stopped using context cancellation
func (f *Fanner) Start(ctx context.Context, inChan <-chan int) {
	f.closed = false

	go func() {
		defer f.shutdown()

		for d := range inChan {
			for i := 0; i < len(f.chans); i++ {
				select {
				case <-ctx.Done():
					fmt.Println("Fanout shutting down")
					return
				case f.chans[i] <- d:
				}
			}
		}
	}()
}

// gen - an example generator method to pump data into the pipeline
//	Here it simply passes in integers up to a given max
func gen(ctx context.Context, max int) chan int {
	c := make(chan int)

	go func() {
		defer close(c)

		for i := 0; i < max; i++ {
			select {
			case <-ctx.Done():
				fmt.Println("generator shutting down")
				return
			case c <- i:
				time.Sleep(time.Millisecond * 500) // Do some work
			}
		}
	}()

	return c
}

// reader - an example reader function
//	Just print the data as it arrives
func reader(idx int, ic <-chan int) {
	for d := range ic {
		fmt.Printf("%d) Got %d\n", idx, d)
	}
	fmt.Printf("Reader %d shutting down\n", idx)
}

func main() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc() // Safe to call multiple times

	// Instantiate an empty Fanner struct and use its methods
	f := &Fanner{}
	wg := &sync.WaitGroup{}

	// Start the generator function - will block until readers become available
	genChan := gen(ctx, 10)

	// Start a couple of reader routines
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := f.Add() // returns a channel
		if err != nil {
			fmt.Printf("Got error: %v\n", err.Error())
			return
		}
		reader(1, c) // This example reader accepts an index for display purposes only
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		c, err := f.Add()
		if err != nil {
			fmt.Printf("Got error: %v\n", err.Error())
			return
		}
		reader(2, c)
	}()

	// Start the fan-out process and unlock the writer
	f.Start(ctx, genChan)

	// Some time passes while other operations happen
	//	Or a signal is awaited
	time.Sleep(time.Second * 2)

	// Start another reader
	wg.Add(1)
	go func() {
		defer wg.Done()

		c, err := f.Add()
		if err != nil {
			fmt.Printf("Got error: %v\n", err.Error())
			return
		}
		reader(3, c)
	}()

	time.Sleep(time.Second * 2)
	cancelFunc() // Stop the process to see completion

	fmt.Println("waiting for completion")
	wg.Wait()
}
