package main

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/irai/goflow"
)

func main() {

	w := goflow.NewFlow("simple")
	var mu sync.Mutex
	var wg sync.WaitGroup

	var collector []string

	startTask := func(ctx context.Context, n int) (int, error) {
		for i := range n {
			wg.Add(1)
			w.Emit("loop", "sequence"+strconv.Itoa(i))
		}
		return n, nil
	}

	collectTask := func(ctx context.Context, n int) ([]string, error) {
		wg.Wait()
		return collector, nil
	}

	loopTask := func(ctx context.Context, name string) (any, error) {
		defer wg.Done()

		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		collector = append(collector, name)
		mu.Unlock()
		return nil, nil
	}

	goflow.NewTask(w, startTask).Subscribe(goflow.StartEvent).Emit("collect")
	goflow.NewTask(w, collectTask).Subscribe("collect").Emit(goflow.StopEvent)
	goflow.NewTask(w, loopTask).Subscribe("loop")

	results, err := w.Run(context.Background(), 10)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}

	log.Printf("results: %v", results)
}
