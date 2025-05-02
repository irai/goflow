package goflow

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestFlow_nil(t *testing.T) {
	w := NewFlow("test1")

	type testStruct struct {
		Name string
	}

	var tt ITask = NewTask(w,
		func(ctx context.Context, ev string) (testStruct, error) {
			return testStruct{Name: ev + " worked"}, nil
		}).Subscribe(StartEvent)

	ev, _ := tt.Run(context.Background(), "test")

	if ev.(testStruct).Name != "test worked" {
		t.Errorf("unexpected sequence got: %s, want: %s", ev.(testStruct).Name, "test worked")
	}
}

func TestFlow_single(t *testing.T) {
	w := NewFlow("test1")

	type testStruct struct {
		Name string
	}

	var tt ITask = NewTask(w,
		func(ctx context.Context, ev testStruct) (testStruct, error) {
			return testStruct{Name: ev.Name + " worked"}, nil
		}).Subscribe(StartEvent)

	ev, _ := tt.Run(context.Background(), testStruct{Name: "test"})

	if ev.(testStruct).Name != "test worked" {
		t.Errorf("unexpected sequence got: %s, want: %s", ev.(testStruct).Name, "test worked")
	}
}

func TestFlow_basic(t *testing.T) {
	w := NewFlow("test1")

	NewTask(w,
		func(ctx context.Context, ev map[string]any) (map[string]any, error) {
			return map[string]any{"result": "first message"}, nil
		}).Subscribe(StartEvent).Emit("secondTask").SetLabel("firstTask")

	NewTask(w,
		func(ctx context.Context, ev map[string]any) (map[string]any, error) {
			seq := ev["result"].(string)
			return map[string]any{"result": seq + "second message"}, nil
		}).Subscribe("secondTask").Emit(StopEvent)

	ev, err := w.Run(context.Background(), map[string]any{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	ev1 := ev.(map[string]any)

	if ev1["result"] != "first messagesecond message" {
		t.Errorf("unexpected sequence got: %s, want: %s", ev1["result"], "first messagesecond message")
	}
}

func TestFlow_timeout(t *testing.T) {
	w := NewFlow("test1")
	w.MaxDuration = 100 * time.Millisecond

	NewTask(w,
		func(ctx context.Context, ev map[string]any) (map[string]any, error) {
			return map[string]any{"result": "first message"}, nil
		}).Subscribe(StartEvent).Subscribe("loop").Emit("secondTask")

	NewTask(w,
		func(ctx context.Context, ev map[string]any) (string, error) {
			time.Sleep(200 * time.Millisecond)
			return "timeout", nil
		}).Subscribe("secondTask").Emit("loop")

	_, err := w.Run(context.Background(), nil)
	if err != ErrTimeout {
		t.Errorf("unexpected sequence want: error")
	}
}

func TestFlow_maxtasks(t *testing.T) {
	w := NewFlow("test1")
	w.MaxTasks = 10

	NewTask(w,
		func(ctx context.Context, ev map[string]any) (string, error) {
			return "first message", nil
		}).Subscribe(StartEvent).Subscribe("loop").Emit("secondTask")

	NewTask(w,
		func(ctx context.Context, ev string) (map[string]any, error) {
			time.Sleep(10 * time.Millisecond)
			return nil, nil
		}).Subscribe("secondTask").Emit("loop")

	_, err := w.Run(context.Background(), nil)
	if err != ErrMaxTasks {
		t.Errorf("unexpected sequence want: error")
	}
}

func TestFlow_loop(t *testing.T) {
	w := NewFlow("test1")

	var mu sync.Mutex
	var wg sync.WaitGroup

	var results []string

	NewTask(w,
		func(ctx context.Context, n int) (int, error) {
			for i := range n {
				wg.Add(1)
				w.Emit("loop", "sequence"+strconv.Itoa(i))
			}
			return n, nil
		}).Subscribe(StartEvent).Emit("finish")

	NewTask(w,
		func(ctx context.Context, n int) ([]string, error) {
			wg.Wait()
			return results, nil
		}).Subscribe("finish").Emit(StopEvent)

	NewTask(w,
		func(ctx context.Context, name string) (any, error) {
			defer wg.Done()

			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			results = append(results, name)
			mu.Unlock()
			return nil, nil
		}).Subscribe("loop")

	ev, err := w.Run(context.Background(), 10)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if got, want := ev.([]string), 10; len(got) != want {
		t.Errorf("unexpected sequence got: %v, want: %v", got, want)
	}

}
