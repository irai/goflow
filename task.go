package goflow

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
)

type Func[In, Out any] = func(context.Context, In) (Out, error)

type ITask interface {
	Label() string
	Run(context.Context, any) (any, error)
	Emitting() []string
}

var _ ITask = &Task[any, any]{}

type Task[in, out any] struct {
	id          string
	label       string
	retryPolicy string
	// f           func(context.Context, Event) (map[string]any, error)
	fn         Func[in, out]
	w          *Flow
	emitting   []string
	isFlow     bool
	concurrent bool
	maxRepeat  int
}

func methodName(f any) string {
	fName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	parts := strings.Split(fName, ".")
	methodName := parts[len(parts)-1] // Get the last part (method name)
	if len(parts) > 1 {
		receiverType := filepath.Base(parts[len(parts)-2]) // Get the second to last part (receiver type)
		methodName = receiverType + "." + methodName
	}
	return methodName
}

// func (w *Workflow) NewTask(f func(context.Context, Event) (map[string]any, error)) *Task {
func NewTask[In, Out any](w *Flow, f Func[In, Out]) *Task[In, Out] {
	w.mu.Lock()
	defer w.mu.Unlock()

	t := &Task[In, Out]{w: w, fn: f}
	t.id = strconv.Itoa(len(w.tasks) + 1)
	t.label = t.id + " " + methodName(f)
	w.tasks[t.id] = t
	t.emitting = []string{}
	t.maxRepeat = 8
	return t
}

func (t *Task[In, Out]) Label() string {
	return t.label
}

func (t *Task[In, Out]) Emitting() []string {
	return t.emitting
}

func (t *Task[In, Out]) IsFlow() bool {
	return t.isFlow
}

func (t *Task[In, Out]) Run(ctx context.Context, input any) (output any, err error) {
	if input == nil {
		input = *new(In) // default to nil type
	}
	if in, ok := input.(In); !ok {
		return nil, fmt.Errorf("%w: task: %s input %T is not of type %T", ErrInvalidInput, t.label, input, in)
	} else {
		return t.fn(ctx, in)
	}
}

func (t *Task[In, Out]) RunJSON(ctx context.Context, input json.RawMessage) (output json.RawMessage, err error) {
	var in In
	if input != nil {
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, err
		}
	}
	out, err := t.fn(ctx, in)
	if err != nil {
		return nil, err
	}
	bytes, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(bytes), nil
}

func (t *Task[In, Out]) SetRetryPolicy(policy string) *Task[In, Out] {
	t.retryPolicy = policy
	return t
}

func (t *Task[In, Out]) SetLabel(label string) *Task[In, Out] {
	t.label = label
	return t
}

func (t *Task[In, Out]) AsGoroutine() *Task[In, Out] {
	t.concurrent = true
	return t
}

func (t *Task[In, Out]) Subscribe(eventID string) *Task[In, Out] {
	if eventID == StopEvent {
		return t
	}
	t.w.mu.Lock()
	defer t.w.mu.Unlock()

	t.w.events[eventID] = append(t.w.events[eventID], t)
	return t
}

func (t *Task[In, Out]) Emit(eventID string) *Task[In, Out] {
	if eventID == StartEvent {
		return t
	}
	t.w.mu.Lock()
	defer t.w.mu.Unlock()

	t.emitting = append(t.emitting, eventID)
	return t
}

func (t *Task[In, Out]) EmitMultiple(eventIDs ...string) *Task[In, Out] {
	t.w.mu.Lock()
	defer t.w.mu.Unlock()

	for _, eventID := range eventIDs {
		if eventID != StartEvent {
			t.emitting = append(t.emitting, eventID)
		}
	}
	t.isFlow = true
	return t
}
