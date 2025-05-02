package goflow

import (
	"bytes"
	"context"
	"errors"
	"html/template"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultMaxTasks  = 256              // Maximum number of tasks to run in a single workflow
	DefaultMaxDur    = time.Minute * 60 // Maximum workflow duration
	DefaultMaxEvents = 256              // Maximum number of events to buffer
)

type Flow struct {
	label        string
	tasks        map[string]ITask
	events       map[string][]ITask
	templates    map[string]*template.Template
	c            chan event
	errorC       chan event
	Logger       *slog.Logger
	logLevel     *slog.LevelVar
	mu           sync.RWMutex
	MaxDuration  time.Duration
	MaxTasks     int32
	totalTasks   int32
	runningTasks int32
	timeout      time.Time
	ctx          context.Context
	stream       chan any
}

func NewFlow(label string) *Flow {
	w := &Flow{label: label}
	w.c = make(chan event, DefaultMaxEvents)
	w.stream = make(chan any, 16)
	w.errorC = make(chan event, 8)
	w.tasks = make(map[string]ITask)
	w.events = make(map[string][]ITask)
	w.templates = make(map[string]*template.Template)
	w.logLevel = new(slog.LevelVar)
	w.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: w.logLevel})).With("workflow", w.label)
	w.MaxTasks = DefaultMaxTasks
	w.MaxDuration = DefaultMaxDur

	return w
}

func (w *Flow) SetLogLevel(level slog.Level) {
	w.logLevel.Set(level)
}

func (w *Flow) Stream() any {
	v, ok := <-w.stream
	if ok {
		return v
	}
	return nil
}

func (w *Flow) RenderTemplate(name string, gotmpl string, values map[string]any) (string, error) {
	w.mu.Lock()
	tmpl, ok := w.templates[name]
	w.mu.Unlock()
	if !ok {
		var err error
		tmpl, err = template.New(name).Parse(gotmpl)
		if err != nil {
			return "", err
		}
		w.mu.Lock()
		w.templates[name] = tmpl
		w.mu.Unlock()
	}

	var buf bytes.Buffer
	err := tmpl.Execute(&buf, values)
	return buf.String(), err
}

func (w *Flow) Run(ctx context.Context, values any) (any, error) {
	w.ctx = ctx
	w.timeout = time.Now().Add(w.MaxDuration)
	w.Emit(StartEvent, values)

	workerChan := make(chan event, 32)
	// defer close(workerChan) // TOD: this is causing an error in the stack? why?

	go w.worker(workerChan)
	go w.worker(workerChan)
	go w.worker(workerChan)
	go w.worker(workerChan)
	go w.worker(workerChan)

	for {

		if atomic.LoadInt32(&w.totalTasks) >= w.MaxTasks {
			err := ErrMaxTasks
			w.emitError(ErrorEvent, err, nil)
		}

		ev := w.nextEvent()
		if ev.ID == StopEvent || ev.ID == ErrorEvent {
			close(workerChan)
			return ev.Values, ev.Error
		}

		w.mu.RLock()
		events, ok := w.events[ev.ID]
		w.mu.RUnlock()

		if !ok {
			w.Logger.Error("event has no tasks", "event", ev.ID)
			continue
		}

		for _, t := range events {
			e := ev.clone(t) // clone event for task
			e.Logger = w.Logger.With("event", e.ID, "task", t.Label())
			workerChan <- e
		}

	}
}

var (
	ErrChannelClosed = errors.New("channel closed")
	ErrTimeout       = errors.New("timeout occurred")
	ErrMaxTasks      = errors.New("max tasks reached")
	ErrInvalidInput  = errors.New("invalid input type")
)

func (w *Flow) nextEvent() event {

	select {
	case ev, ok := <-w.errorC:
		if !ok {
			ev = event{ID: ErrorEvent, Logger: w.Logger, Error: ErrChannelClosed, Values: map[string]any{"error": "channel closed"}}
		}
		w.Logger.Error("error event received", "event", ev.ID, "error", ev.Error)
		return ev

	case ev, ok := <-w.c:
		if !ok {
			ev = event{ID: ErrorEvent, Logger: w.Logger, Error: ErrChannelClosed, Values: map[string]any{"error": "channel closed"}}
		}
		w.Logger.Debug("event received", "event", ev.ID)
		return ev

	case <-time.After(time.Until(w.timeout)):
		w.Logger.Error("timeout while waiting for events")
		return event{ID: ErrorEvent, Logger: w.Logger, Error: ErrTimeout, Values: map[string]any{"error": "timeout occurred"}}
	}

}

func (w *Flow) Emit(eventID string, values any) {
	w.Logger.Debug("emitting event", "event", eventID)
	if len(w.c) >= DefaultMaxEvents {
		panic("max events reached. deadlock?")
	}
	w.c <- event{ID: eventID, Values: values, Logger: w.Logger, w: w}
}

func (w *Flow) emitError(eventID string, err error, triggerEvent *event) {
	values := map[string]any{"error": err.Error()}
	if triggerEvent != nil {
		values["triggerEvent"] = triggerEvent.ID
		if triggerEvent.task != nil {
			values["taskID"] = triggerEvent.task.Label()
		}
	}
	w.errorC <- event{ID: eventID, Values: values, Logger: w.Logger, Error: err, w: w}
}

func (w *Flow) worker(c <-chan event) {
	defer w.Logger.Warn("worker channel closed - stopping worker")

	for {
		ev, ok := <-c
		if !ok { //channel is closed - terminate worker
			return
		}

		atomic.AddInt32(&w.totalTasks, 1)
		ev.Logger.Info("task started")
		atomic.AddInt32(&w.runningTasks, 1)

		values, err := ev.task.Run(w.ctx, ev.Values)

		atomic.AddInt32(&w.runningTasks, -1)

		if err != nil {
			ev.Logger.Error("task failed", "error", err)
			w.emitError(ErrorEvent, err, &ev)
			continue
		}

		ev.Logger.Info("task completed successful")

		if len(ev.task.Emitting()) > 0 {
			ev.Emit(ev.task.Emitting()[0], values)
		}
	}
}
