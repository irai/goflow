package goflow

import (
	"log/slog"
)

var StartEvent = "__startEvent"
var StopEvent = "__stopEvent"
var ErrorEvent = "__errorEvent"

// event is a value container for a workflow event.
type event struct {
	ID     string
	Values any
	Error  error
	Logger *slog.Logger
	w      *Flow
	task   ITask
}

func (e event) RenderTemplate(name string, gotmpl string, values map[string]any) (string, error) {
	return e.w.RenderTemplate(name, gotmpl, values)
}

func (e event) Stream(v any) {
	if len(e.w.stream) == cap(e.w.stream) {
		e.Logger.Warn("workflow stream channel is full - skipping value")
		return
	}
	e.w.stream <- v
}

func (e event) Emit(eventID string, values any) {
	e.w.Emit(eventID, values)
}

func (e event) clone(task ITask) event {
	return event{ID: e.ID, Values: e.Values, Logger: e.Logger, w: e.w, task: task}
}
