# Go Workflow Framework

## Overview

The Workflow framework is a simple, event-driven abstraction for building agentic applications in Go. It provides a flexible foundation for creating complex, multi-step processes with robust control flow patterns including sequential execution, branching, looping, and concurrency.

This method breaks down a large problem into smaller tasks, each handled by an LLM instance or agent. The orchestrating LLM delegates and composes responses.
This lightweight yet powerful workflow engine allows you to:

- Define task-based workflows with clear event-driven execution paths
- Build intelligent agents that can make decisions and take actions
- Connect multiple components in a structured, manageable way
- Handle errors gracefully with dedicated error channels
- Monitor execution with comprehensive logging

## Getting Started

### Installation

```bash
go get github.com/irai/rag/workflow
```

### Creating a Simple Workflow

```go
package main

import (
    "context"
    "fmt"
    "github.com/irai/rag/workflow"
)

func main() {
    w := workflow.NewWorkflow("SimpleExample")
    w.NewTask(singleShotTask).Subscribe(workflow.StartEvent)
    ctx := context.Background()
    result := w.Run(ctx)
    fmt.Printf("Workflow completed with result: %v\n", result.GetString("result"))
}

func (h *ChatAgent) singleShotTask(ctx context.Context, ev workflow.Event, c chan<- string) error {
	s, err := renderTemplate(h.chatTemplate, ev.Values)
	if err != nil {
		return "", nil, err
	}

	msg := llms.TextParts(llms.ChatMessageTypeHuman, s)
	result, _, err := h.model.GeneratContent([]llms.MessageContent{msg}, nil)
	if err != nil {
		return "", nil, err
	}

	values := ev.Values
	values["result"] = result
	ev.Emit(workflow.StopEvent, values)
	return nil
}

```

## Core Concepts

### Workflows

A Workflow is the main container for your application logic. It manages the execution of tasks and the flow of events between them.

```go
w := workflow.NewWorkflow("MyWorkflow")
w.SetLogLevel(slog.LevelDebug) // Set logging verbosity
```

### Tasks

Tasks are the units of work in your workflow. Each task is a function that receives a context and an event.

```go
w.NewTask(myTaskFunction).Subscribe("trigger_event").SetLabel("task_name")
```

### Events

Events are the signals that drive the workflow forward. Tasks can subscribe to specific events and emit new ones.

```go
// Emit an event with data
ev.Emit("my_event", map[string]any{"key": "value"})

// Access event data in a task
data := ev.GetString("key")
```

## Advanced Patterns

### Looping

```go
func loopTask(ctx context.Context, ev workflow.Event) (string, map[string]any, error) {
    count := ev.GetInt("counter", 0)
    if count < 5 {
        // Loop back by emitting the same event
        ev.Emit("loop", map[string]any{"counter": count + 1})
    } else {
        // Exit the loop
        ev.Emit("loop_complete", nil)
    }
    return nil
}
```

### Branching

```go
func decisionTask(ctx context.Context, ev workflow.Event) (string, map[string]any, error) {
    value := ev.GetInt("value")
    if value > 10 {
        ev.Emit("high_value", ev.Values)
    } else {
        ev.Emit("low_value", ev.Values)
    }
    return nil
}
```

### Concurrent Execution

While the current implementation runs tasks sequentially, you can enable concurrent execution by uncommenting the goroutine in the `Fire` method:

```go
for _, t := range tasks {
    go func(task *TaskEntry) {
        // Task execution...
    }(t)
}
```

### Error Handling

Workflows have built-in error channels for handling exceptions:

```go
func myTask(ctx context.Context, ev workflow.Event) (string, map[string]any, error) {
    // If something goes wrong
    if err := someOperation(); err != nil {
        return "", nil, err // This will emit an ErrorEvent
    }
    return nil
}
```

## Best Practices

1. **Give meaningful labels** to tasks for better debugging and logging
2. **Handle errors** at appropriate levels rather than letting them bubble up
3. **Set timeouts** using the `MaxDuration` property to prevent hanging workflows
4. **Use logging** to monitor workflow execution
5. **Design for idempotency** so tasks can be safely retried
