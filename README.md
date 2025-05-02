# Go Workflow Framework

## Overview

The Workflow framework is a simple, event-driven abstraction for building applications in Go. It provides a flexible foundation for creating complex, multi-step processes with robust control flow patterns including sequential execution, branching, looping, and concurrency.

This method breaks down a large problem into smaller tasks.

- Define task-based workflows with clear event-driven execution paths
- Build intelligent agents that can make decisions and take actions
- Connect multiple components in a structured, manageable way
- Handle errors gracefully with dedicated error channels

## Getting Started

### Creating a Simple Workflow

Here's a basic example demonstrating a simple workflow:

```go
package main

import (
	"context"
	"log"

	"github.com/irai/goflow"
)

func main() {
	// Create a new workflow named "simple-example"
	w := goflow.NewFlow("simple-example")

	startTask := func(ctx context.Context, input string) (string, error) {
		log.Println("Starting workflow with:", input)
		return "Hello from Start", nil
	}

	nextTask := func(ctx context.Context, message string) (string, error) {
		log.Println("Next task received:", message)
		return "Finished!", nil
	}

	// Register tasks, define subscriptions and emissions
	goflow.NewTask(w, startTask).
		Subscribe(goflow.StartEvent). // Triggered when workflow starts
		Emit("nextStep")             // Emits "nextStep" event on completion

	goflow.NewTask(w, nextTask).
		Subscribe("nextStep").      // Triggered by the "nextStep" event
		Emit(goflow.StopEvent)       // Emits StopEvent to end the workflow

	results, err := w.Run(context.Background(), "Initial Data")
	if err != nil {
		log.Fatalf("Workflow failed: %v", err)
	}

	log.Printf("Workflow finished. Results: %v", results)
}

```

For more complex examples, see the `examples` folder.

## Core Concepts

The framework revolves around three main components:

*   **Workflow:** The central orchestrator. It holds the definition of tasks and their relationships, manages the event queue, and controls the overall execution flow. You create a workflow instance using `goflow.NewFlow("workflow-name")`.
*   **Task:** A unit of work represented by a Go function. Each task performs a specific action. Tasks are registered within a workflow using `goflow.NewTask()`. A task function typically accepts a `context.Context` and an input data payload, and can return output data and an error.
*   **Event:** A signal that triggers tasks. Events are strings that act as communication channels between tasks.
    *   Tasks **subscribe** to specific events using `.Subscribe("event-name")`. When an event is emitted, all subscribed tasks are triggered concurrently (unless configured otherwise).
    *   Tasks can **emit** new events upon completion using `.Emit("event-name")`, triggering subsequent tasks.
    *   Special built-in events like `goflow.StartEvent` and `goflow.StopEvent` mark the beginning and end of the workflow execution.

Workflows run by calling the `w.Run(ctx, initialData)` method, which injects the initial data and triggers the task(s) subscribed to `goflow.StartEvent`. The workflow continues until a task emits `goflow.StopEvent` or an error occurs.

