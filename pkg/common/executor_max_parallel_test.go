package common

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Simple fast test that verifies max-parallel: 2 limits concurrency
func TestMaxParallel2Quick(t *testing.T) {
	ctx := context.Background()

	var currentRunning int32
	var maxSimultaneous int32

	executors := make([]Executor, 4)
	for i := 0; i < 4; i++ {
		executors[i] = func(ctx context.Context) error {
			current := atomic.AddInt32(&currentRunning, 1)

			// Update max if needed
			for {
				maxValue := atomic.LoadInt32(&maxSimultaneous)
				if current <= maxValue || atomic.CompareAndSwapInt32(&maxSimultaneous, maxValue, current) {
					break
				}
			}

			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&currentRunning, -1)
			return nil
		}
	}

	err := NewParallelExecutor(2, executors...)(ctx)

	assert.NoError(t, err)
	assert.LessOrEqual(t, atomic.LoadInt32(&maxSimultaneous), int32(2),
		"Should not exceed max-parallel: 2")
}

// Test that verifies max-parallel: 1 enforces sequential execution
func TestMaxParallel1Sequential(t *testing.T) {
	ctx := context.Background()

	var currentRunning int32
	var maxSimultaneous int32
	var executionOrder []int
	var orderMutex sync.Mutex

	executors := make([]Executor, 5)
	for i := 0; i < 5; i++ {
		taskID := i
		executors[i] = func(ctx context.Context) error {
			current := atomic.AddInt32(&currentRunning, 1)

			// Track execution order
			orderMutex.Lock()
			executionOrder = append(executionOrder, taskID)
			orderMutex.Unlock()

			// Update max if needed
			for {
				maxValue := atomic.LoadInt32(&maxSimultaneous)
				if current <= maxValue || atomic.CompareAndSwapInt32(&maxSimultaneous, maxValue, current) {
					break
				}
			}

			time.Sleep(20 * time.Millisecond)
			atomic.AddInt32(&currentRunning, -1)
			return nil
		}
	}

	err := NewParallelExecutor(1, executors...)(ctx)

	assert.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&maxSimultaneous),
		"max-parallel: 1 should only run 1 task at a time")
	assert.Len(t, executionOrder, 5, "All 5 tasks should have executed")
}
