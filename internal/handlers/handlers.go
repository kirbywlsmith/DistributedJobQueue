package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

type Handler func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)

func Get(jobType string) (Handler, bool) {
	handler, exists := registry[jobType]
	return handler, exists
}

var registry = map[string]Handler{
	"sleep": handleSleep,
	"cpu":   handleCPU,
	"flaky": handleFlaky,
}

func handleSleep(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Seconds int `json:"seconds"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad payload: %w", err)
	}
	select {
	case <-time.After(time.Duration(p.Seconds) * time.Second):
		return json.RawMessage(`{"slept_seconds": ` + fmt.Sprint(p.Seconds) + `}`), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func handleCPU(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		Iterations int `json:"iterations"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad payload: %w", err)
	}
	if p.Iterations <= 0 {
		p.Iterations = 5_000_000
	}
	sum := sha256.Sum256([]byte("seed"))
	for i := 0; i < p.Iterations; i++ {
		if i%100_000 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		sum = sha256.Sum256(sum[:])
	}
	return json.RawMessage(fmt.Sprintf(`{"iterations": %d, "digest": "%x"}`, p.Iterations, sum[:8])), nil
}

func handleFlaky(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var p struct {
		FailureRate float64 `json:"failure_rate"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("bad payload: %w", err)
	}
	if p.FailureRate == 0 {
		p.FailureRate = 0.5
	}
	if rand.Float64() < p.FailureRate {
		return nil, errors.New("flaky job got unlucky")
	}
	return json.RawMessage(`{"lucky": true}`), nil
}