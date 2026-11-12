package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

type scriptedVerifier struct {
	answers []int
	err     error
	calls   int
}

func (s *scriptedVerifier) VerifyChain(context.Context) (int, error) {
	s.calls++
	if s.err != nil {
		return 0, s.err
	}
	n := s.answers[0]
	if len(s.answers) > 1 {
		s.answers = s.answers[1:]
	}
	return n, nil
}

// A failed chain check used to be a warning, and the writer started on endpoints nobody had checked.
func TestTheWriterWaitsForAConfirmedChainAndRefusesAWrongOne(t *testing.T) {
	defer func(d time.Duration) { chainRetryStart = d }(chainRetryStart)
	chainRetryStart = time.Millisecond

	unreachable := &scriptedVerifier{answers: []int{0, 0, 1}}
	if err := verifyChain(context.Background(), unreachable, quietLog()); err != nil {
		t.Fatal(err)
	}
	if unreachable.calls != 3 {
		t.Errorf("asked %d times, want 3: it should retry until an endpoint answers", unreachable.calls)
	}

	wrong := errors.New("endpoint h is on chain 560048, CHAIN_ID is 1")
	if err := verifyChain(context.Background(), &scriptedVerifier{err: wrong}, quietLog()); !errors.Is(err, wrong) {
		t.Errorf("an endpoint on another chain: %v, want the startup refused", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := verifyChain(ctx, &scriptedVerifier{answers: []int{0}}, quietLog()); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("no endpoint ever answering: %v, want the wait to end with the context", err)
	}
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
