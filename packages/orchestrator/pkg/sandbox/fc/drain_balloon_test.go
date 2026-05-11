package fc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPollFphDone_FastCycleAfterPriorDone(t *testing.T) {
	t.Parallel()

	// Regression: previously the loop required observing host > hostBefore
	// before accepting host == freePageHintDone. When hostBefore was already
	// freePageHintDone (steady state after a prior cycle, common after
	// resume-from-snapshot) and the new cycle completed between polls, the
	// bump was missed and the loop hung until ctx timeout.
	calls := 0
	describe := func(_ context.Context) (int64, error) {
		calls++

		return freePageHintDone, nil
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	require.NoError(t, pollFphDone(ctx, describe))
	assert.Equal(t, 1, calls)
}

func TestPollFphDone_WaitsForBumpToReturnToDone(t *testing.T) {
	t.Parallel()

	seq := []int64{2, 2, 2, freePageHintDone}
	idx := 0
	describe := func(_ context.Context) (int64, error) {
		v := seq[idx]
		idx++

		return v, nil
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	require.NoError(t, pollFphDone(ctx, describe))
	assert.Equal(t, len(seq), idx)
}

func TestPollFphDone_TimeoutWhenStuck(t *testing.T) {
	t.Parallel()

	describe := func(_ context.Context) (int64, error) {
		return 2, nil
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := pollFphDone(ctx, describe)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestPollFphDone_DescribeError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")
	describe := func(_ context.Context) (int64, error) {
		return 0, sentinel
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := pollFphDone(ctx, describe)
	assert.ErrorIs(t, err, sentinel)
}
