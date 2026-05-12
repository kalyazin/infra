package fc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccumulateBalloon_SumsAcrossLines(t *testing.T) {
	t.Parallel()

	// FC's SharedIncMetric resets on each flush, so the reader must sum
	// per-line deltas. Three flushes with non-overlapping work should give
	// a cumulative snapshot equal to the field-wise sum.
	deltas := []firecrackerBalloonMetrics{
		{FreePageHintCount: 10, FreePageHintFreed: 40 * 1024 * 1024, FreePageReportCount: 2, FreePageReportFreed: 8 * 1024 * 1024},
		{FreePageHintCount: 5, FreePageHintFreed: 20 * 1024 * 1024, FreePageReportCount: 1},
		{FreePageHintFails: 1, FreePageReportFails: 3},
	}

	var snap *BalloonMetricsSnapshot
	for _, d := range deltas {
		next := accumulateBalloon(snap, d)
		snap = &next
	}

	require.NotNil(t, snap)
	assert.Equal(t, uint64(15), snap.HintCount)
	assert.Equal(t, uint64(60*1024*1024), snap.HintFreed)
	assert.Equal(t, uint64(1), snap.HintFails)
	assert.Equal(t, uint64(3), snap.ReportCount)
	assert.Equal(t, uint64(8*1024*1024), snap.ReportFreed)
	assert.Equal(t, uint64(3), snap.ReportFails)
}

func TestAccumulateBalloon_NilPrev(t *testing.T) {
	t.Parallel()

	got := accumulateBalloon(nil, firecrackerBalloonMetrics{
		FreePageHintCount: 7,
		FreePageHintFreed: 28 * 1024 * 1024,
	})
	assert.Equal(t, uint64(7), got.HintCount)
	assert.Equal(t, uint64(28*1024*1024), got.HintFreed)
}

func TestFirecrackerMetrics_ParsesBalloonLine(t *testing.T) {
	t.Parallel()

	// A trimmed-down version of one real FC metrics line, confirming the
	// JSON tags match what FC writes (so the reader actually populates the
	// balloon snapshot in production).
	const line = `{
		"net": {},
		"block": {},
		"balloon": {
			"free_page_hint_count": 11,
			"free_page_hint_freed": 46137344,
			"free_page_hint_fails": 0,
			"free_page_report_count": 2,
			"free_page_report_freed": 8388608,
			"free_page_report_fails": 0
		}
	}`

	var m firecrackerMetrics
	require.NoError(t, json.Unmarshal([]byte(line), &m))

	snap := accumulateBalloon(nil, m.Balloon)
	assert.Equal(t, uint64(11), snap.HintCount)
	assert.Equal(t, uint64(46137344), snap.HintFreed)
	assert.Equal(t, uint64(2), snap.ReportCount)
	assert.Equal(t, uint64(8388608), snap.ReportFreed)
}
