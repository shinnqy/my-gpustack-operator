package worker

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"gpustack.ai/gpustack/pkg/worker/scaling/algorithm"
)

// TestModelDeploymentScalerWindowRefusesARestart 验证跨越引擎重启的窗口会被拒绝。
//
// 重启后的计数属于新进程。两次读数相减会把重启误算成并未发生的负载突增。
func TestModelDeploymentScalerWindowRefusesARestart(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	previous := modelDeploymentScalerPodSample{
		observedAt:   start,
		restartCount: 1,
		ttft:         modelDeploymentScalerHistogram{buckets: map[float64]float64{1: 50, math.Inf(1): 56}, count: 56},
		itl:          modelDeploymentScalerHistogram{buckets: map[float64]float64{0.05: 40, math.Inf(1): 56}, count: 56},
	}
	current := modelDeploymentScalerPodSample{
		observedAt:   start.Add(10 * time.Second),
		restartCount: 2,
		ttft:         modelDeploymentScalerHistogram{buckets: map[float64]float64{1: 130, math.Inf(1): 140}, count: 140},
		itl:          modelDeploymentScalerHistogram{buckets: map[float64]float64{0.05: 100, math.Inf(1): 140}, count: 140},
	}

	window := modelDeploymentScalerWindowOf(&previous, current, start.Add(10*time.Second), time.Minute)
	assert.Equal(t, algorithm.SignalMissing, window.state,
		"a window that spans a restart is not a reading")
	assert.Contains(t, window.reason, "restarted")
}
