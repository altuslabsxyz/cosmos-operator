package stuckheight

import (
	"context"
	"testing"
	"time"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func ptr[T any](v T) *T {
	return &v
}

func TestHeightMonitor_CheckStuckHeight(t *testing.T) {
	ctx := context.Background()

	t.Run("no height information", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: nil,
			},
		}

		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "5m",
			},
		}

		stuck, podName, height, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.Error(t, err)
		require.Contains(t, err.Error(), "no height information available")
		require.False(t, stuck)
		require.Empty(t, podName)
		require.Equal(t, uint64(0), height)
	})

	t.Run("height is zero", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: map[string]uint64{
					"test-pod": 0,
				},
			},
		}

		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "5m",
			},
		}

		stuck, podName, height, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.Error(t, err)
		require.Contains(t, err.Error(), "height is zero")
		require.False(t, stuck)
		require.Empty(t, podName)
		require.Equal(t, uint64(0), height)
	})

	t.Run("height changed - not stuck", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: map[string]uint64{
					"test-pod": 200,
				},
			},
		}

		pastTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				LastObservedHeight:   100,
				LastHeightUpdateTime: &pastTime,
			},
		}

		stuck, podName, height, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.False(t, stuck)
		require.Empty(t, podName)
		require.Equal(t, uint64(200), height)
	})

	t.Run("first observation of height - not stuck", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: map[string]uint64{
					"test-pod": 100,
				},
			},
		}

		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				LastObservedHeight:   100,
				LastHeightUpdateTime: nil,
			},
		}

		stuck, podName, height, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.False(t, stuck)
		require.Empty(t, podName)
		require.Equal(t, uint64(100), height)
	})

	t.Run("height stuck but duration not exceeded", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: map[string]uint64{
					"test-pod": 100,
				},
			},
		}

		recentTime := metav1.NewTime(time.Now().Add(-2 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				LastObservedHeight:   100,
				LastHeightUpdateTime: &recentTime,
			},
		}

		stuck, podName, height, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.False(t, stuck)
		require.Empty(t, podName)
		require.Equal(t, uint64(100), height)
	})

	t.Run("height stuck and duration exceeded", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: map[string]uint64{
					"test-pod-0": 100,
				},
			},
		}

		pastTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				LastObservedHeight:   100,
				LastHeightUpdateTime: &pastTime,
			},
		}

		stuck, podName, height, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.True(t, stuck)
		require.Equal(t, "test-pod-0", podName)
		require.Equal(t, uint64(100), height)
	})

	t.Run("invalid stuck duration", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: map[string]uint64{
					"test-pod": 100,
				},
			},
		}

		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "invalid",
			},
		}

		stuck, podName, height, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid stuck duration")
		require.False(t, stuck)
		require.Empty(t, podName)
		require.Equal(t, uint64(0), height)
	})

	t.Run("multiple pods - uses first one", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: map[string]uint64{
					"test-pod-0": 100,
					"test-pod-1": 100,
				},
			},
		}

		pastTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				LastObservedHeight:   100,
				LastHeightUpdateTime: &pastTime,
			},
		}

		stuck, podName, height, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.True(t, stuck)
		require.NotEmpty(t, podName)
		require.Equal(t, uint64(100), height)
	})
}
