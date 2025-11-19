package stuckheight

import (
	"context"
	"testing"
	"time"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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

		result, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.Error(t, err)
		require.Contains(t, err.Error(), "no height information available")
		require.Nil(t, result)
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

		result, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.Error(t, err)
		require.Contains(t, err.Error(), "all pod heights are zero")
		require.Nil(t, result)
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

		result, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.Equal(t, uint64(200), result.MaxHeight)
		require.Empty(t, result.NewlyStuckPods)
		require.Empty(t, result.RecoveredPods)
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

		result, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.Equal(t, uint64(100), result.MaxHeight)
		require.Empty(t, result.NewlyStuckPods)
		require.Empty(t, result.RecoveredPods)
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

		result, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.Equal(t, uint64(100), result.MaxHeight)
		require.Empty(t, result.NewlyStuckPods)
		require.Empty(t, result.RecoveredPods)
	})

	t.Run("height stuck and duration exceeded", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: map[string]uint64{
					"test-pod-0": 100,
					"test-pod-1": 1000, // This pod is at normal height
				},
			},
		}

		pastTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		currentHeight := uint64(100)
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				LastObservedHeight:   100,
				LastHeightUpdateTime: &pastTime,
				StuckPods: map[string]*cosmosv1.StuckPodRecoveryStatus{
					"test-pod-0": {
						PodName:        "test-pod-0",
						StuckAtHeight:  100,
						CurrentHeight:  &currentHeight,
						DetectedAt:     pastTime,
						Phase:          cosmosv1.PodRecoveryPhaseLagging,
						LastUpdateTime: &pastTime,
					},
				},
			},
		}

		result, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.Equal(t, uint64(1000), result.MaxHeight)
		require.Contains(t, result.NewlyStuckPods, "test-pod-0")
		require.Equal(t, uint64(100), result.NewlyStuckPods["test-pod-0"])
		require.NotContains(t, result.NewlyStuckPods, "test-pod-1")
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

		result, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid stuck duration")
		require.Nil(t, result)
	})

	t.Run("multiple pods stuck simultaneously", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: map[string]uint64{
					"test-pod-0": 100,
					"test-pod-1": 100,
					"test-pod-2": 1000, // This pod is at normal height
				},
			},
		}

		pastTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		currentHeight := uint64(100)
		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				LastObservedHeight:   100,
				LastHeightUpdateTime: &pastTime,
				StuckPods: map[string]*cosmosv1.StuckPodRecoveryStatus{
					"test-pod-0": {
						PodName:        "test-pod-0",
						StuckAtHeight:  100,
						CurrentHeight:  &currentHeight,
						DetectedAt:     pastTime,
						Phase:          cosmosv1.PodRecoveryPhaseLagging,
						LastUpdateTime: &pastTime,
					},
					"test-pod-1": {
						PodName:        "test-pod-1",
						StuckAtHeight:  100,
						CurrentHeight:  &currentHeight,
						DetectedAt:     pastTime,
						Phase:          cosmosv1.PodRecoveryPhaseLagging,
						LastUpdateTime: &pastTime,
					},
				},
			},
		}

		result, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.Equal(t, uint64(1000), result.MaxHeight)
		require.Len(t, result.NewlyStuckPods, 2)
		require.Contains(t, result.NewlyStuckPods, "test-pod-0")
		require.Contains(t, result.NewlyStuckPods, "test-pod-1")
		require.NotContains(t, result.NewlyStuckPods, "test-pod-2")
	})

	t.Run("lagging pod - not yet stuck", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: map[string]uint64{
					"test-pod-0": 100,
					"test-pod-1": 1000, // This pod is at normal height
				},
			},
		}

		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				LastObservedHeight: 1000,
				StuckPods:          map[string]*cosmosv1.StuckPodRecoveryStatus{},
			},
		}

		result, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.Equal(t, uint64(1000), result.MaxHeight)
		require.Empty(t, result.NewlyStuckPods)
		require.Len(t, result.LaggingPods, 1)
		require.Contains(t, result.LaggingPods, "test-pod-0")
		require.Equal(t, uint64(100), result.LaggingPods["test-pod-0"])
	})

	t.Run("pod recovered - height started moving again", func(t *testing.T) {
		mock := &mockClient{}
		monitor := NewHeightMonitor(mock)

		crd := &cosmosv1.CosmosFullNode{
			Status: cosmosv1.FullNodeStatus{
				Height: map[string]uint64{
					"test-pod-0": 1000, // Height recovered
					"test-pod-1": 990,  // Close to max, recovered
				},
			},
		}

		recovery := &cosmosv1.StuckHeightRecovery{
			Spec: cosmosv1.StuckHeightRecoverySpec{
				StuckDuration: "5m",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				StuckPods: map[string]*cosmosv1.StuckPodRecoveryStatus{
					"test-pod-0": {
						PodName:       "test-pod-0",
						StuckAtHeight: 100,
					},
					"test-pod-1": {
						PodName:       "test-pod-1",
						StuckAtHeight: 100,
					},
				},
			},
		}

		result, err := monitor.CheckStuckHeight(ctx, crd, recovery)

		require.NoError(t, err)
		require.Equal(t, uint64(1000), result.MaxHeight)
		require.Empty(t, result.NewlyStuckPods)
		require.Len(t, result.RecoveredPods, 2)
		require.Contains(t, result.RecoveredPods, "test-pod-0")
		require.Contains(t, result.RecoveredPods, "test-pod-1")
	})
}
