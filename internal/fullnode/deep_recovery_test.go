package fullnode

import (
	"context"
	"testing"
	"time"

	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cosmosv1 "github.com/altuslabsxyz/cosmos-operator/api/v1"
)

func TestNewDeepRecoveryManager(t *testing.T) {
	t.Parallel()

	mClient := &mockClient[*corev1.Pod]{}
	manager := NewDeepRecoveryManager(mClient)

	require.NotNil(t, manager)
	require.NotNil(t, manager.client)
}

func TestDeepRecoveryManager_CheckAndTriggerRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("nil spec returns false", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold:    100,
						DeepRecovery: nil,
					},
				},
			},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, nil)
		require.NoError(t, err)
		require.False(t, actionTaken)
	})

	t.Run("suspended spec returns false", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
							Suspend:        true,
						},
					},
				},
			},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, nil)
		require.NoError(t, err)
		require.False(t, actionTaken)
	})

	t.Run("invalid stuck duration returns error", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "invalid",
							RecoveryScript: "echo hello",
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{{PodName: "pod-0", CurrentHeight: 100}}
		_, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid stuck duration")
	})

	t.Run("detects stuck pod and transitions to Detected phase", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 100, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.True(t, actionTaken)

		status := crd.Status.SelfHealing.DeepRecovery["pod-0"]
		require.NotNil(t, status)
		require.Equal(t, cosmosv1.DeepRecoveryPhaseDetected, status.Phase)
		require.Equal(t, uint64(100), status.StuckHeight)
		require.NotNil(t, status.DetectedAt)
	})
}

func TestDeepRecoveryManager_HandleIdlePhase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("transitions from Idle to Detected", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase: cosmosv1.DeepRecoveryPhaseIdle,
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 100, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.True(t, actionTaken)

		status := crd.Status.SelfHealing.DeepRecovery["pod-0"]
		require.Equal(t, cosmosv1.DeepRecoveryPhaseDetected, status.Phase)
		require.Equal(t, uint64(100), status.StuckHeight)
	})
}

func TestDeepRecoveryManager_HandleDetectedPhase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("resets to Idle when height increases", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		now := metav1.Now()
		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:       cosmosv1.DeepRecoveryPhaseDetected,
							StuckHeight: 100,
							DetectedAt:  &now,
						},
					},
				},
			},
		}

		// Current height increased
		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 150, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.True(t, actionTaken)

		status := crd.Status.SelfHealing.DeepRecovery["pod-0"]
		require.Equal(t, cosmosv1.DeepRecoveryPhaseIdle, status.Phase)
		require.Equal(t, uint64(0), status.StuckHeight)
	})

	t.Run("waits if stuck duration not passed", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		now := metav1.Now()
		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "10m",
							RecoveryScript: "echo hello",
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:       cosmosv1.DeepRecoveryPhaseDetected,
							StuckHeight: 100,
							DetectedAt:  &now, // Just detected now
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 100, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.False(t, actionTaken) // No action - waiting for stuck duration
	})

	t.Run("creates snapshot when enabled and stuck duration passed", func(t *testing.T) {
		mClient := &mockClient[*snapshotv1.VolumeSnapshot]{}
		manager := NewDeepRecoveryManager(mClient)

		pastTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
							VolumeSnapshot: &cosmosv1.RecoveryVolumeSnapshotSpec{
								Enabled: true,
							},
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:       cosmosv1.DeepRecoveryPhaseDetected,
							StuckHeight: 100,
							DetectedAt:  &pastTime,
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 100, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.True(t, actionTaken)

		status := crd.Status.SelfHealing.DeepRecovery["pod-0"]
		require.Equal(t, cosmosv1.DeepRecoveryPhaseSnapshotCreating, status.Phase)
		require.NotEmpty(t, status.SnapshotName)
		require.Equal(t, 1, mClient.CreateCount)
	})

	t.Run("creates recovery pod when snapshot disabled and stuck duration passed", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		pastTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:       cosmosv1.DeepRecoveryPhaseDetected,
							StuckHeight: 100,
							DetectedAt:  &pastTime,
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 100, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.True(t, actionTaken)

		status := crd.Status.SelfHealing.DeepRecovery["pod-0"]
		require.Equal(t, cosmosv1.DeepRecoveryPhaseRecoveryRunning, status.Phase)
		require.NotEmpty(t, status.RecoveryPodName)
		require.Equal(t, int32(1), status.RetryCount)
		require.Equal(t, 1, mClient.CreateCount)
	})
}

func TestDeepRecoveryManager_HandleSnapshotCreatingPhase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("transitions to SnapshotReady when snapshot is ready", func(t *testing.T) {
		mClient := &mockClient[*snapshotv1.VolumeSnapshot]{}
		mClient.Object = snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pvc-0-recovery-12345",
				Namespace: "default",
			},
			Status: &snapshotv1.VolumeSnapshotStatus{
				ReadyToUse: ptr(true),
			},
		}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
							VolumeSnapshot: &cosmosv1.RecoveryVolumeSnapshotSpec{
								Enabled: true,
							},
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:        cosmosv1.DeepRecoveryPhaseSnapshotCreating,
							StuckHeight:  100,
							SnapshotName: "pvc-0-recovery-12345",
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 100, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.True(t, actionTaken)

		status := crd.Status.SelfHealing.DeepRecovery["pod-0"]
		require.Equal(t, cosmosv1.DeepRecoveryPhaseSnapshotReady, status.Phase)
	})

	t.Run("waits when snapshot is not ready", func(t *testing.T) {
		mClient := &mockClient[*snapshotv1.VolumeSnapshot]{}
		mClient.Object = snapshotv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pvc-0-recovery-12345",
				Namespace: "default",
			},
			Status: &snapshotv1.VolumeSnapshotStatus{
				ReadyToUse: ptr(false),
			},
		}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
							VolumeSnapshot: &cosmosv1.RecoveryVolumeSnapshotSpec{
								Enabled: true,
							},
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:        cosmosv1.DeepRecoveryPhaseSnapshotCreating,
							StuckHeight:  100,
							SnapshotName: "pvc-0-recovery-12345",
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 100, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.False(t, actionTaken) // Waiting for snapshot
	})
}

func TestDeepRecoveryManager_HandleRecoveryPhase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("transitions to Completed when recovery pod succeeds", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		mClient.Object = corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-0-recovery-12345",
				Namespace: "default",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodSucceeded,
			},
		}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:           cosmosv1.DeepRecoveryPhaseRecoveryRunning,
							StuckHeight:     100,
							RecoveryPodName: "pod-0-recovery-12345",
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 100, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.True(t, actionTaken)

		status := crd.Status.SelfHealing.DeepRecovery["pod-0"]
		require.Equal(t, cosmosv1.DeepRecoveryPhaseRecoveryCompleted, status.Phase)
	})

	t.Run("transitions to Failed when recovery pod fails", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		mClient.Object = corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-0-recovery-12345",
				Namespace: "default",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
			},
		}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:           cosmosv1.DeepRecoveryPhaseRecoveryRunning,
							StuckHeight:     100,
							RecoveryPodName: "pod-0-recovery-12345",
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 100, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.True(t, actionTaken)

		status := crd.Status.SelfHealing.DeepRecovery["pod-0"]
		require.Equal(t, cosmosv1.DeepRecoveryPhaseRecoveryFailed, status.Phase)
	})
}

func TestDeepRecoveryManager_HandleCompletedPhase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("resets to Idle when height recovers", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:           cosmosv1.DeepRecoveryPhaseRecoveryCompleted,
							StuckHeight:     100,
							RecoveryPodName: "pod-0-recovery-12345",
							RetryCount:      1,
						},
					},
				},
			},
		}

		// Height has recovered
		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 500, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.True(t, actionTaken)

		status := crd.Status.SelfHealing.DeepRecovery["pod-0"]
		require.Equal(t, cosmosv1.DeepRecoveryPhaseIdle, status.Phase)
		require.Equal(t, uint64(0), status.StuckHeight)
		require.Equal(t, int32(0), status.RetryCount)
		require.Empty(t, status.RecoveryPodName)
	})
}

func TestDeepRecoveryManager_RateLimiting(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("rate limits after max retries", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		pastTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		recentAttempt := metav1.NewTime(time.Now().Add(-1 * time.Minute))
		maxRetries := int32(3)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:   "5m",
							RecoveryScript:  "echo hello",
							MaxRetries:      &maxRetries,
							RateLimitWindow: "1h",
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:         cosmosv1.DeepRecoveryPhaseDetected,
							StuckHeight:   100,
							DetectedAt:    &pastTime,
							RetryCount:    3, // Already at max retries
							LastAttemptAt: &recentAttempt,
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 100, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.True(t, actionTaken)

		status := crd.Status.SelfHealing.DeepRecovery["pod-0"]
		require.Equal(t, cosmosv1.DeepRecoveryPhaseRateLimited, status.Phase)
	})

	t.Run("resets rate limit after window passes", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		oldAttempt := metav1.NewTime(time.Now().Add(-2 * time.Hour))

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:   "5m",
							RecoveryScript:  "echo hello",
							RateLimitWindow: "1h",
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:         cosmosv1.DeepRecoveryPhaseRateLimited,
							StuckHeight:   100,
							RetryCount:    3,
							LastAttemptAt: &oldAttempt, // 2 hours ago - window passed
						},
					},
				},
			},
		}

		stuckPods := []PodStuckInfo{
			{PodName: "pod-0", CurrentHeight: 100, MaxHeight: 1000, PVCName: "pvc-0"},
		}

		actionTaken, err := manager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
		require.NoError(t, err)
		require.True(t, actionTaken)

		status := crd.Status.SelfHealing.DeepRecovery["pod-0"]
		require.Equal(t, cosmosv1.DeepRecoveryPhaseDetected, status.Phase)
		require.Equal(t, int32(0), status.RetryCount) // Reset
	})
}

func TestDeepRecoveryManager_CreateRecoveryPod(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("creates pod with default settings", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
		}

		podInfo := PodStuckInfo{
			PodName:       "pod-0",
			CurrentHeight: 100,
			PVCName:       "pvc-0",
		}

		spec := &cosmosv1.DeepRecoverySpec{
			StuckDuration:  "5m",
			RecoveryScript: "echo hello",
		}

		name, err := manager.createRecoveryPod(ctx, crd, podInfo, spec)
		require.NoError(t, err)
		require.NotEmpty(t, name)
		require.Equal(t, 1, mClient.CreateCount)

		createdPod := mClient.LastCreateObject
		require.Equal(t, "default", createdPod.Namespace)
		require.Equal(t, DefaultRecoveryImage, createdPod.Spec.Containers[0].Image)
		require.Equal(t, corev1.RestartPolicyNever, createdPod.Spec.RestartPolicy)

		// Check environment variables
		envVars := createdPod.Spec.Containers[0].Env
		require.Len(t, envVars, 5)
	})

	t.Run("creates pod with custom template", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
		}

		podInfo := PodStuckInfo{
			PodName:       "pod-0",
			CurrentHeight: 100,
			PVCName:       "pvc-0",
		}

		spec := &cosmosv1.DeepRecoverySpec{
			StuckDuration:  "5m",
			RecoveryScript: "echo hello",
			PodTemplate: &cosmosv1.RecoveryPodTemplate{
				Image:   "custom-image:v1",
				Command: []string{"/bin/bash"},
			},
		}

		name, err := manager.createRecoveryPod(ctx, crd, podInfo, spec)
		require.NoError(t, err)
		require.NotEmpty(t, name)

		createdPod := mClient.LastCreateObject
		require.Equal(t, "custom-image:v1", createdPod.Spec.Containers[0].Image)
		require.Equal(t, []string{"/bin/bash"}, createdPod.Spec.Containers[0].Command)
	})
}

func TestDeepRecoveryManager_CreateVolumeSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("creates snapshot with labels", func(t *testing.T) {
		mClient := &mockClient[*snapshotv1.VolumeSnapshot]{}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
		}

		podInfo := PodStuckInfo{
			PodName:       "pod-0",
			CurrentHeight: 100,
			PVCName:       "pvc-0",
		}

		spec := &cosmosv1.DeepRecoverySpec{
			StuckDuration:  "5m",
			RecoveryScript: "echo hello",
			VolumeSnapshot: &cosmosv1.RecoveryVolumeSnapshotSpec{
				Enabled:                 true,
				VolumeSnapshotClassName: "csi-snapclass",
			},
		}

		name, err := manager.createVolumeSnapshot(ctx, crd, podInfo, spec)
		require.NoError(t, err)
		require.NotEmpty(t, name)
		require.Contains(t, name, "pvc-0-recovery-")
		require.Equal(t, 1, mClient.CreateCount)

		snapshot := mClient.LastCreateObject
		require.Equal(t, "default", snapshot.Namespace)
		require.Equal(t, "test-node", snapshot.Labels["cosmos.altuslabsxyz.io/recovery"])
		require.Equal(t, "pod-0", snapshot.Labels["cosmos.altuslabsxyz.io/pod"])
		require.Equal(t, "deep-recovery-backup", snapshot.Labels["cosmos.altuslabsxyz.io/type"])
		require.Equal(t, "csi-snapclass", *snapshot.Spec.VolumeSnapshotClassName)
	})
}

func TestDeepRecoveryManager_CleanupRecoveredPods(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("deletes recovery pod for Idle status", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:           cosmosv1.DeepRecoveryPhaseIdle,
							RecoveryPodName: "pod-0-recovery-12345",
						},
					},
				},
			},
		}

		err := manager.CleanupRecoveredPods(ctx, crd)
		require.NoError(t, err)
		require.Equal(t, 1, mClient.DeleteCount)
	})

	t.Run("skips non-Idle status", func(t *testing.T) {
		mClient := &mockClient[*corev1.Pod]{}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase:           cosmosv1.DeepRecoveryPhaseRecoveryRunning,
							RecoveryPodName: "pod-0-recovery-12345",
						},
					},
				},
			},
		}

		err := manager.CleanupRecoveredPods(ctx, crd)
		require.NoError(t, err)
		require.Equal(t, 0, mClient.DeleteCount) // No deletion
	})

	t.Run("cleans up old snapshots when enabled", func(t *testing.T) {
		mClient := &mockClient[*snapshotv1.VolumeSnapshot]{}
		retentionCount := int32(2)
		mClient.ObjectList = snapshotv1.VolumeSnapshotList{
			Items: []snapshotv1.VolumeSnapshot{
				{ObjectMeta: metav1.ObjectMeta{Name: "snap-1"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "snap-2"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "snap-3"}},
				{ObjectMeta: metav1.ObjectMeta{Name: "snap-4"}},
			},
		}
		manager := NewDeepRecoveryManager(mClient)

		crd := &cosmosv1.CosmosFullNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node",
				Namespace: "default",
			},
			Spec: cosmosv1.FullNodeSpec{
				SelfHeal: &cosmosv1.SelfHealSpec{
					HeightDriftMitigation: &cosmosv1.HeightDriftMitigationSpec{
						Threshold: 100,
						DeepRecovery: &cosmosv1.DeepRecoverySpec{
							StuckDuration:  "5m",
							RecoveryScript: "echo hello",
							VolumeSnapshot: &cosmosv1.RecoveryVolumeSnapshotSpec{
								Enabled:        true,
								RetentionCount: &retentionCount,
							},
						},
					},
				},
			},
			Status: cosmosv1.FullNodeStatus{
				SelfHealing: cosmosv1.SelfHealingStatus{
					DeepRecovery: map[string]*cosmosv1.DeepRecoveryStatus{
						"pod-0": {
							Phase: cosmosv1.DeepRecoveryPhaseIdle,
						},
					},
				},
			},
		}

		err := manager.CleanupRecoveredPods(ctx, crd)
		require.NoError(t, err)
		require.Equal(t, 2, mClient.DeleteCount) // 4 - 2 = 2 deleted
	})
}

func TestDeepRecoveryManager_IsRateLimited(t *testing.T) {
	t.Parallel()

	t.Run("not rate limited with zero retries", func(t *testing.T) {
		manager := &DeepRecoveryManager{}

		status := &cosmosv1.DeepRecoveryStatus{
			RetryCount: 0,
		}
		spec := &cosmosv1.DeepRecoverySpec{
			StuckDuration:  "5m",
			RecoveryScript: "echo hello",
		}

		result := manager.isRateLimited(status, spec)
		require.False(t, result)
	})

	t.Run("not rate limited when window passed", func(t *testing.T) {
		manager := &DeepRecoveryManager{}

		oldTime := metav1.NewTime(time.Now().Add(-2 * time.Hour))
		maxRetries := int32(3)
		status := &cosmosv1.DeepRecoveryStatus{
			RetryCount:    3,
			LastAttemptAt: &oldTime,
		}
		spec := &cosmosv1.DeepRecoverySpec{
			StuckDuration:   "5m",
			RecoveryScript:  "echo hello",
			MaxRetries:      &maxRetries,
			RateLimitWindow: "1h",
		}

		result := manager.isRateLimited(status, spec)
		require.False(t, result)
	})

	t.Run("rate limited when max retries exceeded within window", func(t *testing.T) {
		manager := &DeepRecoveryManager{}

		recentTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		maxRetries := int32(3)
		status := &cosmosv1.DeepRecoveryStatus{
			RetryCount:    3,
			LastAttemptAt: &recentTime,
		}
		spec := &cosmosv1.DeepRecoverySpec{
			StuckDuration:   "5m",
			RecoveryScript:  "echo hello",
			MaxRetries:      &maxRetries,
			RateLimitWindow: "1h",
		}

		result := manager.isRateLimited(status, spec)
		require.True(t, result)
	})
}
