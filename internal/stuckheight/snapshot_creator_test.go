package stuckheight

import (
	"context"
	"testing"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSnapshotCreator_CreateSnapshot(t *testing.T) {
	ctx := context.Background()

	t.Run("happy path - no snapshot class", func(t *testing.T) {
		mock := &mockClient{}
		creator := NewSnapshotCreator(mock)

		recovery := &cosmosv1.StuckHeightRecovery{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-recovery",
				Namespace: "default",
			},
			Spec: cosmosv1.StuckHeightRecoverySpec{
				VolumeSnapshotClassName: "",
			},
		}

		snapshotName, err := creator.CreateSnapshot(ctx, recovery, "test-pvc")

		require.NoError(t, err)
		require.NotEmpty(t, snapshotName)
		require.Contains(t, snapshotName, "test-pvc-recovery-")
		require.Equal(t, 1, mock.CreateCount)

		createdSnapshot := mock.LastCreateObject.(*snapshotv1.VolumeSnapshot)
		require.NotNil(t, createdSnapshot)
		require.Equal(t, "default", createdSnapshot.Namespace)
		require.Equal(t, "test-pvc", *createdSnapshot.Spec.Source.PersistentVolumeClaimName)
		require.Nil(t, createdSnapshot.Spec.VolumeSnapshotClassName)

		// Check labels
		require.Equal(t, "cosmos-operator", createdSnapshot.Labels["app.kubernetes.io/managed-by"])
		require.Equal(t, "test-recovery", createdSnapshot.Labels["cosmos.bharvest.io/recovery"])
	})

	t.Run("with snapshot class", func(t *testing.T) {
		mock := &mockClient{}
		creator := NewSnapshotCreator(mock)

		recovery := &cosmosv1.StuckHeightRecovery{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-recovery",
				Namespace: "default",
			},
			Spec: cosmosv1.StuckHeightRecoverySpec{
				VolumeSnapshotClassName: "custom-snapshot-class",
			},
		}

		snapshotName, err := creator.CreateSnapshot(ctx, recovery, "test-pvc")

		require.NoError(t, err)
		require.NotEmpty(t, snapshotName)

		createdSnapshot := mock.LastCreateObject.(*snapshotv1.VolumeSnapshot)
		require.NotNil(t, createdSnapshot.Spec.VolumeSnapshotClassName)
		require.Equal(t, "custom-snapshot-class", *createdSnapshot.Spec.VolumeSnapshotClassName)
	})

	t.Run("create error", func(t *testing.T) {
		mock := &mockClient{}
		mock.CreateErr = errTest
		creator := NewSnapshotCreator(mock)

		recovery := &cosmosv1.StuckHeightRecovery{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-recovery",
				Namespace: "default",
			},
		}

		_, err := creator.CreateSnapshot(ctx, recovery, "test-pvc")

		require.Error(t, err)
		require.Contains(t, err.Error(), "create volume snapshot")
	})
}

func TestSnapshotCreator_CheckSnapshotReady(t *testing.T) {
	ctx := context.Background()

	t.Run("snapshot ready", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = snapshotv1.VolumeSnapshot{
			Status: &snapshotv1.VolumeSnapshotStatus{
				ReadyToUse: ptr(true),
			},
		}
		creator := NewSnapshotCreator(mock)

		ready, err := creator.CheckSnapshotReady(ctx, "default", "test-snapshot")

		require.NoError(t, err)
		require.True(t, ready)
	})

	t.Run("snapshot not ready", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = snapshotv1.VolumeSnapshot{
			Status: &snapshotv1.VolumeSnapshotStatus{
				ReadyToUse: ptr(false),
			},
		}
		creator := NewSnapshotCreator(mock)

		ready, err := creator.CheckSnapshotReady(ctx, "default", "test-snapshot")

		require.NoError(t, err)
		require.False(t, ready)
	})

	t.Run("snapshot status nil", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = snapshotv1.VolumeSnapshot{
			Status: nil,
		}
		creator := NewSnapshotCreator(mock)

		ready, err := creator.CheckSnapshotReady(ctx, "default", "test-snapshot")

		require.NoError(t, err)
		require.False(t, ready)
	})

	t.Run("snapshot ready to use nil", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = snapshotv1.VolumeSnapshot{
			Status: &snapshotv1.VolumeSnapshotStatus{
				ReadyToUse: nil,
			},
		}
		creator := NewSnapshotCreator(mock)

		ready, err := creator.CheckSnapshotReady(ctx, "default", "test-snapshot")

		require.NoError(t, err)
		require.False(t, ready)
	})

	t.Run("get error", func(t *testing.T) {
		mock := &mockClient{}
		mock.GetObjectErr = errTest
		creator := NewSnapshotCreator(mock)

		_, err := creator.CheckSnapshotReady(ctx, "default", "test-snapshot")

		require.Error(t, err)
		require.Contains(t, err.Error(), "get volume snapshot")
	})
}

func TestSnapshotCreator_GetPVCForPod(t *testing.T) {
	ctx := context.Background()

	t.Run("happy path", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						Name: "vol-chain-home",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "test-pvc",
							},
						},
					},
				},
			},
		}
		creator := NewSnapshotCreator(mock)

		pvcName, err := creator.GetPVCForPod(ctx, "default", "test-pod")

		require.NoError(t, err)
		require.Equal(t, "test-pvc", pvcName)
	})

	t.Run("multiple volumes - find correct one", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						Name: "other-volume",
						VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						},
					},
					{
						Name: "vol-chain-home",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "test-pvc",
							},
						},
					},
				},
			},
		}
		creator := NewSnapshotCreator(mock)

		pvcName, err := creator.GetPVCForPod(ctx, "default", "test-pod")

		require.NoError(t, err)
		require.Equal(t, "test-pvc", pvcName)
	})

	t.Run("no matching volume", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						Name: "other-volume",
						VolumeSource: corev1.VolumeSource{
							EmptyDir: &corev1.EmptyDirVolumeSource{},
						},
					},
				},
			},
		}
		creator := NewSnapshotCreator(mock)

		_, err := creator.GetPVCForPod(ctx, "default", "test-pod")

		require.Error(t, err)
		require.Contains(t, err.Error(), "no PVC found")
	})

	t.Run("no volumes", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{},
			},
		}
		creator := NewSnapshotCreator(mock)

		_, err := creator.GetPVCForPod(ctx, "default", "test-pod")

		require.Error(t, err)
		require.Contains(t, err.Error(), "no PVC found")
	})

	t.Run("get pod error", func(t *testing.T) {
		mock := &mockClient{}
		mock.GetObjectErr = errTest
		creator := NewSnapshotCreator(mock)

		_, err := creator.GetPVCForPod(ctx, "default", "test-pod")

		require.Error(t, err)
		require.Contains(t, err.Error(), "get pod")
	})
}
