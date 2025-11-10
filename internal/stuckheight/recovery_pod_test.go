package stuckheight

import (
	"context"
	"testing"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRecoveryPodBuilder_BuildPod(t *testing.T) {
	t.Run("default pod template", func(t *testing.T) {
		mock := &mockClient{}
		builder := NewRecoveryPodBuilder(mock)

		recovery := &cosmosv1.StuckHeightRecovery{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-recovery",
				Namespace: "default",
			},
			Spec: cosmosv1.StuckHeightRecoverySpec{
				RecoveryScript: "echo 'test'",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				StuckPodName: "test-pod",
			},
		}

		pod := builder.BuildPod(recovery, "test-pod", "test-pvc", 12345)

		require.NotNil(t, pod)
		require.Equal(t, "test-recovery-recovery-test-pod", pod.Name)
		require.Equal(t, "default", pod.Namespace)
		require.Equal(t, DefaultRecoveryImage, pod.Spec.Containers[0].Image)
		require.Equal(t, corev1.RestartPolicyNever, pod.Spec.RestartPolicy)

		// Check labels
		require.Equal(t, "cosmos-operator", pod.Labels["app.kubernetes.io/managed-by"])
		require.Equal(t, "test-recovery", pod.Labels["cosmos.bharvest.io/recovery"])
		require.Equal(t, "recovery-pod", pod.Labels["cosmos.bharvest.io/type"])

		// Check container
		container := pod.Spec.Containers[0]
		require.Equal(t, "recovery", container.Name)
		require.Equal(t, []string{"/bin/sh"}, container.Command)
		require.Equal(t, []string{"-c", "echo 'test'"}, container.Args)

		// Check environment variables
		envMap := make(map[string]string)
		for _, env := range container.Env {
			envMap[env.Name] = env.Value
		}
		require.Equal(t, "test-pod", envMap["POD_NAME"])
		require.Equal(t, "default", envMap["POD_NAMESPACE"])
		require.Equal(t, "12345", envMap["CURRENT_HEIGHT"])
		require.Equal(t, "test-pvc", envMap["PVC_NAME"])
		require.Equal(t, "/chain-home", envMap["CHAIN_HOME"])

		// Check volume mounts
		require.Len(t, container.VolumeMounts, 1)
		require.Equal(t, "chain-data", container.VolumeMounts[0].Name)
		require.Equal(t, "/chain-home", container.VolumeMounts[0].MountPath)

		// Check volumes
		require.Len(t, pod.Spec.Volumes, 1)
		require.Equal(t, "chain-data", pod.Spec.Volumes[0].Name)
		require.NotNil(t, pod.Spec.Volumes[0].PersistentVolumeClaim)
		require.Equal(t, "test-pvc", pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName)

		// Check default resources
		require.Equal(t, resource.MustParse("100m"), container.Resources.Requests[corev1.ResourceCPU])
		require.Equal(t, resource.MustParse("128Mi"), container.Resources.Requests[corev1.ResourceMemory])
	})

	t.Run("custom pod template", func(t *testing.T) {
		mock := &mockClient{}
		builder := NewRecoveryPodBuilder(mock)

		recovery := &cosmosv1.StuckHeightRecovery{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-recovery",
				Namespace: "default",
			},
			Spec: cosmosv1.StuckHeightRecoverySpec{
				RecoveryScript: "echo 'test'",
				PodTemplate: &cosmosv1.RecoveryPodTemplate{
					Image:           "alpine:3.18",
					ImagePullPolicy: corev1.PullAlways,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("200m"),
							corev1.ResourceMemory: resource.MustParse("256Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
					Env: []corev1.EnvVar{
						{Name: "CUSTOM_VAR", Value: "custom-value"},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "custom-mount", MountPath: "/custom"},
					},
					Volumes: []corev1.Volume{
						{
							Name: "custom-volume",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				StuckPodName: "test-pod",
			},
		}

		pod := builder.BuildPod(recovery, "test-pod", "test-pvc", 12345)

		require.NotNil(t, pod)

		// Check custom image
		container := pod.Spec.Containers[0]
		require.Equal(t, "alpine:3.18", container.Image)
		require.Equal(t, corev1.PullAlways, container.ImagePullPolicy)

		// Check custom resources
		require.Equal(t, resource.MustParse("200m"), container.Resources.Requests[corev1.ResourceCPU])
		require.Equal(t, resource.MustParse("256Mi"), container.Resources.Requests[corev1.ResourceMemory])
		require.Equal(t, resource.MustParse("500m"), container.Resources.Limits[corev1.ResourceCPU])
		require.Equal(t, resource.MustParse("512Mi"), container.Resources.Limits[corev1.ResourceMemory])

		// Check custom env vars (should include both base and custom)
		envMap := make(map[string]string)
		for _, env := range container.Env {
			envMap[env.Name] = env.Value
		}
		require.Equal(t, "custom-value", envMap["CUSTOM_VAR"])
		require.Equal(t, "test-pod", envMap["POD_NAME"])

		// Check custom volume mounts (should include both base and custom)
		require.Len(t, container.VolumeMounts, 2)

		// Check custom volumes (should include both base and custom)
		require.Len(t, pod.Spec.Volumes, 2)
	})

	t.Run("nil pod template", func(t *testing.T) {
		mock := &mockClient{}
		builder := NewRecoveryPodBuilder(mock)

		recovery := &cosmosv1.StuckHeightRecovery{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-recovery",
				Namespace: "default",
			},
			Spec: cosmosv1.StuckHeightRecoverySpec{
				RecoveryScript: "echo 'test'",
				PodTemplate:    nil,
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				StuckPodName: "test-pod",
			},
		}

		pod := builder.BuildPod(recovery, "test-pod", "test-pvc", 12345)

		require.NotNil(t, pod)
		require.Equal(t, DefaultRecoveryImage, pod.Spec.Containers[0].Image)
	})
}

func TestRecoveryPodBuilder_CreatePod(t *testing.T) {
	ctx := context.Background()

	t.Run("happy path", func(t *testing.T) {
		mock := &mockClient{}
		builder := NewRecoveryPodBuilder(mock)

		recovery := &cosmosv1.StuckHeightRecovery{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-recovery",
				Namespace: "default",
			},
			Spec: cosmosv1.StuckHeightRecoverySpec{
				RecoveryScript: "echo 'test'",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				StuckPodName: "test-pod",
			},
		}

		podName, err := builder.CreatePod(ctx, recovery, "test-pod", "test-pvc", 12345)

		require.NoError(t, err)
		require.Equal(t, "test-recovery-recovery-test-pod", podName)
		require.Equal(t, 1, mock.CreateCount)

		createdPod := mock.LastCreateObject.(*corev1.Pod)
		require.NotNil(t, createdPod)
		require.Equal(t, "test-recovery-recovery-test-pod", createdPod.Name)
	})

	t.Run("create error", func(t *testing.T) {
		mock := &mockClient{}
		mock.CreateErr = errTest
		builder := NewRecoveryPodBuilder(mock)

		recovery := &cosmosv1.StuckHeightRecovery{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-recovery",
				Namespace: "default",
			},
			Spec: cosmosv1.StuckHeightRecoverySpec{
				RecoveryScript: "echo 'test'",
			},
			Status: cosmosv1.StuckHeightRecoveryStatus{
				StuckPodName: "test-pod",
			},
		}

		_, err := builder.CreatePod(ctx, recovery, "test-pod", "test-pvc", 12345)

		require.Error(t, err)
		require.Contains(t, err.Error(), "create recovery pod")
	})
}

func TestRecoveryPodBuilder_CheckPodComplete(t *testing.T) {
	ctx := context.Background()

	t.Run("pod succeeded", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodSucceeded,
			},
		}
		builder := NewRecoveryPodBuilder(mock)

		complete, success, err := builder.CheckPodComplete(ctx, "default", "test-pod")

		require.NoError(t, err)
		require.True(t, complete)
		require.True(t, success)
	})

	t.Run("pod failed", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
			},
		}
		builder := NewRecoveryPodBuilder(mock)

		complete, success, err := builder.CheckPodComplete(ctx, "default", "test-pod")

		require.NoError(t, err)
		require.True(t, complete)
		require.False(t, success)
	})

	t.Run("pod running", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		}
		builder := NewRecoveryPodBuilder(mock)

		complete, success, err := builder.CheckPodComplete(ctx, "default", "test-pod")

		require.NoError(t, err)
		require.False(t, complete)
		require.False(t, success)
	})

	t.Run("pod pending", func(t *testing.T) {
		mock := &mockClient{}
		mock.Object = corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
			},
		}
		builder := NewRecoveryPodBuilder(mock)

		complete, success, err := builder.CheckPodComplete(ctx, "default", "test-pod")

		require.NoError(t, err)
		require.False(t, complete)
		require.False(t, success)
	})

	t.Run("get error", func(t *testing.T) {
		mock := &mockClient{}
		mock.GetObjectErr = errTest
		builder := NewRecoveryPodBuilder(mock)

		_, _, err := builder.CheckPodComplete(ctx, "default", "test-pod")

		require.Error(t, err)
		require.Contains(t, err.Error(), "get recovery pod")
	})
}
