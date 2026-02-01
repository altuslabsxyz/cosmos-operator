package fullnode

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	"github.com/b-harvest/cosmos-operator/internal/diff"
	"github.com/b-harvest/cosmos-operator/internal/kube"
)

type mockPodClient struct{ mockClient[*corev1.Pod] }

// setPodsReady sets pods to K8s Ready state (Running phase, init containers completed, Ready condition true)
func setPodsReady(pods []*corev1.Pod) {
	for _, pod := range pods {
		pod.Status = corev1.PodStatus{
			Phase: corev1.PodRunning,
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "chain-init",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
					},
				},
				{
					Name: "version-check",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
					},
				},
			},
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		}
	}
}

func newMockPodClient(pods []*corev1.Pod) *mockPodClient {
	return &mockPodClient{
		mockClient: mockClient[*corev1.Pod]{
			ObjectList: corev1.PodList{
				Items: valueSlice(pods),
			},
		},
	}
}

func (c *mockPodClient) setPods(pods []*corev1.Pod) {
	c.ObjectList = corev1.PodList{
		Items: valueSlice(pods),
	}
}

func (c *mockPodClient) upgradePods(
	t *testing.T,
	crdName string,
	ordinals ...int,
) {
	existing := ptrSlice(c.ObjectList.(corev1.PodList).Items)
	for _, ordinal := range ordinals {
		updatePod(t, crdName, ordinal, existing, newPodWithNewImage, true)
	}
	c.setPods(existing)
}

func (c *mockPodClient) deletePods(
	t *testing.T,
	crdName string,
	ordinals ...int,
) {
	existing := ptrSlice(c.ObjectList.(corev1.PodList).Items)
	for _, ordinal := range ordinals {
		updatePod(t, crdName, ordinal, existing, deletedPod, false)
	}
	c.setPods(existing)
}

func TestPodControl_Reconcile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const namespace = "test"

	t.Run("no changes", func(t *testing.T) {
		crd := defaultCRD()
		crd.Name = "hub"
		crd.Namespace = namespace
		crd.Spec.Replicas = 1

		pods, err := BuildPods(&crd, nil)
		require.NoError(t, err)
		existing := diff.New(nil, pods).Creates()

		require.Len(t, existing, 1)

		mClient := newMockPodClient(existing)

		syncInfo := map[string]*cosmosv1.SyncInfoPodStatus{
			"hub-0": {InSync: ptr(true)},
		}

		control := NewPodControl(mClient, nil)
		requeue, err := control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)
		require.False(t, requeue)

		require.Len(t, mClient.GotListOpts, 2)
		var listOpt client.ListOptions
		for _, opt := range mClient.GotListOpts {
			opt.ApplyToList(&listOpt)
		}
		require.Equal(t, namespace, listOpt.Namespace)
		require.Zero(t, listOpt.Limit)
		require.Equal(t, ".metadata.controller=hub", listOpt.FieldSelector.String())
	})

	t.Run("scale phase", func(t *testing.T) {
		crd := defaultCRD()
		crd.Name = "hub"
		crd.Namespace = namespace
		crd.Spec.Replicas = 3

		mClient := newMockPodClient([]*corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "hub-98"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "hub-99"}},
		})

		control := NewPodControl(mClient, nil)
		requeue, err := control.Reconcile(ctx, nopReporter, &crd, nil, nil)
		require.NoError(t, err)
		require.True(t, requeue)

		require.Equal(t, 3, mClient.CreateCount)
		require.Equal(t, 2, mClient.DeleteCount)

		require.NotEmpty(t, mClient.LastCreateObject.OwnerReferences)
		require.Equal(t, crd.Name, mClient.LastCreateObject.OwnerReferences[0].Name)
		require.Equal(t, "CosmosFullNode", mClient.LastCreateObject.OwnerReferences[0].Kind)
		require.True(t, *mClient.LastCreateObject.OwnerReferences[0].Controller)
	})

	t.Run("rollout phase", func(t *testing.T) {
		crd := defaultCRD()
		crd.Name = "hub"
		crd.Namespace = namespace
		crd.Spec.Replicas = 5
		crd.Spec.RolloutStrategy = cosmosv1.RolloutStrategy{
			MaxUnavailable: ptr(intstr.FromInt(2)),
		}

		pods, err := BuildPods(&crd, nil)
		require.NoError(t, err)

		existing := diff.New(nil, pods).Creates()
		setPodsReady(existing)
		mClient := newMockPodClient(existing)

		syncInfo := map[string]*cosmosv1.SyncInfoPodStatus{
			"hub-0": {InSync: ptr(true)},
			"hub-1": {InSync: ptr(true)},
			"hub-2": {InSync: ptr(true)},
			"hub-3": {InSync: ptr(true)},
			"hub-4": {InSync: ptr(true)},
		}

		control := NewPodControl(mClient, nil)

		control.computeRollout = func(maxUnavail *intstr.IntOrString, desired, ready int) int {
			require.EqualValues(t, crd.Spec.Replicas, desired)
			require.Equal(t, 5, ready) // mockPodFilter only returns 1 candidate as ready
			return kube.ComputeRollout(maxUnavail, desired, ready)
		}

		// Trigger updates
		crd.Spec.PodTemplate.Image = "new-image"
		requeue, err := control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)
		require.True(t, requeue)

		require.Zero(t, mClient.CreateCount)

		mClient.deletePods(t, crd.Name, 0, 1)

		control.computeRollout = func(maxUnavail *intstr.IntOrString, desired, ready int) int {
			require.EqualValues(t, crd.Spec.Replicas, desired)
			require.Equal(t, 3, ready) // only 3 should be marked ready because 2 are in the deleting state.
			return kube.ComputeRollout(maxUnavail, desired, ready)
		}

		requeue, err = control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)

		require.True(t, requeue)

		// pod status has not changed, but 0 and 1 are now in deleting state.
		// should not delete any more.
		require.Equal(t, 2, mClient.DeleteCount)

		// once pod deletion is complete, new pods are created with new image.
		mClient.upgradePods(t, crd.Name, 0, 1)

		syncInfo["hub-0"].InSync = nil
		syncInfo["hub-0"].Error = ptr("upgrade in progress")

		syncInfo["hub-1"].InSync = nil
		syncInfo["hub-1"].Error = ptr("upgrade in progress")

		control.computeRollout = func(maxUnavail *intstr.IntOrString, desired, ready int) int {
			require.EqualValues(t, crd.Spec.Replicas, desired)
			require.Equal(t, 3, ready)
			return kube.ComputeRollout(maxUnavail, desired, ready)
		}

		requeue, err = control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)
		require.True(t, requeue)

		require.Zero(t, mClient.CreateCount)

		// should not delete any more yet.
		require.Equal(t, 2, mClient.DeleteCount)
	})

	t.Run("rollout version upgrade rolling", func(t *testing.T) {
		crd := defaultCRD()
		crd.Name = "hub"
		crd.Namespace = namespace
		crd.Spec.Replicas = 5
		crd.Spec.RolloutStrategy = cosmosv1.RolloutStrategy{
			MaxUnavailable: ptr(intstr.FromInt(2)),
		}
		crd.Spec.ChainSpec = cosmosv1.ChainSpec{
			Versions: []cosmosv1.ChainVersion{
				{
					Image: "image",
				},
				{
					UpgradeHeight: 100,
					Image:         "new-image",
				},
			},
		}
		crd.Status.Height = make(map[string]uint64)

		pods, err := BuildPods(&crd, nil)
		require.NoError(t, err)
		existing := diff.New(nil, pods).Creates()
		setPodsReady(existing)

		mClient := newMockPodClient(existing)

		// pods are at upgrade height and reachable
		syncInfo := map[string]*cosmosv1.SyncInfoPodStatus{
			"hub-0": {
				Height: ptr(uint64(100)),
				InSync: ptr(true),
			},
			"hub-1": {
				Height: ptr(uint64(100)),
				InSync: ptr(true),
			},
			"hub-2": {
				Height: ptr(uint64(100)),
				InSync: ptr(true),
			},
			"hub-3": {
				Height: ptr(uint64(100)),
				InSync: ptr(true),
			},
			"hub-4": {
				Height: ptr(uint64(100)),
				InSync: ptr(true),
			},
		}

		control := NewPodControl(mClient, nil)

		control.computeRollout = func(maxUnavail *intstr.IntOrString, desired, ready int) int {
			require.EqualValues(t, crd.Spec.Replicas, desired)
			require.Equal(t, 5, ready) // all are reachable and reporting ready, so we will maintain liveliness.
			return kube.ComputeRollout(maxUnavail, desired, ready)
		}

		// Trigger updates
		for _, pod := range existing {
			crd.Status.Height[pod.Name] = 100
		}

		// Reconcile 1, should update 0 and 1

		requeue, err := control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)

		// only handled 2 updates, so should requeue.
		require.True(t, requeue)

		require.Zero(t, mClient.CreateCount)
		require.Equal(t, 2, mClient.DeleteCount)

		mClient.deletePods(t, crd.Name, 0, 1)

		control.computeRollout = func(maxUnavail *intstr.IntOrString, desired, ready int) int {
			require.EqualValues(t, crd.Spec.Replicas, desired)
			require.Equal(t, 3, ready) // only 3 should be marked ready because 2 are in the deleting state.
			return kube.ComputeRollout(maxUnavail, desired, ready)
		}

		requeue, err = control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)

		require.True(t, requeue)

		// pod status has not changed, but 0 and 1 are now in deleting state.
		// should not delete any more.
		require.Equal(t, 2, mClient.DeleteCount)

		mClient.upgradePods(t, crd.Name, 0, 1)

		// 0 and 1 are now unavailable, working on upgrade
		syncInfo["hub-0"].InSync = nil
		syncInfo["hub-0"].Error = ptr("upgrade in progress")

		syncInfo["hub-1"].InSync = nil
		syncInfo["hub-1"].Error = ptr("upgrade in progress")

		// Reconcile 2, should not update anything because 0 and 1 are still in progress.

		control.computeRollout = func(maxUnavail *intstr.IntOrString, desired, ready int) int {
			require.EqualValues(t, crd.Spec.Replicas, desired)
			require.Equal(t, 3, ready)
			return kube.ComputeRollout(maxUnavail, desired, ready)
		}

		requeue, err = control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)

		// no further updates yet, should requeue.
		require.True(t, requeue)

		require.Zero(t, mClient.CreateCount)

		// should not delete any more yet.
		require.Equal(t, 2, mClient.DeleteCount)

		// mock out that one of the pods completed the upgrade. should begin upgrading one more
		syncInfo["hub-0"].InSync = ptr(true)
		syncInfo["hub-0"].Height = ptr(uint64(101))
		syncInfo["hub-0"].Error = nil

		// Reconcile 3, should update pod 2 (only one) because 1 is still in progress, but 0 is done.

		control.computeRollout = func(maxUnavail *intstr.IntOrString, desired, ready int) int {
			require.EqualValues(t, crd.Spec.Replicas, desired)
			require.Equal(t, 4, ready)
			return kube.ComputeRollout(maxUnavail, desired, ready)
		}

		requeue, err = control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)

		// only handled 1 updates, so should requeue.
		require.True(t, requeue)

		require.Zero(t, mClient.CreateCount)

		// should delete one more
		require.Equal(t, 3, mClient.DeleteCount)

		mClient.deletePods(t, crd.Name, 2)

		control.computeRollout = func(maxUnavail *intstr.IntOrString, desired, ready int) int {
			require.EqualValues(t, crd.Spec.Replicas, desired)
			require.Equal(t, 3, ready) // only 3 should be marked ready because 2 is in the deleting state and 1 is still in progress upgrading.
			return kube.ComputeRollout(maxUnavail, desired, ready)
		}

		requeue, err = control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)

		require.True(t, requeue)

		// pod status has not changed, but 2 is now in deleting state.
		// should not delete any more.
		require.Equal(t, 3, mClient.DeleteCount)

		mClient.upgradePods(t, crd.Name, 2)

		// mock out that both pods completed the upgrade. should begin upgrading the last 2
		syncInfo["hub-1"].InSync = ptr(true)
		syncInfo["hub-1"].Height = ptr(uint64(101))
		syncInfo["hub-1"].Error = nil

		syncInfo["hub-2"].InSync = ptr(true)
		syncInfo["hub-2"].Height = ptr(uint64(101))
		syncInfo["hub-2"].Error = nil

		// Reconcile 4, should update 3 and 4 because the rest are done.

		control.computeRollout = func(maxUnavail *intstr.IntOrString, desired, ready int) int {
			require.EqualValues(t, crd.Spec.Replicas, desired)
			require.Equal(t, 5, ready)
			return kube.ComputeRollout(maxUnavail, desired, ready)
		}

		requeue, err = control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)

		// all updates are now handled, no longer need requeue.
		require.False(t, requeue)

		require.Zero(t, mClient.CreateCount)

		// should delete the last 2
		require.Equal(t, 5, mClient.DeleteCount)
	})

	t.Run("rollout version upgrade halt", func(t *testing.T) {
		crd := defaultCRD()
		crd.Name = "hub"
		crd.Namespace = namespace
		crd.Spec.Replicas = 5
		crd.Spec.RolloutStrategy = cosmosv1.RolloutStrategy{
			MaxUnavailable: ptr(intstr.FromInt(2)),
		}
		crd.Spec.ChainSpec = cosmosv1.ChainSpec{
			Versions: []cosmosv1.ChainVersion{
				{
					Image: "image",
				},
				{
					UpgradeHeight: 100,
					Image:         "new-image",
					SetHaltHeight: true,
				},
			},
		}
		crd.Status.Height = make(map[string]uint64)

		pods, err := BuildPods(&crd, nil)
		require.NoError(t, err)
		existing := diff.New(nil, pods).Creates()
		setPodsReady(existing)

		mClient := newMockPodClient(existing)

		// pods are at upgrade height and reachable
		syncInfo := map[string]*cosmosv1.SyncInfoPodStatus{
			"hub-0": {
				Height: ptr(uint64(100)),
				Error:  ptr("panic at upgrade height"),
			},
			"hub-1": {
				Height: ptr(uint64(100)),
				Error:  ptr("panic at upgrade height"),
			},
			"hub-2": {
				Height: ptr(uint64(100)),
				Error:  ptr("panic at upgrade height"),
			},
			"hub-3": {
				Height: ptr(uint64(100)),
				Error:  ptr("panic at upgrade height"),
			},
			"hub-4": {
				Height: ptr(uint64(100)),
				Error:  ptr("panic at upgrade height"),
			},
		}

		control := NewPodControl(mClient, nil)

		control.computeRollout = func(maxUnavail *intstr.IntOrString, desired, ready int) int {
			require.EqualValues(t, crd.Spec.Replicas, desired)
			require.Equal(t, 0, ready) // mockPodFilter returns no pods as synced, but all are at the upgrade height.
			return kube.ComputeRollout(maxUnavail, desired, ready)
		}

		// Trigger updates
		for _, pod := range existing {
			crd.Status.Height[pod.Name] = 100
		}

		requeue, err := control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)

		// all updates are handled, so should not requeue
		require.False(t, requeue)

		require.Zero(t, mClient.CreateCount)
		require.Equal(t, 5, mClient.DeleteCount)
	})
}

// revision hash must be taken without the revision label, the ordinal annotation,
// and Status (since BuildPods creates pods with empty Status).
func recalculatePodRevision(pod *corev1.Pod, ordinal int) {
	delete(pod.Labels, "app.kubernetes.io/revision")
	delete(pod.Annotations, "app.kubernetes.io/ordinal")
	// Temporarily clear status to match what BuildPods produces
	savedStatus := pod.Status
	pod.Status = corev1.PodStatus{}
	rev1 := diff.Adapt(pod, ordinal).Revision()
	pod.Status = savedStatus
	pod.Labels["app.kubernetes.io/revision"] = rev1
	pod.Annotations["app.kubernetes.io/ordinal"] = fmt.Sprintf("%d", ordinal)
}

func newPodWithNewImage(pod *corev1.Pod) {
	pod.DeletionTimestamp = nil
	pod.Spec.Containers[0].Image = "new-image"
	pod.Spec.InitContainers[1].Image = "new-image"
	// Set pod to ready state after upgrade
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		InitContainerStatuses: []corev1.ContainerStatus{
			{
				Name: "chain-init",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
				},
			},
			{
				Name: "version-check",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
				},
			},
		},
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		},
	}
}

func deletedPod(pod *corev1.Pod) {
	pod.DeletionTimestamp = ptr(metav1.Now())
}

func updatePod(t *testing.T, crdName string, ordinal int, pods []*corev1.Pod, updateFn func(pod *corev1.Pod), recalc bool) {
	podName := fmt.Sprintf("%s-%d", crdName, ordinal)
	for _, pod := range pods {
		if pod.Name == podName {
			updateFn(pod)
			if recalc {
				recalculatePodRevision(pod, ordinal)
			}
			return
		}
	}

	require.FailNow(t, "pod not found", podName)
}

func TestIsPodReadyForRollout(t *testing.T) {
	t.Parallel()

	t.Run("pod is ready when all conditions met", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				InitContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "version-check",
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
						},
					},
				},
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		require.True(t, isPodReadyForRollout(pod))
	})

	t.Run("pod is not ready when init container running", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				InitContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "version-check",
						State: corev1.ContainerState{
							Running: &corev1.ContainerStateRunning{},
						},
					},
				},
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				},
			},
		}
		require.False(t, isPodReadyForRollout(pod))
	})

	t.Run("pod is not ready when init container failed", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				InitContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "version-check",
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{ExitCode: 1},
						},
					},
				},
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				},
			},
		}
		require.False(t, isPodReadyForRollout(pod))
	})

	t.Run("pod is not ready when phase is pending", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
			},
		}
		require.False(t, isPodReadyForRollout(pod))
	})

	t.Run("pod is not ready when Ready condition is false", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				InitContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "version-check",
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
						},
					},
				},
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				},
			},
		}
		require.False(t, isPodReadyForRollout(pod))
	})

	t.Run("pod is not ready when no Ready condition exists", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				InitContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "version-check",
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
						},
					},
				},
				Conditions: []corev1.PodCondition{},
			},
		}
		require.False(t, isPodReadyForRollout(pod))
	})

	t.Run("pod with no init containers is ready when phase and condition are correct", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase:                 corev1.PodRunning,
				InitContainerStatuses: []corev1.ContainerStatus{}, // No init containers
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}
		require.True(t, isPodReadyForRollout(pod))
	})
}

func TestPodControl_Reconcile_InitContainerRunning(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const namespace = "test"

	t.Run("does not count pod as ready when init container is running", func(t *testing.T) {
		crd := defaultCRD()
		crd.Name = "hub"
		crd.Namespace = namespace
		crd.Spec.Replicas = 3
		crd.Spec.RolloutStrategy = cosmosv1.RolloutStrategy{
			MaxUnavailable: ptr(intstr.FromInt(1)),
		}

		pods, err := BuildPods(&crd, nil)
		require.NoError(t, err)
		existing := diff.New(nil, pods).Creates()

		// Set pod-0 as not ready (init container running)
		existing[0].Status = corev1.PodStatus{
			Phase: corev1.PodPending,
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "version-check",
					State: corev1.ContainerState{
						Running: &corev1.ContainerStateRunning{},
					},
				},
			},
		}

		// Set pod-1 and pod-2 as ready
		for i := 1; i < 3; i++ {
			existing[i].Status = corev1.PodStatus{
				Phase: corev1.PodRunning,
				InitContainerStatuses: []corev1.ContainerStatus{
					{
						Name: "version-check",
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{ExitCode: 0},
						},
					},
				},
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			}
		}

		mClient := newMockPodClient(existing)

		syncInfo := map[string]*cosmosv1.SyncInfoPodStatus{
			"hub-0": {InSync: ptr(true)}, // RPC says in sync, but K8s pod not ready
			"hub-1": {InSync: ptr(true)},
			"hub-2": {InSync: ptr(true)},
		}

		control := NewPodControl(mClient, nil)

		control.computeRollout = func(maxUnavail *intstr.IntOrString, desired, ready int) int {
			require.EqualValues(t, crd.Spec.Replicas, desired)
			// Only 2 pods should be ready (pod-0 has init container running)
			require.Equal(t, 2, ready)
			return kube.ComputeRollout(maxUnavail, desired, ready)
		}

		// Trigger updates by changing the image
		crd.Spec.PodTemplate.Image = "new-image"
		requeue, err := control.Reconcile(ctx, nopReporter, &crd, nil, syncInfo)
		require.NoError(t, err)
		require.True(t, requeue)

		// With maxUnavailable=1 and ready=2, we can delete 0 pods
		// because 2 - 1 = 1 minAvail, ready(2) > minAvail(1), target = 1 - (3-2) = 0
		// Wait, let's recalculate: unavail=1, minAvail=3-1=2, ready=2, ready<=minAvail, so target=0
		require.Zero(t, mClient.DeleteCount)
	})
}
