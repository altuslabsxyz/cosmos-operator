/*
Copyright 2025 B-Harvest Corporation.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fullnode

import (
	"context"
	"fmt"
	"time"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DefaultRecoveryImage   = "busybox:latest"
	DefaultMaxRetries      = int32(3)
	DefaultRateLimitWindow = "1h"
)

// DeepRecoveryManager handles advanced recovery for stuck pods.
type DeepRecoveryManager struct {
	client client.Client
}

// NewDeepRecoveryManager creates a new DeepRecoveryManager.
func NewDeepRecoveryManager(c client.Client) *DeepRecoveryManager {
	return &DeepRecoveryManager{client: c}
}

// PodStuckInfo contains information about a potentially stuck pod.
type PodStuckInfo struct {
	PodName       string
	CurrentHeight uint64
	MaxHeight     uint64
	PVCName       string
}

// CheckAndTriggerRecovery checks for stuck pods and triggers recovery if needed.
// Returns true if any recovery action was taken.
func (m *DeepRecoveryManager) CheckAndTriggerRecovery(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	stuckPods []PodStuckInfo,
) (bool, error) {
	spec := crd.Spec.SelfHeal.HeightDriftMitigation.DeepRecovery
	if spec == nil || spec.Suspend {
		return false, nil
	}

	stuckDuration, err := time.ParseDuration(spec.StuckDuration)
	if err != nil {
		return false, fmt.Errorf("invalid stuck duration: %w", err)
	}

	var actionTaken bool
	for _, pod := range stuckPods {
		taken, err := m.processStuckPod(ctx, crd, pod, spec, stuckDuration)
		if err != nil {
			return actionTaken, err
		}
		actionTaken = actionTaken || taken
	}

	return actionTaken, nil
}

func (m *DeepRecoveryManager) processStuckPod(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	pod PodStuckInfo,
	spec *cosmosv1.DeepRecoverySpec,
	stuckDuration time.Duration,
) (bool, error) {
	status := m.getOrCreateStatus(crd, pod.PodName)
	now := metav1.Now()

	switch status.Phase {
	case cosmosv1.DeepRecoveryPhaseIdle, "":
		// First detection - mark as detected and start tracking
		return m.handleIdlePhase(ctx, crd, pod, status, now)

	case cosmosv1.DeepRecoveryPhaseDetected:
		// Check if stuck duration has passed
		return m.handleDetectedPhase(ctx, crd, pod, status, spec, stuckDuration, now)

	case cosmosv1.DeepRecoveryPhaseSnapshotCreating:
		// Check if snapshot is ready
		return m.handleSnapshotCreatingPhase(ctx, crd, pod, status)

	case cosmosv1.DeepRecoveryPhaseSnapshotReady, cosmosv1.DeepRecoveryPhaseRecoveryRunning:
		// Check recovery pod status
		return m.handleRecoveryPhase(ctx, crd, pod, status)

	case cosmosv1.DeepRecoveryPhaseRecoveryCompleted:
		// Reset after successful recovery
		return m.handleCompletedPhase(ctx, crd, pod, status)

	case cosmosv1.DeepRecoveryPhaseRecoveryFailed:
		// Check rate limit and potentially retry
		return m.handleFailedPhase(ctx, crd, pod, status, spec)

	case cosmosv1.DeepRecoveryPhaseRateLimited:
		// Check if rate limit window has passed
		return m.handleRateLimitedPhase(ctx, crd, pod, status, spec)
	}

	return false, nil
}

func (m *DeepRecoveryManager) getOrCreateStatus(crd *cosmosv1.CosmosFullNode, podName string) *cosmosv1.DeepRecoveryStatus {
	if crd.Status.SelfHealing.DeepRecovery == nil {
		crd.Status.SelfHealing.DeepRecovery = make(map[string]*cosmosv1.DeepRecoveryStatus)
	}
	if crd.Status.SelfHealing.DeepRecovery[podName] == nil {
		crd.Status.SelfHealing.DeepRecovery[podName] = &cosmosv1.DeepRecoveryStatus{
			Phase: cosmosv1.DeepRecoveryPhaseIdle,
		}
	}
	return crd.Status.SelfHealing.DeepRecovery[podName]
}

func (m *DeepRecoveryManager) handleIdlePhase(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	pod PodStuckInfo,
	status *cosmosv1.DeepRecoveryStatus,
	now metav1.Time,
) (bool, error) {
	status.Phase = cosmosv1.DeepRecoveryPhaseDetected
	status.StuckHeight = pod.CurrentHeight
	status.DetectedAt = &now
	status.Message = fmt.Sprintf("Stuck at height %d detected, monitoring", pod.CurrentHeight)
	return true, nil
}

func (m *DeepRecoveryManager) handleDetectedPhase(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	pod PodStuckInfo,
	status *cosmosv1.DeepRecoveryStatus,
	spec *cosmosv1.DeepRecoverySpec,
	stuckDuration time.Duration,
	now metav1.Time,
) (bool, error) {
	// Check if height has changed (pod recovered on its own)
	if pod.CurrentHeight > status.StuckHeight {
		status.Phase = cosmosv1.DeepRecoveryPhaseIdle
		status.Message = "Height started moving, resetting"
		status.StuckHeight = 0
		status.DetectedAt = nil
		return true, nil
	}

	// Check if stuck duration has passed
	if status.DetectedAt == nil {
		return false, nil
	}
	if time.Since(status.DetectedAt.Time) < stuckDuration {
		return false, nil
	}

	// Check rate limit
	if m.isRateLimited(status, spec) {
		status.Phase = cosmosv1.DeepRecoveryPhaseRateLimited
		status.Message = "Rate limit exceeded, waiting"
		return true, nil
	}

	// Start recovery - create snapshot if enabled
	if spec.VolumeSnapshot != nil && spec.VolumeSnapshot.Enabled {
		snapshotName, err := m.createVolumeSnapshot(ctx, crd, pod, spec)
		if err != nil {
			status.Message = fmt.Sprintf("Failed to create snapshot: %v", err)
			return false, err
		}
		status.Phase = cosmosv1.DeepRecoveryPhaseSnapshotCreating
		status.SnapshotName = snapshotName
		status.Message = "Creating VolumeSnapshot backup"
		return true, nil
	}

	// No snapshot needed - create recovery pod directly
	recoveryPodName, err := m.createRecoveryPod(ctx, crd, pod, spec)
	if err != nil {
		status.Message = fmt.Sprintf("Failed to create recovery pod: %v", err)
		return false, err
	}

	status.Phase = cosmosv1.DeepRecoveryPhaseRecoveryRunning
	status.RecoveryPodName = recoveryPodName
	status.RetryCount++
	status.LastAttemptAt = &now
	status.Message = "Running recovery script"
	return true, nil
}

func (m *DeepRecoveryManager) handleSnapshotCreatingPhase(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	pod PodStuckInfo,
	status *cosmosv1.DeepRecoveryStatus,
) (bool, error) {
	if status.SnapshotName == "" {
		return false, nil
	}

	snapshot := &snapshotv1.VolumeSnapshot{}
	if err := m.client.Get(ctx, client.ObjectKey{
		Name:      status.SnapshotName,
		Namespace: crd.Namespace,
	}, snapshot); err != nil {
		return false, client.IgnoreNotFound(err)
	}

	if snapshot.Status != nil && snapshot.Status.ReadyToUse != nil && *snapshot.Status.ReadyToUse {
		status.Phase = cosmosv1.DeepRecoveryPhaseSnapshotReady
		status.Message = "Snapshot ready, creating recovery pod"
		return true, nil
	}

	return false, nil
}

func (m *DeepRecoveryManager) handleRecoveryPhase(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	pod PodStuckInfo,
	status *cosmosv1.DeepRecoveryStatus,
) (bool, error) {
	spec := crd.Spec.SelfHeal.HeightDriftMitigation.DeepRecovery

	// If in SnapshotReady phase, create recovery pod
	if status.Phase == cosmosv1.DeepRecoveryPhaseSnapshotReady {
		recoveryPodName, err := m.createRecoveryPod(ctx, crd, pod, spec)
		if err != nil {
			status.Message = fmt.Sprintf("Failed to create recovery pod: %v", err)
			return false, err
		}

		now := metav1.Now()
		status.Phase = cosmosv1.DeepRecoveryPhaseRecoveryRunning
		status.RecoveryPodName = recoveryPodName
		status.RetryCount++
		status.LastAttemptAt = &now
		status.Message = "Running recovery script"
		return true, nil
	}

	// Check recovery pod status
	if status.RecoveryPodName == "" {
		return false, nil
	}

	recoveryPod := &corev1.Pod{}
	if err := m.client.Get(ctx, client.ObjectKey{
		Name:      status.RecoveryPodName,
		Namespace: crd.Namespace,
	}, recoveryPod); err != nil {
		return false, client.IgnoreNotFound(err)
	}

	switch recoveryPod.Status.Phase {
	case corev1.PodSucceeded:
		status.Phase = cosmosv1.DeepRecoveryPhaseRecoveryCompleted
		status.Message = "Recovery completed successfully"
		return true, nil

	case corev1.PodFailed:
		status.Phase = cosmosv1.DeepRecoveryPhaseRecoveryFailed
		status.Message = "Recovery pod failed"
		return true, nil
	}

	return false, nil
}

func (m *DeepRecoveryManager) handleCompletedPhase(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	pod PodStuckInfo,
	status *cosmosv1.DeepRecoveryStatus,
) (bool, error) {
	// Check if height has recovered
	if pod.CurrentHeight > status.StuckHeight {
		// Reset status
		status.Phase = cosmosv1.DeepRecoveryPhaseIdle
		status.StuckHeight = 0
		status.DetectedAt = nil
		status.SnapshotName = ""
		status.RecoveryPodName = ""
		status.RetryCount = 0
		status.Message = "Height recovered"
		return true, nil
	}

	// Height still stuck after recovery - might need manual intervention
	status.Message = "Recovery completed but height still stuck"
	return false, nil
}

func (m *DeepRecoveryManager) handleFailedPhase(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	pod PodStuckInfo,
	status *cosmosv1.DeepRecoveryStatus,
	spec *cosmosv1.DeepRecoverySpec,
) (bool, error) {
	if m.isRateLimited(status, spec) {
		status.Phase = cosmosv1.DeepRecoveryPhaseRateLimited
		status.Message = "Rate limit exceeded after failure"
		return true, nil
	}

	// Retry by going back to detected phase
	status.Phase = cosmosv1.DeepRecoveryPhaseDetected
	status.Message = "Retrying recovery"
	return true, nil
}

func (m *DeepRecoveryManager) handleRateLimitedPhase(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	pod PodStuckInfo,
	status *cosmosv1.DeepRecoveryStatus,
	spec *cosmosv1.DeepRecoverySpec,
) (bool, error) {
	rateLimitWindow := DefaultRateLimitWindow
	if spec.RateLimitWindow != "" {
		rateLimitWindow = spec.RateLimitWindow
	}

	windowDuration, err := time.ParseDuration(rateLimitWindow)
	if err != nil {
		windowDuration = time.Hour
	}

	if status.LastAttemptAt != nil && time.Since(status.LastAttemptAt.Time) >= windowDuration {
		// Rate limit window has passed - reset and retry
		status.Phase = cosmosv1.DeepRecoveryPhaseDetected
		status.RetryCount = 0
		status.Message = "Rate limit window passed, retrying"
		return true, nil
	}

	return false, nil
}

func (m *DeepRecoveryManager) isRateLimited(status *cosmosv1.DeepRecoveryStatus, spec *cosmosv1.DeepRecoverySpec) bool {
	maxRetries := DefaultMaxRetries
	if spec.MaxRetries != nil {
		maxRetries = *spec.MaxRetries
	}

	rateLimitWindow := DefaultRateLimitWindow
	if spec.RateLimitWindow != "" {
		rateLimitWindow = spec.RateLimitWindow
	}

	windowDuration, err := time.ParseDuration(rateLimitWindow)
	if err != nil {
		windowDuration = time.Hour
	}

	// Check if within rate limit window
	if status.LastAttemptAt != nil && time.Since(status.LastAttemptAt.Time) < windowDuration {
		return status.RetryCount >= maxRetries
	}

	return false
}

func (m *DeepRecoveryManager) createVolumeSnapshot(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	pod PodStuckInfo,
	spec *cosmosv1.DeepRecoverySpec,
) (string, error) {
	snapshotName := fmt.Sprintf("%s-recovery-%d", pod.PVCName, time.Now().Unix())

	snapshot := &snapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName,
			Namespace: crd.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by":   "cosmos-operator",
				"cosmos.bharvest.io/recovery":    crd.Name,
				"cosmos.bharvest.io/pod":         pod.PodName,
				"cosmos.bharvest.io/type":        "deep-recovery-backup",
			},
		},
		Spec: snapshotv1.VolumeSnapshotSpec{
			Source: snapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pod.PVCName,
			},
		},
	}

	if spec.VolumeSnapshot.VolumeSnapshotClassName != "" {
		snapshot.Spec.VolumeSnapshotClassName = &spec.VolumeSnapshot.VolumeSnapshotClassName
	}

	if err := m.client.Create(ctx, snapshot); err != nil {
		return "", fmt.Errorf("create volume snapshot: %w", err)
	}

	return snapshotName, nil
}

func (m *DeepRecoveryManager) createRecoveryPod(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	podInfo PodStuckInfo,
	spec *cosmosv1.DeepRecoverySpec,
) (string, error) {
	image := DefaultRecoveryImage
	var command []string
	var resources corev1.ResourceRequirements

	if spec.PodTemplate != nil {
		if spec.PodTemplate.Image != "" {
			image = spec.PodTemplate.Image
		}
		if len(spec.PodTemplate.Command) > 0 {
			command = spec.PodTemplate.Command
		}
		if spec.PodTemplate.Resources != nil {
			resources = *spec.PodTemplate.Resources
		}
	}

	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		}
	}

	recoveryPodName := fmt.Sprintf("%s-recovery-%d", podInfo.PodName, time.Now().Unix())

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      recoveryPodName,
			Namespace: crd.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by":   "cosmos-operator",
				"cosmos.bharvest.io/recovery":    crd.Name,
				"cosmos.bharvest.io/pod":         podInfo.PodName,
				"cosmos.bharvest.io/type":        "recovery-pod",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:           ptr(int64(1025)),
				RunAsGroup:          ptr(int64(1025)),
				FSGroup:             ptr(int64(1025)),
				FSGroupChangePolicy: ptr(corev1.FSGroupChangeOnRootMismatch),
				RunAsNonRoot:        ptr(true),
				SeccompProfile: &corev1.SeccompProfile{
					Type: corev1.SeccompProfileTypeRuntimeDefault,
				},
			},
			Containers: []corev1.Container{
				{
					Name:            "recovery",
					Image:           image,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Command:         command,
					Args: []string{
						"-c",
						spec.RecoveryScript,
					},
					Env: []corev1.EnvVar{
						{Name: "POD_NAME", Value: podInfo.PodName},
						{Name: "POD_NAMESPACE", Value: crd.Namespace},
						{Name: "CURRENT_HEIGHT", Value: fmt.Sprintf("%d", podInfo.CurrentHeight)},
						{Name: "PVC_NAME", Value: podInfo.PVCName},
						{Name: "CHAIN_HOME", Value: "/chain-home"},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "chain-data",
							MountPath: "/chain-home",
						},
					},
					Resources: resources,
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "chain-data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: podInfo.PVCName,
						},
					},
				},
			},
		},
	}

	// Set default command if not specified
	if len(command) == 0 {
		pod.Spec.Containers[0].Command = []string{"/bin/sh"}
	}

	if err := m.client.Create(ctx, pod); err != nil {
		return "", fmt.Errorf("create recovery pod: %w", err)
	}

	return recoveryPodName, nil
}

// CleanupRecoveredPods removes recovery pods and old snapshots for pods that have recovered.
func (m *DeepRecoveryManager) CleanupRecoveredPods(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
) error {
	if crd.Status.SelfHealing.DeepRecovery == nil {
		return nil
	}

	spec := crd.Spec.SelfHeal.HeightDriftMitigation.DeepRecovery
	if spec == nil {
		return nil
	}

	for podName, status := range crd.Status.SelfHealing.DeepRecovery {
		if status.Phase != cosmosv1.DeepRecoveryPhaseIdle {
			continue
		}

		// Clean up recovery pod
		if status.RecoveryPodName != "" {
			recoveryPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      status.RecoveryPodName,
					Namespace: crd.Namespace,
				},
			}
			_ = m.client.Delete(ctx, recoveryPod)
		}

		// Clean up old snapshots (keep retention count)
		if spec.VolumeSnapshot != nil && spec.VolumeSnapshot.Enabled {
			if err := m.cleanupOldSnapshots(ctx, crd, podName, spec.VolumeSnapshot); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *DeepRecoveryManager) cleanupOldSnapshots(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	podName string,
	spec *cosmosv1.RecoveryVolumeSnapshotSpec,
) error {
	retentionCount := int32(3)
	if spec.RetentionCount != nil {
		retentionCount = *spec.RetentionCount
	}

	snapshots := &snapshotv1.VolumeSnapshotList{}
	if err := m.client.List(ctx, snapshots,
		client.InNamespace(crd.Namespace),
		client.MatchingLabels{
			"cosmos.bharvest.io/recovery": crd.Name,
			"cosmos.bharvest.io/pod":      podName,
			"cosmos.bharvest.io/type":     "deep-recovery-backup",
		},
	); err != nil {
		return err
	}

	if int32(len(snapshots.Items)) <= retentionCount {
		return nil
	}

	// Sort by creation time and delete oldest
	// Simple approach: delete if over retention count
	for i := int32(0); i < int32(len(snapshots.Items))-retentionCount; i++ {
		_ = m.client.Delete(ctx, &snapshots.Items[i])
	}

	return nil
}
