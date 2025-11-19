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

package controllers

import (
	"context"
	"fmt"
	"time"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	"github.com/b-harvest/cosmos-operator/internal/kube"
	"github.com/b-harvest/cosmos-operator/internal/stuckheight"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// StuckHeightRecoveryReconciler reconciles a StuckHeightRecovery object
type StuckHeightRecoveryReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	recorder           record.EventRecorder
	heightMonitor      *stuckheight.HeightMonitor
	snapshotCreator    *stuckheight.SnapshotCreator
	rateLimiter        *stuckheight.RateLimiter
	recoveryPodBuilder *stuckheight.RecoveryPodBuilder
}

// NewStuckHeightRecoveryReconciler creates a new StuckHeightRecoveryReconciler
func NewStuckHeightRecoveryReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	recorder record.EventRecorder,
) *StuckHeightRecoveryReconciler {
	return &StuckHeightRecoveryReconciler{
		Client:             client,
		Scheme:             scheme,
		recorder:           recorder,
		heightMonitor:      stuckheight.NewHeightMonitor(client),
		snapshotCreator:    stuckheight.NewSnapshotCreator(client),
		rateLimiter:        stuckheight.NewRateLimiter(),
		recoveryPodBuilder: stuckheight.NewRecoveryPodBuilder(client),
	}
}

// +kubebuilder:rbac:groups=cosmos.bharvest.io,resources=stuckheightrecoveries,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cosmos.bharvest.io,resources=stuckheightrecoveries/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cosmos.bharvest.io,resources=stuckheightrecoveries/finalizers,verbs=update
// +kubebuilder:rbac:groups=cosmos.bharvest.io,resources=cosmosfullnodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop
func (r *StuckHeightRecoveryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName("StuckHeightRecovery")
	logger.V(1).Info("Entering reconcile loop", "request", req.NamespacedName)

	// Fetch the StuckHeightRecovery instance
	recovery := &cosmosv1.StuckHeightRecovery{}
	if err := r.Get(ctx, req.NamespacedName, recovery); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Create event reporter
	reporter := kube.NewEventReporter(logger, r.recorder, recovery)

	// Handle deletion
	if !recovery.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, recovery, reporter)
	}

	// Add finalizer if not present
	const finalizerName = "stuckheightrecovery.cosmos.bharvest.io/finalizer"
	if !containsString(recovery.Finalizers, finalizerName) {
		recovery.Finalizers = append(recovery.Finalizers, finalizerName)
		if err := r.Update(ctx, recovery); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if suspended
	if recovery.Spec.Suspend {
		if recovery.Status.Phase != cosmosv1.StuckHeightRecoveryPhaseSuspended {
			recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseSuspended
			recovery.Status.Message = "Recovery is suspended"
			if err := r.Status().Update(ctx, recovery); err != nil {
				return ctrl.Result{}, err
			}
			reporter.Info("Recovery suspended")
		}
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// Fetch the referenced CosmosFullNode
	crd := &cosmosv1.CosmosFullNode{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      recovery.Spec.FullNodeRef.Name,
		Namespace: recovery.Namespace,
	}, crd); err != nil {
		reporter.Error(err, "Failed to get CosmosFullNode")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRecoveryFailed
		recovery.Status.Message = fmt.Sprintf("CosmosFullNode not found: %s", recovery.Spec.FullNodeRef.Name)
		_ = r.Status().Update(ctx, recovery)
		return ctrl.Result{RequeueAfter: 60 * time.Second}, err
	}

	// Update observed generation
	recovery.Status.ObservedGeneration = recovery.Generation

	// Initialize StuckPods map if needed
	if recovery.Status.StuckPods == nil {
		recovery.Status.StuckPods = make(map[string]*cosmosv1.StuckPodRecoveryStatus)
	}

	// Handle different phases
	switch recovery.Status.Phase {
	case "", cosmosv1.StuckHeightRecoveryPhaseMonitoring:
		return r.handleMonitoring(ctx, recovery, crd, reporter)

	case cosmosv1.StuckHeightRecoveryPhaseRecovering:
		return r.handleRecovering(ctx, recovery, crd, reporter)

	case cosmosv1.StuckHeightRecoveryPhaseRateLimited:
		return r.handleRateLimited(ctx, recovery, crd, reporter)

	// Deprecated phases - redirect to new logic
	case cosmosv1.StuckHeightRecoveryPhaseHeightStuck,
		cosmosv1.StuckHeightRecoveryPhaseCreatingSnapshot,
		cosmosv1.StuckHeightRecoveryPhaseWaitingForSnapshot,
		cosmosv1.StuckHeightRecoveryPhaseRunningRecovery,
		cosmosv1.StuckHeightRecoveryPhaseWaitingForRecovery,
		cosmosv1.StuckHeightRecoveryPhaseRecoveryComplete,
		cosmosv1.StuckHeightRecoveryPhaseRecoveryFailed:
		// Migrate from old single-pod logic to new multi-pod logic
		reporter.Info("Migrating from legacy phase to new multi-pod logic")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseMonitoring
		recovery.Status.StuckPods = make(map[string]*cosmosv1.StuckPodRecoveryStatus)
		if err := r.Status().Update(ctx, recovery); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *StuckHeightRecoveryReconciler) handleMonitoring(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	// Check all pods for stuck height and recovery
	result, err := r.heightMonitor.CheckStuckHeight(ctx, crd, recovery)
	if err != nil {
		reporter.Error(err, "Failed to check stuck height")
		recovery.Status.Message = fmt.Sprintf("Error checking height: %v", err)
		_ = r.Status().Update(ctx, recovery)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	// Update last observed max height
	now := metav1.Now()
	if recovery.Status.LastObservedHeight != result.MaxHeight {
		recovery.Status.LastObservedHeight = result.MaxHeight
		recovery.Status.LastHeightUpdateTime = &now

		// Reset rate limiter when max height changes
		r.rateLimiter.ResetWindow(recovery)
	}

	// Handle lagging pods - track them to detect if they become stuck
	for podName, laggingHeight := range result.LaggingPods {
		// Check if we're already tracking this pod
		if existingPod, exists := recovery.Status.StuckPods[podName]; exists {
			// Update current height
			existingPod.CurrentHeight = &laggingHeight
			existingPod.LastUpdateTime = &now

			// Check if height has changed - if so, remove from tracking
			if existingPod.StuckAtHeight != laggingHeight {
				reporter.Info(fmt.Sprintf("Pod %s height changed (%d -> %d), removing from tracking",
					podName, existingPod.StuckAtHeight, laggingHeight))
				delete(recovery.Status.StuckPods, podName)
			}
		} else {
			// Start tracking this lagging pod
			reporter.Info(fmt.Sprintf("Tracking lagging pod %s at height %d (max: %d)", podName, laggingHeight, result.MaxHeight))
			recovery.Status.StuckPods[podName] = &cosmosv1.StuckPodRecoveryStatus{
				PodName:        podName,
				StuckAtHeight:  laggingHeight,
				CurrentHeight:  &laggingHeight,
				DetectedAt:     now,
				Phase:          cosmosv1.PodRecoveryPhaseLagging,
				Message:        fmt.Sprintf("Lagging %d blocks behind max height %d", result.MaxHeight-laggingHeight, result.MaxHeight),
				LastUpdateTime: &now,
			}
		}
	}

	// Handle pods that have recovered on their own
	for podName, currentHeight := range result.RecoveredPods {
		stuckPod := recovery.Status.StuckPods[podName]
		reporter.Info(fmt.Sprintf("Pod %s recovered on its own (height %d -> %d)", podName, stuckPod.StuckAtHeight, currentHeight))
		reporter.RecordInfo("PodHeightRecovered", fmt.Sprintf("Pod %s height started moving again", podName))

		// Update pod status to HeightRecovered
		stuckPod.Phase = cosmosv1.PodRecoveryPhaseHeightRecovered
		stuckPod.CurrentHeight = &currentHeight
		stuckPod.Message = fmt.Sprintf("Height recovered from %d to %d", stuckPod.StuckAtHeight, currentHeight)
		stuckPod.LastUpdateTime = &now

		// Add to history
		recovery.Status.RecoveryHistory = append(recovery.Status.RecoveryHistory, cosmosv1.RecoveryHistoryEntry{
			Timestamp: now,
			PodName:   podName,
			Height:    currentHeight,
			EventType: "height_recovered",
			Message:   fmt.Sprintf("Pod height started moving again from %d to %d", stuckPod.StuckAtHeight, currentHeight),
		})

		// Remove from stuck pods after recording
		delete(recovery.Status.StuckPods, podName)
	}

	// Handle newly stuck pods
	for podName, stuckHeight := range result.NewlyStuckPods {
		reporter.Info(fmt.Sprintf("Detected stuck pod %s at height %d (max: %d)", podName, stuckHeight, result.MaxHeight))
		reporter.RecordInfo("PodStuckDetected", fmt.Sprintf("Pod %s stuck at height %d", podName, stuckHeight))

		// Create stuck pod entry
		recovery.Status.StuckPods[podName] = &cosmosv1.StuckPodRecoveryStatus{
			PodName:       podName,
			StuckAtHeight: stuckHeight,
			CurrentHeight: &stuckHeight,
			DetectedAt:    now,
			Phase:         cosmosv1.PodRecoveryPhaseStuck,
			Message:       fmt.Sprintf("Stuck at height %d, max height is %d", stuckHeight, result.MaxHeight),
			LastUpdateTime: &now,
		}

		// Add to history
		recovery.Status.RecoveryHistory = append(recovery.Status.RecoveryHistory, cosmosv1.RecoveryHistoryEntry{
			Timestamp: now,
			PodName:   podName,
			Height:    stuckHeight,
			EventType: "detected",
			Message:   fmt.Sprintf("Pod stuck detected at height %d", stuckHeight),
		})
	}

	// Update overall phase and message
	if len(recovery.Status.StuckPods) > 0 {
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRecovering
		recovery.Status.Message = fmt.Sprintf("Recovering %d stuck pod(s)", len(recovery.Status.StuckPods))
	} else {
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseMonitoring
		recovery.Status.Message = fmt.Sprintf("Monitoring height: %d", result.MaxHeight)
	}

	if err := r.Status().Update(ctx, recovery); err != nil {
		return ctrl.Result{}, err
	}

	// If there are stuck pods, switch to recovering phase immediately
	if len(recovery.Status.StuckPods) > 0 {
		return ctrl.Result{Requeue: true}, nil
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *StuckHeightRecoveryReconciler) handleRecovering(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	// First, check for any pods that have recovered on their own
	result, err := r.heightMonitor.CheckStuckHeight(ctx, crd, recovery)
	if err == nil {
		// Handle auto-recovered pods
		now := metav1.Now()
		for podName, currentHeight := range result.RecoveredPods {
			if stuckPod, exists := recovery.Status.StuckPods[podName]; exists {
				reporter.Info(fmt.Sprintf("Pod %s recovered on its own during recovery (height %d -> %d)",
					podName, stuckPod.StuckAtHeight, currentHeight))

				stuckPod.Phase = cosmosv1.PodRecoveryPhaseHeightRecovered
				stuckPod.CurrentHeight = &currentHeight
				stuckPod.Message = "Height started moving again during recovery"
				stuckPod.LastUpdateTime = &now

				recovery.Status.RecoveryHistory = append(recovery.Status.RecoveryHistory, cosmosv1.RecoveryHistoryEntry{
					Timestamp: now,
					PodName:   podName,
					Height:    currentHeight,
					EventType: "height_recovered",
					Message:   "Pod recovered during recovery process",
				})
			}
		}
	}

	// Process each stuck pod based on its phase
	podsNeedingWork := false
	for _, stuckPod := range recovery.Status.StuckPods {
		// Skip pods that are just being tracked or have already recovered/failed
		if stuckPod.Phase == cosmosv1.PodRecoveryPhaseLagging ||
			stuckPod.Phase == cosmosv1.PodRecoveryPhaseRecovered ||
			stuckPod.Phase == cosmosv1.PodRecoveryPhaseHeightRecovered ||
			stuckPod.Phase == cosmosv1.PodRecoveryPhaseFailed {
			continue
		}

		podsNeedingWork = true

		switch stuckPod.Phase {
		case cosmosv1.PodRecoveryPhaseStuck:
			if err := r.handlePodStuck(ctx, recovery, crd, stuckPod, reporter); err != nil {
				return ctrl.Result{RequeueAfter: 10 * time.Second}, err
			}

		case cosmosv1.PodRecoveryPhaseCreatingSnapshot:
			if err := r.handlePodCreatingSnapshot(ctx, recovery, crd, stuckPod, reporter); err != nil {
				return ctrl.Result{RequeueAfter: 10 * time.Second}, err
			}

		case cosmosv1.PodRecoveryPhaseWaitingForSnapshot:
			if err := r.handlePodWaitingForSnapshot(ctx, recovery, stuckPod, reporter); err != nil {
				return ctrl.Result{RequeueAfter: 10 * time.Second}, err
			}

		case cosmosv1.PodRecoveryPhaseRunningRecovery:
			if err := r.handlePodRunningRecovery(ctx, recovery, stuckPod, reporter); err != nil {
				return ctrl.Result{RequeueAfter: 10 * time.Second}, err
			}

		case cosmosv1.PodRecoveryPhaseWaitingForRecovery:
			if err := r.handlePodWaitingForRecovery(ctx, recovery, stuckPod, reporter); err != nil {
				return ctrl.Result{RequeueAfter: 10 * time.Second}, err
			}
		}
	}

	// Clean up completed pods and update CosmosFullNode status
	if err := r.cleanupCompletedPods(ctx, recovery, crd, reporter); err != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// Update overall status
	if err := r.updateOverallStatus(ctx, recovery); err != nil {
		return ctrl.Result{}, err
	}

	// If no pods need work, go back to monitoring
	if !podsNeedingWork && len(recovery.Status.StuckPods) == 0 {
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseMonitoring
		recovery.Status.Message = "All pods recovered, returning to monitoring"
		if err := r.Status().Update(ctx, recovery); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *StuckHeightRecoveryReconciler) handlePodStuck(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	stuckPod *cosmosv1.StuckPodRecoveryStatus,
	reporter kube.EventReporter,
) error {
	// Check rate limit
	canAttempt, message, err := r.rateLimiter.CanAttemptRecovery(recovery)
	if err != nil {
		return err
	}

	if !canAttempt {
		reporter.RecordInfo("RateLimited", message)
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRateLimited
		recovery.Status.Message = message
		return r.Status().Update(ctx, recovery)
	}

	// Record attempt
	r.rateLimiter.RecordAttempt(recovery)
	now := metav1.Now()
	recovery.Status.LastRecoveryTime = &now

	// Get PVC name for this pod
	pvcName, err := r.snapshotCreator.GetPVCForPod(ctx, recovery.Namespace, stuckPod.PodName)
	if err != nil {
		stuckPod.Phase = cosmosv1.PodRecoveryPhaseFailed
		stuckPod.Message = fmt.Sprintf("Failed to get PVC name: %v", err)
		stuckPod.LastUpdateTime = &now

		recovery.Status.RecoveryHistory = append(recovery.Status.RecoveryHistory, cosmosv1.RecoveryHistoryEntry{
			Timestamp: now,
			PodName:   stuckPod.PodName,
			Height:    stuckPod.StuckAtHeight,
			EventType: "failed",
			Message:   fmt.Sprintf("Failed to get PVC: %v", err),
		})

		return r.Status().Update(ctx, recovery)
	}

	stuckPod.PVCName = pvcName
	reporter.Info(fmt.Sprintf("Found PVC %s for pod %s", pvcName, stuckPod.PodName))

	// Decide next phase based on snapshot requirement
	if recovery.Spec.CreateVolumeSnapshot {
		stuckPod.Phase = cosmosv1.PodRecoveryPhaseCreatingSnapshot
		stuckPod.Message = "Creating VolumeSnapshot"
	} else {
		stuckPod.Phase = cosmosv1.PodRecoveryPhaseRunningRecovery
		stuckPod.Message = "Starting recovery script"
	}
	stuckPod.LastUpdateTime = &now

	return r.Status().Update(ctx, recovery)
}

func (r *StuckHeightRecoveryReconciler) handlePodCreatingSnapshot(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	stuckPod *cosmosv1.StuckPodRecoveryStatus,
	reporter kube.EventReporter,
) error {
	// Mark pod for deletion in CosmosFullNode status
	if crd.Status.StuckHeightRecoveryStatus == nil {
		crd.Status.StuckHeightRecoveryStatus = make(map[string]cosmosv1.StuckHeightRecoveryPodStatus)
	}

	recoveryKey := fmt.Sprintf("%s-%s", recovery.Name, stuckPod.PodName)
	if _, exists := crd.Status.StuckHeightRecoveryStatus[recoveryKey]; !exists {
		crd.Status.StuckHeightRecoveryStatus[recoveryKey] = cosmosv1.StuckHeightRecoveryPodStatus{
			PodName: stuckPod.PodName,
		}
		if err := r.Status().Update(ctx, crd); err != nil {
			return err
		}
		reporter.Info(fmt.Sprintf("Marked pod %s for deletion before snapshot", stuckPod.PodName))
		return nil
	}

	// Check if pod is deleted
	pod := &corev1.Pod{}
	err := r.Get(ctx, client.ObjectKey{Name: stuckPod.PodName, Namespace: recovery.Namespace}, pod)
	if err == nil {
		reporter.Info(fmt.Sprintf("Waiting for pod %s to be deleted", stuckPod.PodName))
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	reporter.Info(fmt.Sprintf("Pod %s deleted, creating snapshot", stuckPod.PodName))

	// Create snapshot
	snapshotName, err := r.snapshotCreator.CreateSnapshot(ctx, recovery, stuckPod.PVCName)
	if err != nil {
		now := metav1.Now()
		stuckPod.Phase = cosmosv1.PodRecoveryPhaseFailed
		stuckPod.Message = fmt.Sprintf("Failed to create snapshot: %v", err)
		stuckPod.LastUpdateTime = &now

		recovery.Status.RecoveryHistory = append(recovery.Status.RecoveryHistory, cosmosv1.RecoveryHistoryEntry{
			Timestamp: now,
			PodName:   stuckPod.PodName,
			Height:    stuckPod.StuckAtHeight,
			EventType: "failed",
			Message:   fmt.Sprintf("Snapshot creation failed: %v", err),
		})

		return r.Status().Update(ctx, recovery)
	}

	now := metav1.Now()
	stuckPod.VolumeSnapshotName = snapshotName
	stuckPod.Phase = cosmosv1.PodRecoveryPhaseWaitingForSnapshot
	stuckPod.Message = fmt.Sprintf("Waiting for snapshot %s", snapshotName)
	stuckPod.LastUpdateTime = &now

	reporter.Info(fmt.Sprintf("Created snapshot %s for pod %s", snapshotName, stuckPod.PodName))
	reporter.RecordInfo("SnapshotCreated", fmt.Sprintf("Created snapshot %s for pod %s", snapshotName, stuckPod.PodName))

	return r.Status().Update(ctx, recovery)
}

func (r *StuckHeightRecoveryReconciler) handlePodWaitingForSnapshot(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	stuckPod *cosmosv1.StuckPodRecoveryStatus,
	reporter kube.EventReporter,
) error {
	ready, err := r.snapshotCreator.CheckSnapshotReady(ctx, recovery.Namespace, stuckPod.VolumeSnapshotName)
	if err != nil {
		if errors.IsNotFound(err) {
			// Snapshot deleted, recreate
			reporter.Info(fmt.Sprintf("Snapshot %s not found, recreating", stuckPod.VolumeSnapshotName))
			now := metav1.Now()
			stuckPod.VolumeSnapshotName = ""
			stuckPod.Phase = cosmosv1.PodRecoveryPhaseCreatingSnapshot
			stuckPod.Message = "Snapshot was deleted, recreating"
			stuckPod.LastUpdateTime = &now
			return r.Status().Update(ctx, recovery)
		}
		return err
	}

	if !ready {
		reporter.Info(fmt.Sprintf("Snapshot %s not ready yet for pod %s", stuckPod.VolumeSnapshotName, stuckPod.PodName))
		return nil
	}

	now := metav1.Now()
	stuckPod.Phase = cosmosv1.PodRecoveryPhaseRunningRecovery
	stuckPod.Message = "Snapshot ready, starting recovery"
	stuckPod.LastUpdateTime = &now

	reporter.Info(fmt.Sprintf("Snapshot %s ready for pod %s", stuckPod.VolumeSnapshotName, stuckPod.PodName))
	reporter.RecordInfo("SnapshotReady", fmt.Sprintf("Snapshot %s ready for pod %s", stuckPod.VolumeSnapshotName, stuckPod.PodName))

	return r.Status().Update(ctx, recovery)
}

func (r *StuckHeightRecoveryReconciler) handlePodRunningRecovery(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	stuckPod *cosmosv1.StuckPodRecoveryStatus,
	reporter kube.EventReporter,
) error {
	// Check if pod is deleted
	pod := &corev1.Pod{}
	err := r.Get(ctx, client.ObjectKey{Name: stuckPod.PodName, Namespace: recovery.Namespace}, pod)
	if err == nil {
		reporter.Info(fmt.Sprintf("Waiting for pod %s to be deleted before recovery", stuckPod.PodName))
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	reporter.Info(fmt.Sprintf("Pod %s deleted, creating recovery pod", stuckPod.PodName))

	// Create recovery pod
	recoveryPodName, err := r.recoveryPodBuilder.CreatePod(
		ctx,
		recovery,
		stuckPod.PodName,
		stuckPod.PVCName,
		stuckPod.StuckAtHeight,
	)
	if err != nil {
		now := metav1.Now()
		stuckPod.Phase = cosmosv1.PodRecoveryPhaseFailed
		stuckPod.Message = fmt.Sprintf("Failed to create recovery pod: %v", err)
		stuckPod.LastUpdateTime = &now

		recovery.Status.RecoveryHistory = append(recovery.Status.RecoveryHistory, cosmosv1.RecoveryHistoryEntry{
			Timestamp: now,
			PodName:   stuckPod.PodName,
			Height:    stuckPod.StuckAtHeight,
			EventType: "failed",
			Message:   fmt.Sprintf("Recovery pod creation failed: %v", err),
		})

		return r.Status().Update(ctx, recovery)
	}

	now := metav1.Now()
	stuckPod.RecoveryPodName = recoveryPodName
	stuckPod.Phase = cosmosv1.PodRecoveryPhaseWaitingForRecovery
	stuckPod.Message = fmt.Sprintf("Waiting for recovery pod %s", recoveryPodName)
	stuckPod.LastUpdateTime = &now

	reporter.Info(fmt.Sprintf("Created recovery pod %s for pod %s", recoveryPodName, stuckPod.PodName))
	reporter.RecordInfo("RecoveryStarted", fmt.Sprintf("Started recovery pod %s for %s", recoveryPodName, stuckPod.PodName))

	return r.Status().Update(ctx, recovery)
}

func (r *StuckHeightRecoveryReconciler) handlePodWaitingForRecovery(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	stuckPod *cosmosv1.StuckPodRecoveryStatus,
	reporter kube.EventReporter,
) error {
	completed, success, err := r.recoveryPodBuilder.CheckPodComplete(ctx, recovery.Namespace, stuckPod.RecoveryPodName)
	if err != nil {
		return err
	}

	if !completed {
		reporter.Info(fmt.Sprintf("Recovery pod %s still running for pod %s", stuckPod.RecoveryPodName, stuckPod.PodName))
		return nil
	}

	now := metav1.Now()
	if success {
		stuckPod.Phase = cosmosv1.PodRecoveryPhaseRecovered
		stuckPod.Message = "Recovery completed successfully"
		stuckPod.LastUpdateTime = &now

		reporter.Info(fmt.Sprintf("Recovery completed successfully for pod %s", stuckPod.PodName))
		reporter.RecordInfo("RecoveryComplete", fmt.Sprintf("Recovery completed for pod %s", stuckPod.PodName))

		recovery.Status.RecoveryHistory = append(recovery.Status.RecoveryHistory, cosmosv1.RecoveryHistoryEntry{
			Timestamp: now,
			PodName:   stuckPod.PodName,
			Height:    stuckPod.StuckAtHeight,
			EventType: "recovered",
			Message:   "Recovery script executed successfully",
		})
	} else {
		stuckPod.Phase = cosmosv1.PodRecoveryPhaseFailed
		stuckPod.Message = "Recovery script failed"
		stuckPod.LastUpdateTime = &now

		reporter.RecordError("RecoveryFailed", fmt.Errorf("recovery failed for pod %s", stuckPod.PodName))

		recovery.Status.RecoveryHistory = append(recovery.Status.RecoveryHistory, cosmosv1.RecoveryHistoryEntry{
			Timestamp: now,
			PodName:   stuckPod.PodName,
			Height:    stuckPod.StuckAtHeight,
			EventType: "failed",
			Message:   "Recovery script failed",
		})
	}

	return r.Status().Update(ctx, recovery)
}

func (r *StuckHeightRecoveryReconciler) cleanupCompletedPods(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	reporter kube.EventReporter,
) error {
	if crd.Status.StuckHeightRecoveryStatus == nil {
		return nil
	}

	// Remove completed pods from StuckPods map and CosmosFullNode status
	// Keep Lagging pods in the map for tracking
	for podName, stuckPod := range recovery.Status.StuckPods {
		if stuckPod.Phase == cosmosv1.PodRecoveryPhaseRecovered ||
			stuckPod.Phase == cosmosv1.PodRecoveryPhaseHeightRecovered ||
			stuckPod.Phase == cosmosv1.PodRecoveryPhaseFailed {

			// Remove from CosmosFullNode status
			recoveryKey := fmt.Sprintf("%s-%s", recovery.Name, podName)
			delete(crd.Status.StuckHeightRecoveryStatus, recoveryKey)

			// Also try old key format for backward compatibility
			delete(crd.Status.StuckHeightRecoveryStatus, recovery.Name)

			reporter.Info(fmt.Sprintf("Cleaned up pod %s (phase: %s)", podName, stuckPod.Phase))

			// Remove from StuckPods map
			delete(recovery.Status.StuckPods, podName)
		}
	}

	// Update CosmosFullNode status if we made changes
	return r.Status().Update(ctx, crd)
}

func (r *StuckHeightRecoveryReconciler) updateOverallStatus(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
) error {
	totalPods := len(recovery.Status.StuckPods)
	if totalPods == 0 {
		return nil
	}

	// Count pods in different states
	lagging := 0
	recovering := 0
	recovered := 0
	failed := 0
	heightRecovered := 0

	for _, stuckPod := range recovery.Status.StuckPods {
		switch stuckPod.Phase {
		case cosmosv1.PodRecoveryPhaseLagging:
			lagging++
		case cosmosv1.PodRecoveryPhaseRecovered:
			recovered++
		case cosmosv1.PodRecoveryPhaseHeightRecovered:
			heightRecovered++
		case cosmosv1.PodRecoveryPhaseFailed:
			failed++
		default:
			recovering++
		}
	}

	// Update message
	if lagging > 0 {
		recovery.Status.Message = fmt.Sprintf(
			"Status: %d lagging, %d recovering, %d recovered, %d auto-recovered, %d failed (total: %d)",
			lagging, recovering, recovered, heightRecovered, failed, totalPods,
		)
	} else {
		recovery.Status.Message = fmt.Sprintf(
			"Recovery status: %d recovering, %d recovered, %d auto-recovered, %d failed (total: %d)",
			recovering, recovered, heightRecovered, failed, totalPods,
		)
	}

	return nil
}

func (r *StuckHeightRecoveryReconciler) handleHeightStuck(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	// Check rate limit
	canAttempt, message, err := r.rateLimiter.CanAttemptRecovery(recovery)
	if err != nil {
		reporter.Error(err, "Rate limit check failed")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	if !canAttempt {
		reporter.RecordInfo("RateLimited", message)
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRateLimited
		recovery.Status.Message = message
		if err := r.Status().Update(ctx, recovery); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	}

	// Record attempt
	r.rateLimiter.RecordAttempt(recovery)

	// Get PVC name before pod is deleted
	pvcName, err := r.snapshotCreator.GetPVCForPod(ctx, recovery.Namespace, recovery.Status.StuckPodName)
	if err != nil {
		reporter.Error(err, "Failed to get PVC name")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRecoveryFailed
		recovery.Status.Message = fmt.Sprintf("Failed to get PVC name: %v", err)
		_ = r.Status().Update(ctx, recovery)
		return ctrl.Result{RequeueAfter: 60 * time.Second}, err
	}
	recovery.Status.StuckPodPVCName = pvcName
	reporter.Info(fmt.Sprintf("Found PVC %s for pod %s", pvcName, recovery.Status.StuckPodName))

	// Check if we need to create a snapshot
	if recovery.Spec.CreateVolumeSnapshot {
		reporter.Info("Creating VolumeSnapshot before recovery")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseCreatingSnapshot
		recovery.Status.Message = "Creating VolumeSnapshot"
	} else {
		reporter.Info("Skipping VolumeSnapshot, proceeding to recovery")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRunningRecovery
		recovery.Status.Message = "Starting recovery script"
	}

	if err := r.Status().Update(ctx, recovery); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{Requeue: true}, nil
}

func (r *StuckHeightRecoveryReconciler) handleCreatingSnapshot(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	// First, mark the stuck pod in CosmosFullNode status to delete it before creating snapshot
	if crd.Status.StuckHeightRecoveryStatus == nil {
		crd.Status.StuckHeightRecoveryStatus = make(map[string]cosmosv1.StuckHeightRecoveryPodStatus)
	}
	if _, exists := crd.Status.StuckHeightRecoveryStatus[recovery.Name]; !exists {
		crd.Status.StuckHeightRecoveryStatus[recovery.Name] = cosmosv1.StuckHeightRecoveryPodStatus{
			PodName: recovery.Status.StuckPodName,
		}
		if err := r.Status().Update(ctx, crd); err != nil {
			reporter.Error(err, "Failed to update CosmosFullNode status")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		}
		reporter.Info(fmt.Sprintf("Marked pod %s for deletion before snapshot", recovery.Status.StuckPodName))
		reporter.RecordInfo("PodMarkedForDeletion", fmt.Sprintf("Pod %s will be deleted before snapshot", recovery.Status.StuckPodName))
		// Wait for pod to be deleted by CosmosFullNode controller
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Check if pod is actually deleted before creating snapshot
	pod := &corev1.Pod{}
	err := r.Get(ctx, client.ObjectKey{
		Name:      recovery.Status.StuckPodName,
		Namespace: recovery.Namespace,
	}, pod)
	if err == nil {
		// Pod still exists, wait for deletion
		reporter.Info(fmt.Sprintf("Waiting for pod %s to be deleted", recovery.Status.StuckPodName))
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if !errors.IsNotFound(err) {
		reporter.Error(err, "Failed to check pod existence")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	reporter.Info(fmt.Sprintf("Pod %s deleted, proceeding with snapshot", recovery.Status.StuckPodName))

	// Use the PVC name saved in status
	pvcName := recovery.Status.StuckPodPVCName
	if pvcName == "" {
		reporter.Error(nil, "PVC name not found in status")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRecoveryFailed
		recovery.Status.Message = "PVC name not found in status"
		_ = r.Status().Update(ctx, recovery)
		return ctrl.Result{RequeueAfter: 60 * time.Second}, fmt.Errorf("PVC name not found in status")
	}

	// Create snapshot
	snapshotName, err := r.snapshotCreator.CreateSnapshot(ctx, recovery, pvcName)
	if err != nil {
		reporter.Error(err, "Failed to create VolumeSnapshot")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRecoveryFailed
		recovery.Status.Message = fmt.Sprintf("Failed to create snapshot: %v", err)
		_ = r.Status().Update(ctx, recovery)
		return ctrl.Result{RequeueAfter: 60 * time.Second}, err
	}

	reporter.Info(fmt.Sprintf("VolumeSnapshot created: %s", snapshotName))
	reporter.RecordInfo("SnapshotCreated", fmt.Sprintf("Created VolumeSnapshot %s", snapshotName))

	recovery.Status.VolumeSnapshotName = snapshotName
	recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseWaitingForSnapshot
	recovery.Status.Message = fmt.Sprintf("Waiting for VolumeSnapshot %s to be ready", snapshotName)

	if err := r.Status().Update(ctx, recovery); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *StuckHeightRecoveryReconciler) handleWaitingForSnapshot(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	ready, err := r.snapshotCreator.CheckSnapshotReady(ctx, recovery.Namespace, recovery.Status.VolumeSnapshotName)
	if err != nil {
		// If VolumeSnapshot was deleted, recreate it by going back to CreatingSnapshot phase
		if errors.IsNotFound(err) {
			reporter.Info(fmt.Sprintf("VolumeSnapshot %s not found, recreating", recovery.Status.VolumeSnapshotName))
			reporter.RecordInfo("SnapshotNotFound", fmt.Sprintf("VolumeSnapshot %s was deleted, recreating", recovery.Status.VolumeSnapshotName))
			recovery.Status.VolumeSnapshotName = ""
			recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseCreatingSnapshot
			recovery.Status.Message = "VolumeSnapshot was deleted, recreating"
			if err := r.Status().Update(ctx, recovery); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}
		reporter.Error(err, "Failed to check VolumeSnapshot status")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	if !ready {
		reporter.Info("VolumeSnapshot not ready yet")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	reporter.Info("VolumeSnapshot is ready")
	reporter.RecordInfo("SnapshotReady", fmt.Sprintf("VolumeSnapshot %s is ready", recovery.Status.VolumeSnapshotName))

	recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRunningRecovery
	recovery.Status.Message = "Starting recovery script"

	if err := r.Status().Update(ctx, recovery); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{Requeue: true}, nil
}

func (r *StuckHeightRecoveryReconciler) handleRunningRecovery(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	// Ensure the stuck pod is deleted before creating recovery pod
	pod := &corev1.Pod{}
	err := r.Get(ctx, client.ObjectKey{
		Name:      recovery.Status.StuckPodName,
		Namespace: recovery.Namespace,
	}, pod)
	if err == nil {
		// Pod still exists, wait for deletion
		reporter.Info(fmt.Sprintf("Waiting for pod %s to be fully deleted before recovery", recovery.Status.StuckPodName))
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if !errors.IsNotFound(err) {
		reporter.Error(err, "Failed to check pod existence")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	reporter.Info(fmt.Sprintf("Pod %s confirmed deleted, creating recovery pod", recovery.Status.StuckPodName))

	// Use the PVC name saved in status
	pvcName := recovery.Status.StuckPodPVCName
	if pvcName == "" {
		reporter.Error(nil, "PVC name not found in status")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRecoveryFailed
		recovery.Status.Message = "PVC name not found in status"
		_ = r.Status().Update(ctx, recovery)
		return ctrl.Result{RequeueAfter: 60 * time.Second}, fmt.Errorf("PVC name not found in status")
	}

	// Create recovery pod
	recoveryPodName, err := r.recoveryPodBuilder.CreatePod(
		ctx,
		recovery,
		recovery.Status.StuckPodName,
		pvcName,
		recovery.Status.LastObservedHeight,
	)
	if err != nil {
		reporter.Error(err, "Failed to create recovery pod")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRecoveryFailed
		recovery.Status.Message = fmt.Sprintf("Failed to create recovery pod: %v", err)
		_ = r.Status().Update(ctx, recovery)
		return ctrl.Result{RequeueAfter: 60 * time.Second}, err
	}

	reporter.Info(fmt.Sprintf("Recovery pod created: %s", recoveryPodName))
	reporter.RecordInfo("RecoveryStarted", fmt.Sprintf("Started recovery pod %s", recoveryPodName))

	recovery.Status.RecoveryPodName = recoveryPodName
	recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseWaitingForRecovery
	recovery.Status.Message = fmt.Sprintf("Waiting for recovery pod %s to complete", recoveryPodName)

	if err := r.Status().Update(ctx, recovery); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *StuckHeightRecoveryReconciler) handleWaitingForRecovery(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	completed, success, err := r.recoveryPodBuilder.CheckPodComplete(ctx, recovery.Namespace, recovery.Status.RecoveryPodName)
	if err != nil {
		reporter.Error(err, "Failed to check recovery pod status")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	if !completed {
		reporter.Info("Recovery pod still running")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if success {
		reporter.Info("Recovery completed successfully")
		reporter.RecordInfo("RecoveryComplete", "Recovery script executed successfully")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRecoveryComplete
		recovery.Status.Message = "Recovery completed successfully"
	} else {
		reporter.RecordError("RecoveryFailed", fmt.Errorf("recovery pod failed"))
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRecoveryFailed
		recovery.Status.Message = "Recovery script failed"
	}

	if err := r.Status().Update(ctx, recovery); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *StuckHeightRecoveryReconciler) handleRecoveryComplete(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	// Remove the stuck pod from CosmosFullNode status to allow it to be recreated
	if crd.Status.StuckHeightRecoveryStatus != nil {
		delete(crd.Status.StuckHeightRecoveryStatus, recovery.Name)
		if err := r.Status().Update(ctx, crd); err != nil {
			reporter.Error(err, "Failed to update CosmosFullNode status")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		}
		reporter.Info(fmt.Sprintf("Removed pod %s from recovery status, it can now be recreated", recovery.Status.StuckPodName))
		reporter.RecordInfo("PodUnmarkedFromRecovery", fmt.Sprintf("Pod %s unmarked from recovery", recovery.Status.StuckPodName))
	}

	// Refetch recovery to get the latest version before updating
	if err := r.Get(ctx, client.ObjectKeyFromObject(recovery), recovery); err != nil {
		reporter.Error(err, "Failed to refetch StuckHeightRecovery")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// Go back to monitoring
	reporter.Info("Returning to monitoring mode")
	recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseMonitoring
	recovery.Status.Message = "Monitoring height"
	recovery.Status.StuckPodName = ""
	recovery.Status.RecoveryPodName = ""
	recovery.Status.VolumeSnapshotName = ""

	if err := r.Status().Update(ctx, recovery); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *StuckHeightRecoveryReconciler) handleRecoveryFailed(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	// Remove the stuck pod from CosmosFullNode status to allow it to be recreated
	if crd.Status.StuckHeightRecoveryStatus != nil {
		delete(crd.Status.StuckHeightRecoveryStatus, recovery.Name)
		if err := r.Status().Update(ctx, crd); err != nil {
			reporter.Error(err, "Failed to update CosmosFullNode status")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		}
		reporter.Info(fmt.Sprintf("Removed pod %s from recovery status after failure", recovery.Status.StuckPodName))
		reporter.RecordInfo("PodUnmarkedAfterFailure", fmt.Sprintf("Pod %s unmarked after recovery failure", recovery.Status.StuckPodName))
	}

	// Refetch recovery to get the latest version before updating
	if err := r.Get(ctx, client.ObjectKeyFromObject(recovery), recovery); err != nil {
		reporter.Error(err, "Failed to refetch StuckHeightRecovery")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// Wait before retrying
	reporter.Info("Recovery failed, will retry monitoring")
	recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseMonitoring
	recovery.Status.Message = "Monitoring height after failure"
	recovery.Status.StuckPodName = ""
	recovery.Status.RecoveryPodName = ""

	if err := r.Status().Update(ctx, recovery); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

func (r *StuckHeightRecoveryReconciler) handleRateLimited(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	// Check if we can retry now
	canAttempt, message, err := r.rateLimiter.CanAttemptRecovery(recovery)
	if err != nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	if canAttempt {
		reporter.Info("Rate limit window expired, resuming monitoring")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseMonitoring
		recovery.Status.Message = "Monitoring height"
		if err := r.Status().Update(ctx, recovery); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Still rate limited
	recovery.Status.Message = message
	if err := r.Status().Update(ctx, recovery); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *StuckHeightRecoveryReconciler) handleDeletion(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	const finalizerName = "stuckheightrecovery.cosmos.bharvest.io/finalizer"

	// Clean up CosmosFullNode status
	crd := &cosmosv1.CosmosFullNode{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      recovery.Spec.FullNodeRef.Name,
		Namespace: recovery.Namespace,
	}, crd); err == nil {
		// Remove from StuckHeightRecoveryStatus
		if crd.Status.StuckHeightRecoveryStatus != nil {
			delete(crd.Status.StuckHeightRecoveryStatus, recovery.Name)
			if err := r.Status().Update(ctx, crd); err != nil {
				reporter.Error(err, "Failed to clean up CosmosFullNode status")
				return ctrl.Result{RequeueAfter: 10 * time.Second}, err
			}
			reporter.Info(fmt.Sprintf("Cleaned up CosmosFullNode status for %s", recovery.Name))
		}
	} else if !errors.IsNotFound(err) {
		reporter.Error(err, "Failed to get CosmosFullNode during cleanup")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// Remove finalizer
	if containsString(recovery.Finalizers, finalizerName) {
		recovery.Finalizers = removeString(recovery.Finalizers, finalizerName)
		if err := r.Update(ctx, recovery); err != nil {
			return ctrl.Result{}, err
		}
		reporter.Info("Finalizer removed, resource will be deleted")
	}

	return ctrl.Result{}, nil
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func removeString(slice []string, s string) []string {
	result := []string{}
	for _, item := range slice {
		if item != s {
			result = append(result, item)
		}
	}
	return result
}

// SetupWithManager sets up the controller with the Manager.
func (r *StuckHeightRecoveryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cosmosv1.StuckHeightRecovery{}).
		Complete(r)
}
