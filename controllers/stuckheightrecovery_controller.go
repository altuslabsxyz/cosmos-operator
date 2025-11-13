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

	// Handle different phases
	switch recovery.Status.Phase {
	case "", cosmosv1.StuckHeightRecoveryPhaseMonitoring:
		return r.handleMonitoring(ctx, recovery, crd, reporter)

	case cosmosv1.StuckHeightRecoveryPhaseHeightStuck:
		return r.handleHeightStuck(ctx, recovery, crd, reporter)

	case cosmosv1.StuckHeightRecoveryPhaseCreatingSnapshot:
		return r.handleCreatingSnapshot(ctx, recovery, reporter)

	case cosmosv1.StuckHeightRecoveryPhaseWaitingForSnapshot:
		return r.handleWaitingForSnapshot(ctx, recovery, reporter)

	case cosmosv1.StuckHeightRecoveryPhaseRunningRecovery:
		return r.handleRunningRecovery(ctx, recovery, crd, reporter)

	case cosmosv1.StuckHeightRecoveryPhaseWaitingForRecovery:
		return r.handleWaitingForRecovery(ctx, recovery, reporter)

	case cosmosv1.StuckHeightRecoveryPhaseRecoveryComplete:
		return r.handleRecoveryComplete(ctx, recovery, reporter)

	case cosmosv1.StuckHeightRecoveryPhaseRecoveryFailed:
		return r.handleRecoveryFailed(ctx, recovery, reporter)

	case cosmosv1.StuckHeightRecoveryPhaseRateLimited:
		return r.handleRateLimited(ctx, recovery, crd, reporter)
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *StuckHeightRecoveryReconciler) handleMonitoring(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	crd *cosmosv1.CosmosFullNode,
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	// Check if height is stuck
	isStuck, stuckPodName, currentHeight, err := r.heightMonitor.CheckStuckHeight(ctx, crd, recovery)
	if err != nil {
		reporter.Error(err, "Failed to check stuck height")
		recovery.Status.Message = fmt.Sprintf("Error checking height: %v", err)
		_ = r.Status().Update(ctx, recovery)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	// Update last observed height
	now := metav1.Now()
	if recovery.Status.LastObservedHeight != currentHeight {
		recovery.Status.LastObservedHeight = currentHeight
		recovery.Status.LastHeightUpdateTime = &now
		recovery.Status.Message = fmt.Sprintf("Monitoring height: %d", currentHeight)

		// Reset rate limiter when height changes
		r.rateLimiter.ResetWindow(recovery)

		if err := r.Status().Update(ctx, recovery); err != nil {
			return ctrl.Result{}, err
		}
	}

	if isStuck {
		reporter.Info(fmt.Sprintf("Height stuck detected on pod %s at height %d", stuckPodName, currentHeight))
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseHeightStuck
		recovery.Status.StuckPodName = stuckPodName
		recovery.Status.Message = fmt.Sprintf("Height stuck at %d for %s", currentHeight, recovery.Spec.StuckDuration)
		if err := r.Status().Update(ctx, recovery); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseMonitoring
	if err := r.Status().Update(ctx, recovery); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
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
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	// Get PVC for the stuck pod
	pvcName, err := r.snapshotCreator.GetPVCForPod(ctx, recovery.Namespace, recovery.Status.StuckPodName)
	if err != nil {
		reporter.Error(err, "Failed to get PVC")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRecoveryFailed
		recovery.Status.Message = fmt.Sprintf("Failed to get PVC: %v", err)
		_ = r.Status().Update(ctx, recovery)
		return ctrl.Result{RequeueAfter: 60 * time.Second}, err
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
	reporter kube.EventReporter,
) (ctrl.Result, error) {
	ready, err := r.snapshotCreator.CheckSnapshotReady(ctx, recovery.Namespace, recovery.Status.VolumeSnapshotName)
	if err != nil {
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
	// Get PVC for the stuck pod
	pvcName, err := r.snapshotCreator.GetPVCForPod(ctx, recovery.Namespace, recovery.Status.StuckPodName)
	if err != nil {
		reporter.Error(err, "Failed to get PVC")
		recovery.Status.Phase = cosmosv1.StuckHeightRecoveryPhaseRecoveryFailed
		recovery.Status.Message = fmt.Sprintf("Failed to get PVC: %v", err)
		_ = r.Status().Update(ctx, recovery)
		return ctrl.Result{RequeueAfter: 60 * time.Second}, err
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
	reporter kube.EventReporter,
) (ctrl.Result, error) {
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
	reporter kube.EventReporter,
) (ctrl.Result, error) {
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

// SetupWithManager sets up the controller with the Manager.
func (r *StuckHeightRecoveryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cosmosv1.StuckHeightRecovery{}).
		Complete(r)
}
