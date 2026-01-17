/*
Copyright 2022 Strangelove Ventures LLC.

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
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	"github.com/b-harvest/cosmos-operator/internal/cosmos"
	"github.com/b-harvest/cosmos-operator/internal/fullnode"
	"github.com/b-harvest/cosmos-operator/internal/healthcheck"
	"github.com/b-harvest/cosmos-operator/internal/kube"
)

// SelfHealingReconciler reconciles the self healing portion of a CosmosFullNode object
type SelfHealingReconciler struct {
	client.Client
	cacheController     *cosmos.CacheController
	diskClient          *fullnode.DiskUsageCollector
	driftDetector       fullnode.DriftDetection
	pvcAutoScaler       *fullnode.PVCAutoScaler
	deepRecoveryManager *fullnode.DeepRecoveryManager
	recorder            record.EventRecorder
}

func NewSelfHealing(
	client client.Client,
	recorder record.EventRecorder,
	statusClient *fullnode.StatusClient,
	httpClient *http.Client,
	cacheController *cosmos.CacheController,
) *SelfHealingReconciler {
	return &SelfHealingReconciler{
		Client:              client,
		cacheController:     cacheController,
		diskClient:          fullnode.NewDiskUsageCollector(healthcheck.NewClient(httpClient), client),
		driftDetector:       fullnode.NewDriftDetection(cacheController),
		pvcAutoScaler:       fullnode.NewPVCAutoScaler(statusClient),
		deepRecoveryManager: fullnode.NewDeepRecoveryManager(client),
		recorder:            recorder,
	}
}

// Reconcile reconciles only the self-healing spec in CosmosFullNode. If changes needed, this controller
// updates a CosmosFullNode status subresource thus triggering another reconcile loop. The CosmosFullNode
// uses the status object to reconcile its state.
func (r *SelfHealingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithName(cosmosv1.SelfHealingController)
	logger.V(1).Info("Entering reconcile loop", "request", req.NamespacedName)

	crd := new(cosmosv1.CosmosFullNode)
	if err := r.Get(ctx, req.NamespacedName, crd); err != nil {
		// Ignore not found errors because can't be fixed by an immediate requeue. We'll have to wait for next notification.
		// Also, will get "not found" error if crd is deleted.
		// No need to explicitly delete resources. Kube GC does so automatically because we set the controller reference
		// for each resource.
		return stopResult, client.IgnoreNotFound(err)
	}

	if crd.Spec.SelfHeal == nil {
		return stopResult, nil
	}

	reporter := kube.NewEventReporter(logger, r.recorder, crd)

	r.pvcAutoScale(ctx, reporter, crd)
	r.mitigateHeightDrift(ctx, reporter, crd)

	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

func (r *SelfHealingReconciler) pvcAutoScale(ctx context.Context, reporter kube.Reporter, crd *cosmosv1.CosmosFullNode) {
	if crd.Spec.SelfHeal.PVCAutoScale == nil {
		return
	}
	usage, err := r.diskClient.CollectDiskUsage(ctx, crd)
	if err != nil {
		reporter.Error(err, "Failed to collect pvc disk usage")
		// This error can be noisy so we record a generic error. Check logs for error details.
		reporter.RecordError("PVCAutoScaleCollectUsage", errors.New("failed to collect pvc disk usage"))
		return
	}
	didSignal, err := r.pvcAutoScaler.SignalPVCResize(ctx, crd, usage)
	if err != nil {
		reporter.Error(err, "Failed to signal pvc resize")
		reporter.RecordError("PVCAutoScaleSignalResize", err)
		return
	}
	if !didSignal {
		return
	}
	const msg = "PVC auto scaling requested disk expansion"
	reporter.Info(msg)
	reporter.RecordInfo("PVCAutoScale", msg)
}

func (r *SelfHealingReconciler) mitigateHeightDrift(ctx context.Context, reporter kube.Reporter, crd *cosmosv1.CosmosFullNode) {
	if crd.Spec.SelfHeal.HeightDriftMitigation == nil {
		return
	}

	pods := r.driftDetector.LaggingPods(ctx, crd)
	var deleted int
	for _, pod := range pods {
		// CosmosFullNodeController will detect missing pod and re-create it.
		if err := r.Delete(ctx, pod); kube.IgnoreNotFound(err) != nil {
			reporter.Error(err, "Failed to delete pod", "pod", pod.Name)
			reporter.RecordError("HeightDriftMitigationDeletePod", err)
			continue
		}
		reporter.Info("Deleted pod for meeting height drift threshold", "pod", pod.Name)
		deleted++
	}
	if deleted > 0 {
		msg := fmt.Sprintf("Height lagged behind by %d or more blocks; deleted pod(s)", crd.Spec.SelfHeal.HeightDriftMitigation.Threshold)
		reporter.RecordInfo("HeightDriftMitigation", msg)
	}

	// Handle deep recovery if enabled
	r.handleDeepRecovery(ctx, reporter, crd)
}

func (r *SelfHealingReconciler) handleDeepRecovery(ctx context.Context, reporter kube.Reporter, crd *cosmosv1.CosmosFullNode) {
	spec := crd.Spec.SelfHeal.HeightDriftMitigation.DeepRecovery
	if spec == nil || spec.Suspend {
		return
	}

	// Cleanup DeepRecovery status for pods that no longer exist (e.g., replicas reduced)
	statusChanged := r.cleanupStaleDeepRecoveryStatus(crd)
	if statusChanged {
		if err := r.Status().Update(ctx, crd); err != nil {
			reporter.Error(err, "Failed to cleanup stale deep recovery status")
		}
	}

	// Find pods that might be stuck (height not changing)
	stuckPods := r.detectStuckPods(ctx, crd)
	if len(stuckPods) == 0 {
		// Cleanup recovered pods if any
		if err := r.deepRecoveryManager.CleanupRecoveredPods(ctx, crd); err != nil {
			reporter.Error(err, "Failed to cleanup recovered pods")
		}
		return
	}

	actionTaken, err := r.deepRecoveryManager.CheckAndTriggerRecovery(ctx, crd, stuckPods)
	if err != nil {
		reporter.Error(err, "Failed to process deep recovery")
		reporter.RecordError("DeepRecoveryError", err)
		return
	}

	if actionTaken {
		// Update status
		if err := r.Status().Update(ctx, crd); err != nil {
			reporter.Error(err, "Failed to update deep recovery status")
			return
		}
		reporter.Info("Deep recovery action taken")
		reporter.RecordInfo("DeepRecovery", "Processing stuck pod recovery")
	}
}

func (r *SelfHealingReconciler) detectStuckPods(ctx context.Context, crd *cosmosv1.CosmosFullNode) []fullnode.PodStuckInfo {
	if len(crd.Status.Height) == 0 {
		return nil
	}

	// Find max height
	var maxHeight uint64
	for _, height := range crd.Status.Height {
		if height > maxHeight {
			maxHeight = height
		}
	}

	if maxHeight == 0 {
		return nil
	}

	threshold := crd.Spec.SelfHeal.HeightDriftMitigation.Threshold
	var stuckPods []fullnode.PodStuckInfo

	// Get existing deep recovery status for tracking
	var existingStatus map[string]*cosmosv1.DeepRecoveryStatus
	if crd.Status.SelfHealing.DeepRecovery != nil {
		existingStatus = crd.Status.SelfHealing.DeepRecovery
	}

	for podName, height := range crd.Status.Height {
		// Skip pods that are outside the current replica range
		// This handles the case where replicas were reduced but height status still has old entries
		ordinal := extractOrdinalFromPodName(podName)
		if ordinal < 0 || ordinal >= int(crd.Spec.Replicas) {
			continue
		}

		// Check if significantly lagging
		if maxHeight-height < uint64(threshold) {
			continue
		}

		// Check if already being tracked and height hasn't changed
		if existingStatus != nil {
			if status, exists := existingStatus[podName]; exists {
				// If height changed, this pod is not stuck
				if status.StuckHeight > 0 && height != status.StuckHeight {
					continue
				}
			}
		}

		// Find PVC name for this pod
		pvcName := r.getPVCNameForPod(ctx, crd, podName)

		stuckPods = append(stuckPods, fullnode.PodStuckInfo{
			PodName:       podName,
			CurrentHeight: height,
			MaxHeight:     maxHeight,
			PVCName:       pvcName,
		})
	}

	return stuckPods
}

func (r *SelfHealingReconciler) getPVCNameForPod(ctx context.Context, crd *cosmosv1.CosmosFullNode, podName string) string {
	// Try to get PVC name from pod volumes first
	pod := &corev1.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Name: podName, Namespace: crd.Namespace}, pod); err == nil {
		// Find PVC from pod volumes
		for _, vol := range pod.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil {
				return vol.PersistentVolumeClaim.ClaimName
			}
		}
	}

	// Fallback: derive PVC name from pod name using naming convention
	// Pod name format: {crd.Name}-{ordinal}
	// PVC name format: pvc-{crd.Name}-{ordinal}
	ordinal := extractOrdinalFromPodName(podName)
	if ordinal >= 0 {
		return fmt.Sprintf("pvc-%s-%d", crd.Name, ordinal)
	}

	return ""
}

// extractOrdinalFromPodName extracts the ordinal number from a pod name
// Pod names follow the pattern: {name}-{ordinal}, e.g., "stable-full-988-1-0" returns 0
func extractOrdinalFromPodName(podName string) int {
	// Find the last hyphen and extract the number after it
	lastHyphen := strings.LastIndex(podName, "-")
	if lastHyphen == -1 || lastHyphen == len(podName)-1 {
		return -1
	}

	ordinalStr := podName[lastHyphen+1:]
	ordinal, err := strconv.Atoi(ordinalStr)
	if err != nil {
		return -1
	}

	return ordinal
}

// cleanupStaleDeepRecoveryStatus removes DeepRecovery status entries for pods
// that no longer exist (e.g., when replicas are reduced).
// Returns true if any entries were removed.
func (r *SelfHealingReconciler) cleanupStaleDeepRecoveryStatus(crd *cosmosv1.CosmosFullNode) bool {
	if crd.Status.SelfHealing.DeepRecovery == nil {
		return false
	}

	changed := false
	for podName := range crd.Status.SelfHealing.DeepRecovery {
		ordinal := extractOrdinalFromPodName(podName)
		if ordinal < 0 || ordinal >= int(crd.Spec.Replicas) {
			delete(crd.Status.SelfHealing.DeepRecovery, podName)
			changed = true
		}
	}

	return changed
}

// SetupWithManager sets up the controller with the Manager.
func (r *SelfHealingReconciler) SetupWithManager(_ context.Context, mgr ctrl.Manager) error {
	// We do not have to index Pods because the CosmosFullNodeReconciler already does so.
	// If we repeat it here, the manager returns an error.
	return ctrl.NewControllerManagedBy(mgr).
		For(&cosmosv1.CosmosFullNode{}).
		Complete(r)
}
