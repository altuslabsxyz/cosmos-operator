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

package stuckheight

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	"github.com/b-harvest/cosmos-operator/internal/kube"
	"github.com/go-logr/logr"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Label used to identify snapshots created by StuckHeightRecovery
const recoverySnapshotLabel = "cosmos.bharvest.io/recovery"

// DefaultSnapshotLimit is the default number of snapshots to keep
const DefaultSnapshotLimit = 3

// SnapshotCreator creates VolumeSnapshots
type SnapshotCreator struct {
	client client.Client
}

// NewSnapshotCreator creates a new SnapshotCreator
func NewSnapshotCreator(client client.Client) *SnapshotCreator {
	return &SnapshotCreator{
		client: client,
	}
}

// CreateSnapshot creates a VolumeSnapshot for the stuck pod's PVC
func (s *SnapshotCreator) CreateSnapshot(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	pvcName string,
) (string, error) {
	// Use human-readable timestamp format: 2025-01-15t10-30-45 (lowercase for RFC 1123)
	timestamp := time.Now().Format("2006-01-02t15-04-05")
	snapshotName := fmt.Sprintf("%s-recovery-%s", pvcName, timestamp)

	snapshot := &snapshotv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotName,
			Namespace: recovery.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "cosmos-operator",
				"cosmos.bharvest.io/recovery":  recovery.Name,
			},
		},
		Spec: snapshotv1.VolumeSnapshotSpec{
			Source: snapshotv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvcName,
			},
		},
	}

	// Set VolumeSnapshotClassName if specified
	if recovery.Spec.VolumeSnapshotClassName != "" {
		snapshot.Spec.VolumeSnapshotClassName = &recovery.Spec.VolumeSnapshotClassName
	}

	// Set owner reference
	if err := controllerutil.SetControllerReference(recovery, snapshot, s.client.Scheme()); err != nil {
		return "", fmt.Errorf("set controller reference: %w", err)
	}

	if err := s.client.Create(ctx, snapshot); err != nil {
		return "", fmt.Errorf("create volume snapshot: %w", err)
	}

	return snapshotName, nil
}

// CheckSnapshotReady checks if the VolumeSnapshot is ready to use
func (s *SnapshotCreator) CheckSnapshotReady(
	ctx context.Context,
	namespace string,
	snapshotName string,
) (bool, error) {
	snapshot := &snapshotv1.VolumeSnapshot{}
	if err := s.client.Get(ctx, client.ObjectKey{
		Name:      snapshotName,
		Namespace: namespace,
	}, snapshot); err != nil {
		return false, fmt.Errorf("get volume snapshot: %w", err)
	}

	if snapshot.Status == nil {
		return false, nil
	}

	return snapshot.Status.ReadyToUse != nil && *snapshot.Status.ReadyToUse, nil
}

// GetPVCForPod gets the PVC name for a given pod
func (s *SnapshotCreator) GetPVCForPod(
	ctx context.Context,
	namespace string,
	podName string,
) (string, error) {
	pod := &corev1.Pod{}
	if err := s.client.Get(ctx, client.ObjectKey{
		Name:      podName,
		Namespace: namespace,
	}, pod); err != nil {
		return "", fmt.Errorf("get pod: %w", err)
	}

	// Find the main chain-home volume
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.Name == "vol-chain-home" {
			return volume.PersistentVolumeClaim.ClaimName, nil
		}
	}

	return "", fmt.Errorf("no PVC found for pod %s", podName)
}

// DeleteOldSnapshots deletes old VolumeSnapshots keeping only the most recent ones up to the limit.
// Snapshots are identified by the recovery label (cosmos.bharvest.io/recovery).
// If limit is <= 0, defaults to DefaultSnapshotLimit (3).
func (s *SnapshotCreator) DeleteOldSnapshots(
	ctx context.Context,
	log logr.Logger,
	namespace string,
	recoveryName string,
	limit int,
) error {
	if limit <= 0 {
		limit = DefaultSnapshotLimit
	}

	// List all snapshots with the recovery label
	var snapshots snapshotv1.VolumeSnapshotList
	if err := s.client.List(ctx,
		&snapshots,
		client.InNamespace(namespace),
		client.MatchingLabels(map[string]string{recoverySnapshotLabel: recoveryName}),
	); err != nil {
		return fmt.Errorf("list volume snapshots: %w", err)
	}

	// Filter snapshots that have a valid CreationTime
	var filtered []snapshotv1.VolumeSnapshot
	for _, snapshot := range snapshots.Items {
		if snapshot.Status != nil && snapshot.Status.CreationTime != nil {
			filtered = append(filtered, snapshot)
		}
	}

	// Nothing to delete if we're within the limit
	if len(filtered) <= limit {
		return nil
	}

	// Sort by creation time descending (newest first)
	sort.Slice(filtered, func(i, j int) bool {
		lhs := filtered[i].Status.CreationTime
		rhs := filtered[j].Status.CreationTime
		return !lhs.Before(rhs)
	})

	// Delete snapshots beyond the limit
	toDelete := filtered[limit:]
	var merr error
	for _, vs := range toDelete {
		vs := vs
		log.Info("Deleting old recovery snapshot",
			"volumeSnapshotName", vs.Name,
			"recoveryName", recoveryName,
			"limit", limit,
		)
		if err := s.client.Delete(ctx, &vs); kube.IgnoreNotFound(err) != nil {
			merr = errors.Join(merr, fmt.Errorf("delete %s: %w", vs.Name, err))
		}
	}

	return merr
}
