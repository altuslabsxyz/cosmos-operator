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
	"fmt"
	"time"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// HeightMonitor monitors blockchain height and detects stuck height
type HeightMonitor struct {
	client client.Reader
}

// NewHeightMonitor creates a new HeightMonitor
func NewHeightMonitor(client client.Reader) *HeightMonitor {
	return &HeightMonitor{
		client: client,
	}
}

// PodHeightInfo contains height information for a pod
type PodHeightInfo struct {
	PodName string
	Height  uint64
}

// HeightCheckResult contains the result of height monitoring
type HeightCheckResult struct {
	// Maximum height observed across all pods
	MaxHeight uint64
	// Pods that are lagging (>100 blocks behind) but not yet confirmed stuck
	LaggingPods map[string]uint64
	// Pods that are newly detected as stuck
	NewlyStuckPods map[string]uint64
	// Pods that have recovered (height started moving again)
	RecoveredPods map[string]uint64
	// All pods with their current heights
	AllPodHeights map[string]uint64
}

// CheckStuckHeight checks all pods for stuck height and recovery
func (m *HeightMonitor) CheckStuckHeight(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	recovery *cosmosv1.StuckHeightRecovery,
) (*HeightCheckResult, error) {
	// Get current height from CosmosFullNode status
	if crd.Status.Height == nil || len(crd.Status.Height) == 0 {
		return nil, fmt.Errorf("no height information available")
	}

	// Parse stuck duration
	stuckDuration, err := time.ParseDuration(recovery.Spec.StuckDuration)
	if err != nil {
		return nil, fmt.Errorf("invalid stuck duration: %w", err)
	}

	result := &HeightCheckResult{
		LaggingPods:    make(map[string]uint64),
		NewlyStuckPods: make(map[string]uint64),
		RecoveredPods:  make(map[string]uint64),
		AllPodHeights:  make(map[string]uint64),
	}

	// Find maximum height across all pods
	var maxHeight uint64
	for podName, height := range crd.Status.Height {
		result.AllPodHeights[podName] = height
		if height > maxHeight {
			maxHeight = height
		}
	}
	result.MaxHeight = maxHeight

	if maxHeight == 0 {
		return nil, fmt.Errorf("all pod heights are zero")
	}

	// Check each pod for stuck or recovered state
	for podName, currentHeight := range crd.Status.Height {
		existingStuckPod, wasStuck := recovery.Status.StuckPods[podName]

		if wasStuck {
			// Pod is being tracked - check its phase
			if existingStuckPod.Phase == cosmosv1.PodRecoveryPhaseLagging {
				// Pod is still in lagging phase - check if it became stuck
				if m.isPodStuck(podName, currentHeight, maxHeight, recovery, stuckDuration, existingStuckPod) {
					result.NewlyStuckPods[podName] = currentHeight
				} else {
					// Still lagging
					result.LaggingPods[podName] = currentHeight
				}
			} else {
				// Pod was in recovery phase - check if it has recovered
				if m.hasHeightRecovered(existingStuckPod, currentHeight, maxHeight) {
					result.RecoveredPods[podName] = currentHeight
				}
			}
		} else {
			// Pod was not being tracked - check if it's lagging or became stuck
			heightDiff := maxHeight - currentHeight
			if heightDiff >= 100 {
				// Pod is lagging - start tracking
				result.LaggingPods[podName] = currentHeight
			}
		}
	}

	return result, nil
}

// hasHeightRecovered checks if a previously stuck pod has recovered
func (m *HeightMonitor) hasHeightRecovered(
	stuckPod *cosmosv1.StuckPodRecoveryStatus,
	currentHeight uint64,
	maxHeight uint64,
) bool {
	// Pod has recovered if:
	// 1. Height has increased from when it was stuck
	// 2. Height is close to max height (within reasonable range)

	heightIncreased := currentHeight > stuckPod.StuckAtHeight
	closeToMax := maxHeight-currentHeight < 100 // Within 100 blocks is considered recovered

	return heightIncreased && closeToMax
}

// isPodStuck checks if a pod's height is stuck
func (m *HeightMonitor) isPodStuck(
	podName string,
	currentHeight uint64,
	maxHeight uint64,
	recovery *cosmosv1.StuckHeightRecovery,
	stuckDuration time.Duration,
	existingTracking *cosmosv1.StuckPodRecoveryStatus,
) bool {
	// A pod is considered stuck if:
	// 1. Its height is significantly below the max height (>100 blocks)
	// 2. Its height hasn't changed for the stuck duration

	// Height difference threshold - pod is potentially stuck if more than 100 blocks behind
	heightDiff := maxHeight - currentHeight
	if heightDiff < 100 {
		return false // Not significantly behind
	}

	// If this pod is not being tracked yet, we can't determine if it's stuck
	// We need to track it first to see if height changes over time
	if existingTracking == nil {
		return false // First observation of this lagging pod
	}

	// Check if height has changed since we started tracking this pod
	if existingTracking.CurrentHeight != nil && *existingTracking.CurrentHeight != currentHeight {
		return false // Height has changed, not stuck
	}

	// Check if stuck duration has been exceeded since detection
	if existingTracking.DetectedAt.IsZero() {
		return false // No detection time recorded
	}

	timeSinceDetection := time.Since(existingTracking.DetectedAt.Time)
	return timeSinceDetection >= stuckDuration
}

// Deprecated: Use CheckStuckHeight instead
// CheckStuckHeightSingle checks if the height has been stuck (single pod mode for backward compatibility)
func (m *HeightMonitor) CheckStuckHeightSingle(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	recovery *cosmosv1.StuckHeightRecovery,
) (bool, string, uint64, error) {
	result, err := m.CheckStuckHeight(ctx, crd, recovery)
	if err != nil {
		return false, "", 0, err
	}

	// Return first stuck pod if any
	for podName, height := range result.NewlyStuckPods {
		return true, podName, height, nil
	}

	return false, "", result.MaxHeight, nil
}
