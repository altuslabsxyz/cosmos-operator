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

// CheckStuckHeight checks if the height has been stuck for the specified duration
func (m *HeightMonitor) CheckStuckHeight(
	ctx context.Context,
	crd *cosmosv1.CosmosFullNode,
	recovery *cosmosv1.StuckHeightRecovery,
) (bool, string, uint64, error) {
	// Get current height from CosmosFullNode status
	if crd.Status.Height == nil || len(crd.Status.Height) == 0 {
		return false, "", 0, fmt.Errorf("no height information available")
	}

	// Find a pod with height information
	var currentHeight uint64
	var stuckPodName string

	for podName, height := range crd.Status.Height {
		currentHeight = height
		stuckPodName = podName
		break
	}

	if currentHeight == 0 {
		return false, "", 0, fmt.Errorf("height is zero")
	}

	// Parse stuck duration
	stuckDuration, err := time.ParseDuration(recovery.Spec.StuckDuration)
	if err != nil {
		return false, "", 0, fmt.Errorf("invalid stuck duration: %w", err)
	}

	// Check if this is a new height or the same as last observed
	if recovery.Status.LastObservedHeight != currentHeight {
		// Height changed, not stuck
		return false, "", currentHeight, nil
	}

	// Height hasn't changed, check if stuck duration exceeded
	if recovery.Status.LastHeightUpdateTime == nil {
		// First time observing this height
		return false, "", currentHeight, nil
	}

	timeSinceUpdate := time.Since(recovery.Status.LastHeightUpdateTime.Time)
	if timeSinceUpdate >= stuckDuration {
		return true, stuckPodName, currentHeight, nil
	}

	return false, "", currentHeight, nil
}
