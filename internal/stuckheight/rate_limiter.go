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
	"fmt"
	"time"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	DefaultMaxRetries       = 3
	DefaultRateLimitWindow  = "5m"
)

// RateLimiter manages retry attempts and rate limiting
type RateLimiter struct{}

// NewRateLimiter creates a new RateLimiter
func NewRateLimiter() *RateLimiter {
	return &RateLimiter{}
}

// CanAttemptRecovery checks if recovery can be attempted based on rate limits
func (r *RateLimiter) CanAttemptRecovery(recovery *cosmosv1.StuckHeightRecovery) (bool, string, error) {
	maxRetries := DefaultMaxRetries
	if recovery.Spec.MaxRetries != nil {
		maxRetries = int(*recovery.Spec.MaxRetries)
	}

	rateLimitWindow := DefaultRateLimitWindow
	if recovery.Spec.RateLimitWindow != "" {
		rateLimitWindow = recovery.Spec.RateLimitWindow
	}

	windowDuration, err := time.ParseDuration(rateLimitWindow)
	if err != nil {
		return false, "", fmt.Errorf("invalid rate limit window: %w", err)
	}

	now := time.Now()

	// Check if we need to reset the rate limit window
	if recovery.Status.RateLimitWindowStart == nil {
		// First attempt, start a new window
		return true, "", nil
	}

	windowStart := recovery.Status.RateLimitWindowStart.Time
	timeSinceWindowStart := now.Sub(windowStart)

	// If we're outside the window, reset
	if timeSinceWindowStart >= windowDuration {
		return true, "", nil
	}

	// We're within the window, check if we've exceeded max retries
	if recovery.Status.RecoveryAttempts >= int32(maxRetries) {
		remainingTime := windowDuration - timeSinceWindowStart
		message := fmt.Sprintf(
			"Rate limit exceeded: %d attempts in %s. Will retry after %s",
			recovery.Status.RecoveryAttempts,
			windowDuration,
			remainingTime.Round(time.Second),
		)
		return false, message, nil
	}

	return true, "", nil
}

// RecordAttempt records a recovery attempt
func (r *RateLimiter) RecordAttempt(recovery *cosmosv1.StuckHeightRecovery) {
	now := metav1.Now()

	// Check if we need to start a new window
	if recovery.Status.RateLimitWindowStart == nil {
		recovery.Status.RateLimitWindowStart = &now
		recovery.Status.RecoveryAttempts = 1
		recovery.Status.LastRecoveryTime = &now
		return
	}

	rateLimitWindow := DefaultRateLimitWindow
	if recovery.Spec.RateLimitWindow != "" {
		rateLimitWindow = recovery.Spec.RateLimitWindow
	}

	windowDuration, _ := time.ParseDuration(rateLimitWindow)
	timeSinceWindowStart := time.Since(recovery.Status.RateLimitWindowStart.Time)

	if timeSinceWindowStart >= windowDuration {
		// Start new window
		recovery.Status.RateLimitWindowStart = &now
		recovery.Status.RecoveryAttempts = 1
	} else {
		// Increment attempts in current window
		recovery.Status.RecoveryAttempts++
	}

	recovery.Status.LastRecoveryTime = &now
}

// ResetWindow resets the rate limit window (called when height changes)
func (r *RateLimiter) ResetWindow(recovery *cosmosv1.StuckHeightRecovery) {
	recovery.Status.RecoveryAttempts = 0
	recovery.Status.RateLimitWindowStart = nil
}
