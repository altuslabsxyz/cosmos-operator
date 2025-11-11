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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// FullNodeRef references a CosmosFullNode in the same namespace
type FullNodeRef struct {
	// Name of the CosmosFullNode resource
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// StuckHeightRecoverySpec defines the desired state of StuckHeightRecovery
type StuckHeightRecoverySpec struct {
	// Reference to the CosmosFullNode to monitor
	// +kubebuilder:validation:Required
	FullNodeRef FullNodeRef `json:"fullNodeRef"`

	// Duration to wait before considering height as stuck
	// Examples: "5m", "10m", "1h"
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(s|m|h))+$`
	StuckDuration string `json:"stuckDuration"`

	// Shell script to execute for recovery
	// The script will have access to environment variables:
	// - POD_NAME: Name of the stuck pod
	// - POD_NAMESPACE: Namespace of the stuck pod
	// - CURRENT_HEIGHT: Current height of the stuck pod
	// - PVC_NAME: Name of the PVC
	// - CHAIN_HOME: Home directory of the chain
	// +kubebuilder:validation:Required
	RecoveryScript string `json:"recoveryScript"`

	// Pod template for running the recovery script
	// If not specified, uses a default busybox pod
	// +optional
	PodTemplate *RecoveryPodTemplate `json:"podTemplate,omitempty"`

	// Create a VolumeSnapshot before running recovery script
	// +optional
	CreateVolumeSnapshot bool `json:"createVolumeSnapshot,omitempty"`

	// VolumeSnapshotClass to use if createVolumeSnapshot is true
	// +optional
	VolumeSnapshotClassName string `json:"volumeSnapshotClassName,omitempty"`

	// Maximum number of recovery attempts within the rate limit window
	// Default: 3
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxRetries *int32 `json:"maxRetries,omitempty"`

	// Rate limit window duration (default: 5 minutes)
	// If maxRetries is exceeded within this window, recovery is suspended
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(s|m|h))+$`
	// +optional
	RateLimitWindow string `json:"rateLimitWindow,omitempty"`

	// Suspend recovery operations
	// +optional
	Suspend bool `json:"suspend,omitempty"`
}

// RecoveryPodTemplate defines the pod template for recovery script execution
type RecoveryPodTemplate struct {
	// Container image to use
	// Default: "busybox:latest"
	// +optional
	Image string `json:"image,omitempty"`

	// Image pull policy
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Image pull secrets
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// Resources for the recovery pod
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// Additional environment variables
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// Additional volume mounts
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`

	// Additional volumes
	// +optional
	Volumes []corev1.Volume `json:"volumes,omitempty"`
}

// StuckHeightRecoveryStatus defines the observed state of StuckHeightRecovery
type StuckHeightRecoveryStatus struct {
	// Current phase of the recovery process
	// +optional
	Phase StuckHeightRecoveryPhase `json:"phase,omitempty"`

	// Last time recovery was attempted
	// +optional
	LastRecoveryTime *metav1.Time `json:"lastRecoveryTime,omitempty"`

	// Number of recovery attempts within the current rate limit window
	// +optional
	RecoveryAttempts int32 `json:"recoveryAttempts,omitempty"`

	// Start time of the current rate limit window
	// +optional
	RateLimitWindowStart *metav1.Time `json:"rateLimitWindowStart,omitempty"`

	// Last observed height
	// +optional
	LastObservedHeight uint64 `json:"lastObservedHeight,omitempty"`

	// Time when the height was last updated
	// +optional
	LastHeightUpdateTime *metav1.Time `json:"lastHeightUpdateTime,omitempty"`

	// Name of the stuck pod being recovered
	// +optional
	StuckPodName string `json:"stuckPodName,omitempty"`

	// Name of the created VolumeSnapshot (if any)
	// +optional
	VolumeSnapshotName string `json:"volumeSnapshotName,omitempty"`

	// Name of the recovery pod
	// +optional
	RecoveryPodName string `json:"recoveryPodName,omitempty"`

	// Human-readable status message
	// +optional
	Message string `json:"message,omitempty"`

	// ObservedGeneration is the most recent generation observed
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// StuckHeightRecoveryPhase represents the current phase of recovery
// +kubebuilder:validation:Enum=Monitoring;HeightStuck;CreatingSnapshot;WaitingForSnapshot;RunningRecovery;WaitingForRecovery;RecoveryComplete;RecoveryFailed;RateLimited;Suspended
type StuckHeightRecoveryPhase string

const (
	// Monitoring normal operation, watching for stuck height
	StuckHeightRecoveryPhaseMonitoring StuckHeightRecoveryPhase = "Monitoring"

	// Height has been detected as stuck
	StuckHeightRecoveryPhaseHeightStuck StuckHeightRecoveryPhase = "HeightStuck"

	// Creating VolumeSnapshot before recovery
	StuckHeightRecoveryPhaseCreatingSnapshot StuckHeightRecoveryPhase = "CreatingSnapshot"

	// Waiting for VolumeSnapshot to be ready
	StuckHeightRecoveryPhaseWaitingForSnapshot StuckHeightRecoveryPhase = "WaitingForSnapshot"

	// Running recovery script
	StuckHeightRecoveryPhaseRunningRecovery StuckHeightRecoveryPhase = "RunningRecovery"

	// Waiting for recovery pod to complete
	StuckHeightRecoveryPhaseWaitingForRecovery StuckHeightRecoveryPhase = "WaitingForRecovery"

	// Recovery completed successfully
	StuckHeightRecoveryPhaseRecoveryComplete StuckHeightRecoveryPhase = "RecoveryComplete"

	// Recovery failed
	StuckHeightRecoveryPhaseRecoveryFailed StuckHeightRecoveryPhase = "RecoveryFailed"

	// Rate limit exceeded, waiting for window to reset
	StuckHeightRecoveryPhaseRateLimited StuckHeightRecoveryPhase = "RateLimited"

	// Recovery is suspended
	StuckHeightRecoveryPhaseSuspended StuckHeightRecoveryPhase = "Suspended"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=stuckheightrecoveries,scope=Namespaced,shortName=shr
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Stuck Pod",type=string,JSONPath=`.status.stuckPodName`
// +kubebuilder:printcolumn:name="Last Recovery",type=date,JSONPath=`.status.lastRecoveryTime`
// +kubebuilder:printcolumn:name="Attempts",type=integer,JSONPath=`.status.recoveryAttempts`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// StuckHeightRecovery is the Schema for the stuckheightrecoveries API
type StuckHeightRecovery struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StuckHeightRecoverySpec   `json:"spec,omitempty"`
	Status StuckHeightRecoveryStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// StuckHeightRecoveryList contains a list of StuckHeightRecovery
type StuckHeightRecoveryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []StuckHeightRecovery `json:"items"`
}

func init() {
	SchemeBuilder.Register(&StuckHeightRecovery{}, &StuckHeightRecoveryList{})
}
