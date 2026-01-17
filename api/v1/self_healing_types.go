package v1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SelfHealingController is the canonical controller name.
const SelfHealingController = "SelfHealing"

// SelfHealSpec is part of a CosmosFullNode but is managed by a separate controller, SelfHealingReconciler.
// This is an effort to reduce complexity in the CosmosFullNodeReconciler.
// The controller only modifies the CosmosFullNode's status subresource relying on the CosmosFullNodeReconciler
// to reconcile appropriately.
type SelfHealSpec struct {
	// Automatically increases PVC storage as they approach capacity.
	//
	// Your cluster must support and use the ExpandInUsePersistentVolumes feature gate. This allows volumes to
	// expand while a pod is attached to it, thus eliminating the need to restart pods.
	// If you cluster does not support ExpandInUsePersistentVolumes, you will need to manually restart pods after
	// resizing is complete.
	// +optional
	PVCAutoScale *PVCAutoScaleSpec `json:"pvcAutoScale"`

	// Take action when a pod's height falls behind the max height of all pods AND still reports itself as in-sync.
	//
	// +optional
	HeightDriftMitigation *HeightDriftMitigationSpec `json:"heightDriftMitigation"`

	// Automatically prune blockchain data to manage disk usage.
	//
	// +optional
	PruningSpec *PruningPodSpec `json:"pruningSpec"`
}

type PVCAutoScaleSpec struct {
	// The percentage of used disk space required to trigger scaling.
	// Example, if set to 80, autoscaling will not trigger until used space reaches >=80% of capacity.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:MaxSize=100
	UsedSpacePercentage int32 `json:"usedSpacePercentage"`

	// How much to increase the PVC's capacity.
	// Either a percentage (e.g. 20%) or a resource storage quantity (e.g. 100Gi).
	//
	// If a percentage, the existing capacity increases by the percentage.
	// E.g. PVC of 100Gi capacity + IncreaseQuantity of 20% increases disk to 120Gi.
	//
	// If a storage quantity (e.g. 100Gi), increases by that amount.
	IncreaseQuantity string `json:"increaseQuantity"`

	// A resource storage quantity (e.g. 2000Gi).
	// When increasing PVC capacity reaches >= MaxSize, autoscaling ceases.
	// Safeguards against storage quotas and costs.
	// +optional
	MaxSize resource.Quantity `json:"maxSize"`
}

type HeightDriftMitigationSpec struct {
	// If pod's height falls behind the max height of all pods by this value or more AND the pod's RPC /status endpoint
	// reports itself as in-sync, the pod is deleted. The CosmosFullNodeController creates a new pod to replace it.
	// Pod deletion respects the CosmosFullNode.Spec.RolloutStrategy and will not delete more pods than set
	// by the strategy to prevent downtime.
	// This workaround is necessary to mitigate a bug in the Cosmos SDK and/or CometBFT where pods report themselves as
	// in-sync even though they can lag thousands of blocks behind the chain tip and cannot catch up.
	// A "rebooted" pod /status reports itself correctly and allows it to catch up to chain tip.
	// +kubebuilder:validation:Minimum:=1
	Threshold uint32 `json:"threshold"`

	// DeepRecovery enables advanced recovery for pods that are completely stuck.
	// When enabled, if a pod's height doesn't change for StuckDuration, a recovery script is executed.
	// +optional
	DeepRecovery *DeepRecoverySpec `json:"deepRecovery,omitempty"`
}

// DeepRecoverySpec defines advanced recovery options for stuck pods.
type DeepRecoverySpec struct {
	// Duration to wait before considering height as stuck.
	// If pod's height doesn't change for this duration, recovery is triggered.
	// Examples: "5m", "10m", "1h"
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(s|m|h))+$`
	StuckDuration string `json:"stuckDuration"`

	// Shell script to execute for recovery.
	// The script will have access to environment variables:
	// - POD_NAME: Name of the stuck pod
	// - POD_NAMESPACE: Namespace of the stuck pod
	// - CURRENT_HEIGHT: Current height of the stuck pod
	// - PVC_NAME: Name of the PVC
	// - CHAIN_HOME: Home directory of the chain
	// +kubebuilder:validation:Required
	RecoveryScript string `json:"recoveryScript"`

	// Pod template for running the recovery script.
	// If not specified, uses a default busybox pod.
	// +optional
	PodTemplate *RecoveryPodTemplate `json:"podTemplate,omitempty"`

	// VolumeSnapshot settings for backup before recovery.
	// +optional
	VolumeSnapshot *RecoveryVolumeSnapshotSpec `json:"volumeSnapshot,omitempty"`

	// Maximum number of recovery attempts within the rate limit window.
	// Default: 3
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxRetries *int32 `json:"maxRetries,omitempty"`

	// Rate limit window duration (default: 5m).
	// If maxRetries is exceeded within this window, recovery is suspended.
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(s|m|h))+$`
	// +optional
	RateLimitWindow string `json:"rateLimitWindow,omitempty"`

	// Suspend deep recovery operations.
	// +optional
	Suspend bool `json:"suspend,omitempty"`
}

// RecoveryPodTemplate defines the pod template for recovery.
type RecoveryPodTemplate struct {
	// Image to use for the recovery pod.
	// +optional
	Image string `json:"image,omitempty"`

	// Command to run in the recovery pod.
	// +optional
	Command []string `json:"command,omitempty"`

	// Resource requirements for the recovery pod.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// RecoveryVolumeSnapshotSpec defines VolumeSnapshot settings for recovery.
type RecoveryVolumeSnapshotSpec struct {
	// Enabled creates a VolumeSnapshot before running recovery script.
	// +kubebuilder:default:=false
	Enabled bool `json:"enabled"`

	// VolumeSnapshotClassName to use.
	// +optional
	VolumeSnapshotClassName string `json:"volumeSnapshotClassName,omitempty"`

	// RetentionCount is maximum snapshots to keep per pod (default: 3).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=3
	// +optional
	RetentionCount *int32 `json:"retentionCount,omitempty"`
}

type PruningPodSpec struct {
	// Docker image for the pruning pod.
	// If not specified, defaults to "ghcr.io/bharvest-devops/cosmos-pruner:latest".
	// +optional
	Image string `json:"image"`

	// Command to execute for pruning.
	// If not specified, defaults to "cosmos-pruner compact /home/operator/cosmos/data/ 2>&1".
	// +optional
	PruningCommand string `json:"pruningCommand"`
}

type SelfHealingStatus struct {
	// PVC auto-scaling status.
	// +optional
	PVCAutoScale map[string]*PVCAutoScaleStatus `json:"pvcAutoScaler"`

	// DeepRecovery status per pod.
	// +optional
	DeepRecovery map[string]*DeepRecoveryStatus `json:"deepRecovery,omitempty"`
}

// DeepRecoveryPhase represents the current phase of deep recovery.
// +kubebuilder:validation:Enum=Idle;Detected;SnapshotCreating;SnapshotReady;RecoveryRunning;RecoveryCompleted;RecoveryFailed;RateLimited
type DeepRecoveryPhase string

const (
	DeepRecoveryPhaseIdle              DeepRecoveryPhase = "Idle"
	DeepRecoveryPhaseDetected          DeepRecoveryPhase = "Detected"
	DeepRecoveryPhaseSnapshotCreating  DeepRecoveryPhase = "SnapshotCreating"
	DeepRecoveryPhaseSnapshotReady     DeepRecoveryPhase = "SnapshotReady"
	DeepRecoveryPhaseRecoveryRunning   DeepRecoveryPhase = "RecoveryRunning"
	DeepRecoveryPhaseRecoveryCompleted DeepRecoveryPhase = "RecoveryCompleted"
	DeepRecoveryPhaseRecoveryFailed    DeepRecoveryPhase = "RecoveryFailed"
	DeepRecoveryPhaseRateLimited       DeepRecoveryPhase = "RateLimited"
)

// DeepRecoveryStatus tracks recovery state for a single pod.
type DeepRecoveryStatus struct {
	// Current phase of recovery.
	Phase DeepRecoveryPhase `json:"phase"`

	// Last known height when stuck was detected.
	// +optional
	StuckHeight uint64 `json:"stuckHeight,omitempty"`

	// When the stuck condition was first detected.
	// +optional
	DetectedAt *metav1.Time `json:"detectedAt,omitempty"`

	// Name of the VolumeSnapshot created for recovery.
	// +optional
	SnapshotName string `json:"snapshotName,omitempty"`

	// Name of the recovery pod.
	// +optional
	RecoveryPodName string `json:"recoveryPodName,omitempty"`

	// Number of recovery attempts in the current rate limit window.
	// +optional
	RetryCount int32 `json:"retryCount,omitempty"`

	// When the last recovery attempt was made.
	// +optional
	LastAttemptAt *metav1.Time `json:"lastAttemptAt,omitempty"`

	// Message providing additional details about the current phase.
	// +optional
	Message string `json:"message,omitempty"`
}

type PVCAutoScaleStatus struct {
	// The PVC size requested by the SelfHealing controller.
	RequestedSize resource.Quantity `json:"requestedSize"`
	// The timestamp the SelfHealing controller requested a PVC increase.
	RequestedAt metav1.Time `json:"requestedAt"`
}
