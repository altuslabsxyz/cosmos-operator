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

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	DefaultRecoveryImage = "busybox:latest"
)

// RecoveryPodBuilder builds recovery pods
type RecoveryPodBuilder struct {
	client client.Client
}

// NewRecoveryPodBuilder creates a new RecoveryPodBuilder
func NewRecoveryPodBuilder(client client.Client) *RecoveryPodBuilder {
	return &RecoveryPodBuilder{
		client: client,
	}
}

// BuildPod creates a recovery pod spec
func (b *RecoveryPodBuilder) BuildPod(
	recovery *cosmosv1.StuckHeightRecovery,
	podName string,
	pvcName string,
	currentHeight uint64,
) *corev1.Pod {
	podTemplate := recovery.Spec.PodTemplate
	if podTemplate == nil {
		podTemplate = &cosmosv1.RecoveryPodTemplate{}
	}

	// Default image
	image := DefaultRecoveryImage
	if podTemplate.Image != "" {
		image = podTemplate.Image
	}

	// Default image pull policy
	imagePullPolicy := corev1.PullIfNotPresent
	if podTemplate.ImagePullPolicy != "" {
		imagePullPolicy = podTemplate.ImagePullPolicy
	}

	// Default resources
	resources := podTemplate.Resources
	if resources.Requests == nil {
		resources.Requests = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		}
	}

	// Base environment variables
	envVars := []corev1.EnvVar{
		{Name: "POD_NAME", Value: recovery.Status.StuckPodName},
		{Name: "POD_NAMESPACE", Value: recovery.Namespace},
		{Name: "CURRENT_HEIGHT", Value: fmt.Sprintf("%d", currentHeight)},
		{Name: "PVC_NAME", Value: pvcName},
		{Name: "CHAIN_HOME", Value: "/chain-home"},
	}

	// Add custom env vars
	if len(podTemplate.Env) > 0 {
		envVars = append(envVars, podTemplate.Env...)
	}

	// Base volume mounts
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "chain-data",
			MountPath: "/chain-home",
		},
	}

	// Add custom volume mounts
	if len(podTemplate.VolumeMounts) > 0 {
		volumeMounts = append(volumeMounts, podTemplate.VolumeMounts...)
	}

	// Base volumes
	volumes := []corev1.Volume{
		{
			Name: "chain-data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		},
	}

	// Add custom volumes
	if len(podTemplate.Volumes) > 0 {
		volumes = append(volumes, podTemplate.Volumes...)
	}

	recoveryPodName := fmt.Sprintf("%s-recovery-%s", recovery.Name, podName)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      recoveryPodName,
			Namespace: recovery.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "cosmos-operator",
				"cosmos.bharvest.io/recovery":  recovery.Name,
				"cosmos.bharvest.io/type":      "recovery-pod",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            "recovery",
					Image:           image,
					ImagePullPolicy: imagePullPolicy,
					Command:         []string{"/bin/sh"},
					Args: []string{
						"-c",
						recovery.Spec.RecoveryScript,
					},
					Env:          envVars,
					VolumeMounts: volumeMounts,
					Resources:    resources,
				},
			},
			Volumes: volumes,
		},
	}

	return pod
}

// CreatePod creates a recovery pod
func (b *RecoveryPodBuilder) CreatePod(
	ctx context.Context,
	recovery *cosmosv1.StuckHeightRecovery,
	podName string,
	pvcName string,
	currentHeight uint64,
) (string, error) {
	pod := b.BuildPod(recovery, podName, pvcName, currentHeight)

	// Set owner reference
	if err := controllerutil.SetControllerReference(recovery, pod, b.client.Scheme()); err != nil {
		return "", fmt.Errorf("set controller reference: %w", err)
	}

	if err := b.client.Create(ctx, pod); err != nil {
		return "", fmt.Errorf("create recovery pod: %w", err)
	}

	return pod.Name, nil
}

// CheckPodComplete checks if the recovery pod has completed
func (b *RecoveryPodBuilder) CheckPodComplete(
	ctx context.Context,
	namespace string,
	podName string,
) (bool, bool, error) {
	pod := &corev1.Pod{}
	if err := b.client.Get(ctx, client.ObjectKey{
		Name:      podName,
		Namespace: namespace,
	}, pod); err != nil {
		return false, false, fmt.Errorf("get recovery pod: %w", err)
	}

	if pod.Status.Phase == corev1.PodSucceeded {
		return true, true, nil
	}

	if pod.Status.Phase == corev1.PodFailed {
		return true, false, nil
	}

	return false, false, nil
}
