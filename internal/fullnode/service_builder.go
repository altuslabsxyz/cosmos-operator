package fullnode

import (
	"fmt"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	cosmosv1 "github.com/altuslabsxyz/cosmos-operator/api/v1"
	"github.com/altuslabsxyz/cosmos-operator/internal/diff"
	"github.com/altuslabsxyz/cosmos-operator/internal/kube"
)

const maxP2PServiceDefault = int32(1)

// BuildServices returns a list of services given the crd.
//
// Creates a single RPC service, likely for use with an Ingress.
//
// Creates 1 p2p service per pod. P2P diverges from traditional web and kubernetes architecture which calls for a single
// p2p service backed by multiple pods.
// Pods may be in various states even with proper readiness probes.
// Therefore, we do not want to confuse or disrupt peer exchange (PEX) within CometBFT.
// If using a single p2p service, an outside peer discovering a pod out of sync it could be
// interpreted as byzantine behavior if the peer previously connected to a pod that was in sync through the same
// external address.
func BuildServices(crd *cosmosv1.CosmosFullNode) []diff.Resource[*corev1.Service] {
	maxP2P := maxP2PServiceDefault
	if v := crd.Spec.Service.MaxP2PExternalAddresses; v != nil {
		maxP2P = *v
	}
	maxExternal := lo.Clamp(maxP2P, 0, crd.Spec.Replicas)
	svcs := make([]diff.Resource[*corev1.Service], func(nodeType cosmosv1.FullNodeType) int32 {
		if nodeType == cosmosv1.Sentry {
			return crd.Spec.Replicas * 2
		}
		return crd.Spec.Replicas
	}(crd.Spec.Type))

	for i := int32(0); i < crd.Spec.Replicas; i++ {
		ordinal := i
		var svc corev1.Service
		svc.Name = p2pServiceName(crd, ordinal)
		svc.Namespace = crd.Namespace
		svc.Kind = "Service"
		svc.APIVersion = "v1"

		svc.Labels = defaultLabels(crd,
			kube.InstanceLabel, instanceName(crd, ordinal),
			kube.ComponentLabel, "p2p",
		)
		svc.Annotations = map[string]string{}

		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "p2p",
				Protocol:   corev1.ProtocolTCP,
				Port:       p2pPort,
				TargetPort: intstr.FromString("p2p"),
			},
		}
		svc.Spec.Selector = map[string]string{kube.InstanceLabel: instanceName(crd, ordinal)}

		if i < maxExternal {
			preserveMergeInto(svc.Labels, crd.Spec.Service.P2PTemplate.Metadata.Labels)
			preserveMergeInto(svc.Annotations, crd.Spec.Service.P2PTemplate.Metadata.Annotations)
			svc.Spec.Type = *valOrDefault(crd.Spec.Service.P2PTemplate.Type, ptr(corev1.ServiceTypeLoadBalancer))
			svc.Spec.ExternalTrafficPolicy = *valOrDefault(crd.Spec.Service.P2PTemplate.ExternalTrafficPolicy, ptr(corev1.ServiceExternalTrafficPolicyLocal))
		} else {
			svc.Spec.Type = corev1.ServiceTypeClusterIP
		}

		svcs[i] = diff.Adapt(&svc, i)
	}

	rpc := rpcService(crd)

	if crd.Spec.Type == cosmosv1.Sentry {
		for i := int32(0); i < crd.Spec.Replicas; i++ {
			ordinal := i
			var svc corev1.Service
			svc.Name = sentryServiceName(crd, ordinal)
			svc.Namespace = crd.Namespace
			svc.Kind = "Service"
			svc.APIVersion = "v1"

			svc.Labels = defaultLabels(crd,
				kube.InstanceLabel, instanceName(crd, ordinal),
				kube.ComponentLabel, "sentry",
			)
			svc.Annotations = map[string]string{}

			svc.Spec.Ports = []corev1.ServicePort{
				{
					// https://github.com/strangelove-ventures/horcrux-proxy/blob/23a7d31806ce62481162a64adcca848fd879bc52/cmd/watcher.go#L162
					Name:       "sentry-privval",
					Protocol:   corev1.ProtocolTCP,
					Port:       privvalPort,
					TargetPort: intstr.FromString("privval"),
				},
			}
			svc.Spec.Selector = map[string]string{kube.InstanceLabel: instanceName(crd, ordinal)}

			preserveMergeInto(svc.Labels, crd.Spec.Service.P2PTemplate.Metadata.Labels)
			preserveMergeInto(svc.Annotations, crd.Spec.Service.P2PTemplate.Metadata.Annotations)
			svc.Spec.Type = corev1.ServiceTypeClusterIP

			// If you run pod as sentry mode, it'll be unhealthy cause of no API from app.
			// But to run cosmos app, horcrux-proxy should connect to pod even pod's status is unhealthy.
			// therefore, you should allow this option.
			svc.Spec.PublishNotReadyAddresses = true

			// Append sentry services after p2p services
			svcs[crd.Spec.Replicas+i] = diff.Adapt(&svc, int(crd.Spec.Replicas+i))
		}
	}

	return append(svcs, diff.Adapt(rpc, len(svcs)))
}

func rpcService(crd *cosmosv1.CosmosFullNode) *corev1.Service {
	var svc corev1.Service
	svc.Name = rpcServiceName(crd)
	svc.Namespace = crd.Namespace
	svc.Kind = "Service"
	svc.APIVersion = "v1"
	svc.Labels = defaultLabels(crd,
		kube.ComponentLabel, "rpc",
	)
	svc.Annotations = map[string]string{}

	svc.Spec.Ports = []corev1.ServicePort{
		{
			Name:       "api",
			Protocol:   corev1.ProtocolTCP,
			Port:       apiPort,
			TargetPort: intstr.FromString("api"),
		},
		{
			Name:       "jsonrpc",
			Protocol:   corev1.ProtocolTCP,
			Port:       jsonrpcPort,
			TargetPort: intstr.FromString("jsonrpc"),
		},
		{
			Name:       "jsonrpc-ws",
			Protocol:   corev1.ProtocolTCP,
			Port:       jsonrpcWsPort,
			TargetPort: intstr.FromString("jsonrpc-ws"),
		},
		{
			Name:       "rosetta",
			Protocol:   corev1.ProtocolTCP,
			Port:       rosettaPort,
			TargetPort: intstr.FromString("rosetta"),
		},
		{
			Name:       "grpc",
			Protocol:   corev1.ProtocolTCP,
			Port:       grpcPort,
			TargetPort: intstr.FromString("grpc"),
		},
		{
			Name:       "rpc",
			Protocol:   corev1.ProtocolTCP,
			Port:       rpcPort,
			TargetPort: intstr.FromString("rpc"),
		},
		{
			Name:       "grpc-web",
			Protocol:   corev1.ProtocolTCP,
			Port:       grpcWebPort,
			TargetPort: intstr.FromString("grpc-web"),
		},
	}

	svc.Spec.Selector = map[string]string{kube.NameLabel: appName(crd)}
	svc.Spec.Type = corev1.ServiceTypeClusterIP

	rpcSpec := crd.Spec.Service.RPCTemplate
	preserveMergeInto(svc.Labels, rpcSpec.Metadata.Labels)
	preserveMergeInto(svc.Annotations, rpcSpec.Metadata.Annotations)
	kube.NormalizeMetadata(&svc.ObjectMeta)

	if v := rpcSpec.ExternalTrafficPolicy; v != nil {
		svc.Spec.ExternalTrafficPolicy = *v
	}
	if v := rpcSpec.Type; v != nil {
		svc.Spec.Type = *v
	}

	return &svc
}

func p2pServiceName(crd *cosmosv1.CosmosFullNode, ordinal int32) string {
	return fmt.Sprintf("%s-p2p-%d", appName(crd), ordinal)
}

func sentryServiceName(crd *cosmosv1.CosmosFullNode, ordinal int32) string {
	return fmt.Sprintf("%s-sentry-%d", appName(crd), ordinal)
}

func rpcServiceName(crd *cosmosv1.CosmosFullNode) string {
	return fmt.Sprintf("%s-rpc", appName(crd))
}
