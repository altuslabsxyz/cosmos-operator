package stuckheight

import (
	"context"
	"errors"
	"fmt"
	"sync"

	cosmosv1 "github.com/b-harvest/cosmos-operator/api/v1"
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var errTest = errors.New("test error")

type mockClient struct {
	mu sync.Mutex

	Object       any
	GetObjectKey client.ObjectKey
	GetObjectErr error

	ObjectList  any
	GotListOpts []client.ListOption
	ListErr     error

	CreateCount      int
	LastCreateObject client.Object
	CreatedObjects   []client.Object
	CreateErr        error

	DeleteCount int

	PatchCount      int
	LastPatchObject client.Object
	LastPatch       client.Patch

	LastUpdateObject client.Object
	UpdateCount      int
	UpdateErr        error
}

func (m *mockClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ctx == nil {
		panic("nil context")
	}
	m.GetObjectKey = key
	if m.Object == nil {
		return m.GetObjectErr
	}

	switch ref := obj.(type) {
	case *corev1.Pod:
		*ref = m.Object.(corev1.Pod)
	case *corev1.PersistentVolumeClaim:
		*ref = m.Object.(corev1.PersistentVolumeClaim)
	case *cosmosv1.CosmosFullNode:
		*ref = m.Object.(cosmosv1.CosmosFullNode)
	case *snapshotv1.VolumeSnapshot:
		*ref = m.Object.(snapshotv1.VolumeSnapshot)
	default:
		panic(fmt.Errorf("unknown Object type: %T", obj))
	}
	return m.GetObjectErr
}

func (m *mockClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ctx == nil {
		panic("nil context")
	}
	m.GotListOpts = opts

	if m.ObjectList == nil {
		return m.ListErr
	}

	switch ref := list.(type) {
	case *corev1.PodList:
		*ref = m.ObjectList.(corev1.PodList)
	case *cosmosv1.CosmosFullNodeList:
		*ref = m.ObjectList.(cosmosv1.CosmosFullNodeList)
	default:
		panic(fmt.Errorf("unknown ObjectList type: %T", list))
	}

	return m.ListErr
}

func (m *mockClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ctx == nil {
		panic("nil context")
	}
	m.LastCreateObject = obj
	m.CreatedObjects = append(m.CreatedObjects, obj)
	m.CreateCount++
	return m.CreateErr
}

func (m *mockClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ctx == nil {
		panic("nil context")
	}
	m.DeleteCount++
	return nil
}

func (m *mockClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ctx == nil {
		panic("nil context")
	}
	m.UpdateCount++
	m.LastUpdateObject = obj
	return m.UpdateErr
}

func (m *mockClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ctx == nil {
		panic("nil context")
	}
	m.PatchCount++
	m.LastPatchObject = obj
	m.LastPatch = patch
	return nil
}

func (m *mockClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	panic("implement me")
}

func (m *mockClient) Scheme() *runtime.Scheme {
	m.mu.Lock()
	defer m.mu.Unlock()

	scheme := runtime.NewScheme()
	if err := cosmosv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := snapshotv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return scheme
}

func (m *mockClient) Status() client.StatusWriter {
	return m
}

func (m *mockClient) RESTMapper() meta.RESTMapper {
	panic("implement me")
}

