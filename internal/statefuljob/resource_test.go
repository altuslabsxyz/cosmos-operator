package statefuljob

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	cosmosalpha "github.com/altuslabsxyz/cosmos-operator/api/v1alpha1"
)

func TestResourceName(t *testing.T) {
	t.Parallel()

	var crd cosmosalpha.StatefulJob
	crd.Name = "test"

	require.Equal(t, "test", ResourceName(&crd))

	crd.Name = strings.Repeat("long", 100)
	name := ResourceName(&crd)
	require.LessOrEqual(t, 253, len(name))
}
