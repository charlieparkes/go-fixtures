package fixtures

import (
	"context"
	"fmt"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocker(t *testing.T) {
	ctx := context.Background()

	require.True(t, IsDockerRunning())

	f := NewDocker()
	assert.NoError(t, f.SetUp(ctx))
	defer f.TearDown(ctx)

	name := GenerateString()
	resource, err := f.Pool().RunWithOptions(&dockertest.RunOptions{Name: name, Repository: "crccheck/hello-world", Tag: "latest", Env: nil})
	assert.NoError(t, err)

	assert.Equal(t, "", HostIP(resource, f.Network()))
	assert.Equal(t, name, HostName(resource))

	assert.Equal(t, fmt.Sprintf("/%v", name), resource.Container.Name)
	assert.NoError(t, f.Pool().Purge(resource))
}
