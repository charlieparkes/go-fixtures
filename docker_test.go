package fixtures

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDocker(t *testing.T) {
	f := Docker{}
	assert.NoError(t, f.SetUp())
	assert.NoError(t, f.TearDown())
}

func TestDockerUtils(t *testing.T) {
	f := Docker{}
	f.SetUp()
	defer f.TearDown()

	name := GenerateString()
	resource, err := f.Pool.BuildAndRun(name, "test/Dockerfile", nil)
	assert.NoError(t, err)

	assert.Equal(t, "", GetHostIP(resource, f.Network))
	assert.Equal(t, name, GetHostName(resource))

	assert.Equal(t, fmt.Sprintf("/%v", name), resource.Container.Name)
	assert.NoError(t, f.Pool.Purge(resource))
}
