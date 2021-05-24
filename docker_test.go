package fixtures

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDocker(t *testing.T) {
	f := Docker{}
	assert.NoError(t, f.SetUp())
	assert.NoError(t, f.TearDown())
}
