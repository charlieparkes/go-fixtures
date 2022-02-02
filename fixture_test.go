package fixtures

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type DummyFixture struct {
	BaseFixture
	DummyMember int
}

func (df *DummyFixture) SetUp(ctx context.Context) error {
	df.DummyMember = 123
	return nil
}

func (df *DummyFixture) TearDown(context.Context) error {
	df.DummyMember = 0
	return nil
}

func TestGetType(t *testing.T) {
	f := DummyFixture{}
	assert.Equal(t, "fixtures.BaseFixture", f.Type())
}

func TestFixtures(t *testing.T) {
	ctx := context.Background()
	fixtures := Fixtures{}
	df := DummyFixture{}
	df2 := DummyFixture{}

	err := fixtures.Add(ctx, &df)
	assert.Nil(t, err)

	err = fixtures.AddByName(ctx, "foobar", &df2)
	assert.Nil(t, err)

	fixtures.SetUp(ctx)
	f := fixtures.Get("foobar").(*DummyFixture)
	assert.Equal(t, 123, f.DummyMember)

	fixtures.TearDown(ctx)
	assert.Equal(t, 0, f.DummyMember)
}
