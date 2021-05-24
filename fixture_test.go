package fixtures

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type DummyFixture struct {
	BaseFixture
	DummyMember int
}

func (df *DummyFixture) SetUp() error {
	df.DummyMember = 123
	return nil
}

func (df *DummyFixture) TearDown() error {
	df.DummyMember = 0
	return nil
}

func TestGetType(t *testing.T) {
	f := DummyFixture{}
	assert.Equal(t, "fixtures.BaseFixture", f.Type())
}

func TestFixtures(t *testing.T) {
	fixtures := Fixtures{}
	df := DummyFixture{}
	df2 := DummyFixture{}

	err := fixtures.Add(&df)
	assert.Nil(t, err)

	err = fixtures.AddByName("foobar", &df2)
	assert.Nil(t, err)

	fixtures.SetUp()
	f := fixtures.Get("foobar").(*DummyFixture)
	assert.Equal(t, 123, f.DummyMember)

	fixtures.TearDown()
	assert.Equal(t, 0, f.DummyMember)
}
