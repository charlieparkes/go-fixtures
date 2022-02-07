package fixtures

import (
    "context"
    "fmt"
    "reflect"
)

type BaseFixture struct{}

func (f *BaseFixture) Type() string {
    return fmt.Sprint(reflect.TypeOf(f).Elem())
}

type Fixture interface {
    Type() string
    SetUp(context.Context) error
    TearDown(context.Context) error
}
