package fixtures

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"go.uber.org/zap"
)

var wg sync.WaitGroup

type Fixtures struct {
	log   *zap.Logger
	store map[string]Fixture
	order []string
}

func (f *Fixtures) Add(ctx context.Context, fixtures ...Fixture) error {
	for _, fix := range fixtures {
		if err := f.AddByName(ctx, GenerateString(), fix); err != nil {
			return err
		}
	}
	return nil
}

func (f *Fixtures) AddByName(ctx context.Context, name string, fixture Fixture) error {
	f.log = logger()
	if f.store == nil {
		f.order = []string{}
		f.store = map[string]Fixture{}
	}
	f.order = append(f.order, name)
	f.store[name] = fixture
	err := fixture.SetUp(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup fixture '%v': %w", name, err)
	}
	f.log.Debug("setup", zap.String("type", fmt.Sprint(reflect.TypeOf(fixture).Elem())), zap.String("name", name))
	return err
}

func (f *Fixtures) Get(name string) Fixture {
	return f.store[name]
}

func (f *Fixtures) SetUp(ctx context.Context) error {
	var err error
	for name, fixture := range f.store {
		err = fixture.SetUp(ctx)
		if err != nil {
			return fmt.Errorf("failed to setup fixture '%v': %w", name, err)
		}
		f.log.Debug("setup", zap.String("type", fmt.Sprint(reflect.TypeOf(fixture).Elem())), zap.String("name", name))
	}
	return err
}

func (f *Fixtures) TearDown(ctx context.Context) error {
	defer f.log.Sync()
	fixtureNames := []string{}
	for _, name := range f.order {
		fixtureNames = append([]string{name}, fixtureNames...)
	}
	var firstErr error
	for _, name := range fixtureNames {
		fixture := f.Get(name)
		err := fixture.TearDown(ctx)
		if err != nil {
			f.log.Warn("failed to teardown fixture", zap.String("fixture", name), zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
		}
		f.log.Debug("teardown", zap.String("type", fmt.Sprint(reflect.TypeOf(fixture).Elem())), zap.String("name", name))
	}

	wg.Wait()
	return firstErr
}

// RecoverTearDown returns a deferrable function that will teardown in the event of a panic.
func (f *Fixtures) RecoverTearDown(ctx context.Context) func() {
	return func() {
		if r := recover(); r != nil {
			if err := f.TearDown(ctx); err != nil {
				f.log.Warn("failed to tear down", zap.Error(err))
			}
			panic(r)
		}
	}
}
