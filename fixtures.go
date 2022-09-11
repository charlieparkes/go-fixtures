package fixtures

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"go.uber.org/zap"
)

var wg sync.WaitGroup

type FixturesOpt func(*Fixtures)

func NewFixtures(opts ...FixturesOpt) *Fixtures {
	f := &Fixtures{}
	for _, opt := range opts {
		opt(f)
	}
	if f.log == nil {
		f.log = logger()
	}
	return f
}

func FixturesLogger(logger *zap.Logger) FixturesOpt {
	return func(f *Fixtures) {
		f.log = logger
	}
}

type Fixtures struct {
	log   *zap.Logger
	store map[string]Fixture
	order []string
}

func (f *Fixtures) Add(ctx context.Context, fixtures ...Fixture) error {
	for _, fix := range fixtures {
		if err := f.AddByName(ctx, GetRandomName(0), fix); err != nil {
			return err
		}
	}
	return nil
}

func (f *Fixtures) AddByName(ctx context.Context, name string, fixture Fixture) error {
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
func (f *Fixtures) RecoverTearDown(ctx context.Context) {
	if r := recover(); r != nil {
		if err := f.TearDown(ctx); err != nil {
			f.log.Warn("failed to tear down", zap.Error(err))
		}
		panic(r)
	}
}

// Docker() returns the first Docker fixture. If none exists, panic.
func (f *Fixtures) Docker() *Docker {
	for _, x := range f.store {
		if val, ok := x.(*Docker); ok {
			return val
		}
	}
	panic("no docker fixture found")
}

// Postgres() returns the first Postgres fixture. If none exists, panic.
func (f *Fixtures) Postgres() *Postgres {
	for _, x := range f.store {
		if val, ok := x.(*Postgres); ok {
			return val
		}
	}
	panic("no postgres fixture found")
}
