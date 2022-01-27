package fixtures

import (
	"fmt"
	"log"
	"reflect"
	"sync"
)

var wg sync.WaitGroup

type Fixtures struct {
	store map[string]Fixture
	order []string
}

func (f *Fixtures) Add(fixtures ...Fixture) error {
	for _, fix := range fixtures {
		if err := f.AddByName(GenerateString(), fix); err != nil {
			return err
		}
	}
	return nil
}

func (f *Fixtures) AddByName(name string, fixture Fixture) error {
	if f.store == nil {
		f.order = []string{}
		f.store = map[string]Fixture{}
	}
	f.order = append(f.order, name)
	f.store[name] = fixture
	err := fixture.SetUp()
	var status int
	if err != nil {
		status = 1
		return fmt.Errorf("failed to setup fixture '%v': %w", name, err)
	} else {
		status = 0
	}
	debugPrintf("%v Setup %v<%v>\n", GetStatusSymbol(status), fmt.Sprint(reflect.TypeOf(fixture).Elem()), name)
	return err
}

func (f *Fixtures) Get(name string) Fixture {
	return f.store[name]
}

func (f *Fixtures) SetUp() error {
	var err error
	for name, fixture := range f.store {
		err = fixture.SetUp()

		var status int
		if err != nil {
			status = 1
			return fmt.Errorf("failed to setup fixture '%v': %w", name, err)
		} else {
			status = 0
		}
		debugPrintf("%v Setup %v<%v>\n", GetStatusSymbol(status), fmt.Sprint(reflect.TypeOf(fixture).Elem()), name)

		if err != nil {
			return err
		}
	}
	return err
}

func (f *Fixtures) TearDown() error {
	fixtureNames := []string{}
	for _, name := range f.order {
		fixtureNames = append([]string{name}, fixtureNames...)
	}
	var firstErr error
	for _, name := range fixtureNames {
		fixture := f.Get(name)
		err := fixture.TearDown()

		var status int
		if err != nil {
			status = 1
			log.Printf("Failed to teardown fixture '%v': %v", name, err)

			if firstErr == nil {
				firstErr = err
			}
		} else {
			status = 0
		}
		debugPrintf("%v Teardown %v<%v>\n", GetStatusSymbol(status), fmt.Sprint(reflect.TypeOf(fixture).Elem()), name)
	}

	wg.Wait()
	return firstErr
}

// RecoverTearDown returns a deferrable function that will teardown in the event of a panic.
func (f *Fixtures) RecoverTearDown() func() {
	return func() {
		if r := recover(); r != nil {
			if err := f.TearDown(); err != nil {
				log.Println("failed to tear down:", err)
			}
			panic(r)
		}
	}
}
