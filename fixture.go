package fixtures

import (
	"fmt"
	"log"
	"reflect"
)

type BaseFixture struct{}

func (f *BaseFixture) Type() string {
	return fmt.Sprint(reflect.TypeOf(f).Elem())
}

type Fixture interface {
	Type() string
	SetUp() error
	TearDown() error
}

type Fixtures struct {
	store map[string]Fixture
	order []string
}

func (f *Fixtures) Add(fixture Fixture) error {
	return f.AddByName(GenerateString(), fixture)
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
		log.Fatalf("Failed to setup fixture '%v': %v", name, err)
	} else {
		status = 0
	}
	if env.Debug {
		fmt.Printf("%v Setup %v<%v>\n", GetStatusSymbol(status), fmt.Sprint(reflect.TypeOf(fixture).Elem()), name)
	}
	return err
}

func (f *Fixtures) Get(name string) Fixture {
	return f.store[name]
}

func (f *Fixtures) SetUp() {
	for name, fixture := range f.store {
		err := fixture.SetUp()
		var status int
		if err != nil {
			status = 1
			log.Fatalf("Failed to setup fixture '%v': %v", name, err)
		} else {
			status = 0
		}
		if env.Debug {
			fmt.Printf("%v Setup %v<%v>\n", GetStatusSymbol(status), fmt.Sprint(reflect.TypeOf(fixture).Elem()), name)
		}
	}
}

func (f *Fixtures) TearDown() {
	fixtureNames := []string{}
	for _, name := range f.order {
		fixtureNames = append([]string{name}, fixtureNames...)
	}
	for _, name := range fixtureNames {
		fixture := f.Get(name)
		err := fixture.TearDown()
		var status int
		if err != nil {
			status = 1
			log.Fatalf("Failed to teardown fixture '%v': %v", name, err)
		} else {
			status = 0
		}
		if env.Debug {
			fmt.Printf("%v Teardown %v<%v>\n", GetStatusSymbol(status), fmt.Sprint(reflect.TypeOf(fixture).Elem()), name)
		}
	}
}
