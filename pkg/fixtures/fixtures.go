package fixtures

import (
	"fmt"
	"log"
	"reflect"

	"github.com/charlieparkes/go-fixtures/internal/env"
	"github.com/charlieparkes/go-fixtures/internal/helpers"
	"github.com/charlieparkes/go-fixtures/pkg/symbols"
)

type Fixtures struct {
	store map[string]Fixture
	order []string
}

func (f *Fixtures) Add(fixture Fixture) error {
	return f.AddByName(helpers.GenerateString(), fixture)
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
	if env.Get().Debug {
		fmt.Printf("%v Setup %v<%v>\n", symbols.GetStatusSymbol(status), fmt.Sprint(reflect.TypeOf(fixture).Elem()), name)
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
		if env.Get().Debug {
			fmt.Printf("%v Setup %v<%v>\n", symbols.GetStatusSymbol(status), fmt.Sprint(reflect.TypeOf(fixture).Elem()), name)
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
		if env.Get().Debug {
			fmt.Printf("%v Teardown %v<%v>\n", symbols.GetStatusSymbol(status), fmt.Sprint(reflect.TypeOf(fixture).Elem()), name)
		}
	}
}
