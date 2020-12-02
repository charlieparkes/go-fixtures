package fixtures

import (
	"log"

	"github.com/vrischmann/envconfig"
)

type Environment struct {
	Debug bool `envconfig:"default=false"`
}

var env *Environment

func init() {
	env = &Environment{}
	if err := envconfig.Init(env); err != nil {
		log.Fatal(err)
	}
}
