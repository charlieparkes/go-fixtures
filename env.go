package fixtures

import (
    "log"

    "github.com/vrischmann/envconfig"
)

type environment struct {
    Debug bool `envconfig:"default=false"`
}

var env *environment

func init() {
    env = &environment{}
    if err := envconfig.Init(env); err != nil {
        log.Fatal(err)
    }
}

func getEnv() *environment {
    return env
}
