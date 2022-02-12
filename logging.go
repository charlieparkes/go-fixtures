package fixtures

import "go.uber.org/zap"

func logger() *zap.Logger {
	var l *zap.Logger
	var err error
	if getEnv().Debug {
		l, err = zap.NewDevelopment()
	} else {
		l, err = zap.NewProduction()
	}
	if err != nil {
		panic(err)
	}
	return l
}
