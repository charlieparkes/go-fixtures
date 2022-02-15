package fixtures

import "go.uber.org/zap"

func logger() *zap.Logger {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	return l
}
