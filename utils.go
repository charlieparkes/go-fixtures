package fixtures

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/charlieparkes/go-fixtures/internal/env"
)

func GenerateString() string {
	rand.Seed(time.Now().UTC().UnixNano())
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, 10) // Make some space
	for i := 0; i < 10; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

func debugPrintf(format string, a ...interface{}) (int, error) {
	if env.Get().Debug {
		return fmt.Printf(format, a...)
	}
	return 0, nil
}
