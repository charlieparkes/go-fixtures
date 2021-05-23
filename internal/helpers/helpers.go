package helpers

import (
	"math/rand"
	"path/filepath"
	"time"
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

func GetTestDataPath(name string) string {
	path := filepath.Join("testdata", name)
	return path
}
