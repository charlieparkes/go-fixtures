package fixtures

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/cenkalti/backoff/v3"
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

func FindPath(containing string) string {
	workingDirectory, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	lastDir := workingDirectory
	for {
		currentPath := fmt.Sprintf("%s/%s", lastDir, containing)
		fi, err := os.Stat(currentPath)
		if err == nil {
			switch mode := fi.Mode(); {
			case mode.IsDir():
				return currentPath
			}
		}
		newDir := filepath.Dir(lastDir)
		if newDir == "/" || newDir == lastDir {
			return ""
		}
		lastDir = newDir
	}
}

func Retry(d time.Duration, op func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Second
	bo.MaxElapsedTime = time.Duration(d)
	return backoff.Retry(op, bo)
}

func debugPrintf(format string, a ...interface{}) (int, error) {
	if env.Get().Debug {
		return fmt.Printf(format, a...)
	}
	return 0, nil
}

func debugPrintln(i ...interface{}) (int, error) {
	if env.Get().Debug {
		return fmt.Println(i...)
	}
	return 0, nil
}
