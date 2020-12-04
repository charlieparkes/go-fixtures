package fixtures

import (
	"log"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/ory/dockertest/v3"
)

func generateString() string {
	rand.Seed(time.Now().UTC().UnixNano())
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, 10) // Make some space
	for i := 0; i < 10; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

func getTestDataPath(name string) string {
	path := filepath.Join("testdata", name)
	return path
}

func waitForContainer(pool *dockertest.Pool, resource *dockertest.Resource) int {
	exitCode, err := pool.Client.WaitContainer(resource.Container.ID)
	if err != nil {
		log.Fatalf("Unable to wait for container: %s", err)
	}
	return exitCode
}
