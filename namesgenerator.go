package fixtures

import (
	"fmt"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/google/uuid"
)

func GetRandomName(retry int) string {
	return fmt.Sprint(namesgenerator.GetRandomName(0), "_", uuid.NewString()[:8])
}
