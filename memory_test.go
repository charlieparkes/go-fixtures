package fixtures

import (
    "fmt"
    "testing"
)

func TestMemory(t *testing.T) {
    fmt.Println("memory MB", memoryMB())
}
