package fixtures

// #include <unistd.h>
import "C"

var mem_bytes int64

func init() {
	mem_bytes = int64(C.sysconf(C._SC_PHYS_PAGES) * C.sysconf(C._SC_PAGE_SIZE))
}

func memoryMB() int64 {
	return mem_bytes / 1e6
}
