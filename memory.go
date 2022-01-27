package fixtures

import (
	"github.com/tklauser/go-sysconf"
)

func mustSysconf(name int) int64 {
	x, err := sysconf.Sysconf(name)
	if err != nil {
		panic(err)
	}
	return x
}

func memoryMB() int64 {
	return int64(mustSysconf(sysconf.SC_PHYS_PAGES)*mustSysconf(sysconf.SC_PAGE_SIZE)) / 1e6
}
