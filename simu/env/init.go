package envior

import (
	"fmt"
	"runtime"
)

func init() {
	if runtime.NumCPU() < 2 {
		fmt.Printf("warning: only One CPU, which may conceal locking bugs\n")
	}
	runtime.GOMAXPROCS(4)
}
