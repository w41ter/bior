package utils

import "fmt"

func Assert(cond bool, format string, a ...interface{}) {
	if !cond {
		panic(fmt.Sprintf(format, a...))
	}
}

func AssertNotNil(obj interface{}, format string, a ...interface{}) {
	Assert(obj != nil, format, a...)
}
