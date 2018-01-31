package utils

import "fmt"

// Debug points whether at debug mode.
var Debug = true

// Assert panic at debug when cond is false
func Assert(cond bool, format string, a ...interface{}) {
	if Debug && !cond {
		panic(fmt.Sprintf(format, a...))
	}
}

// AssertNotNil panic at debug when obj is nil
func AssertNotNil(obj interface{}, format string, a ...interface{}) {
	Assert(obj != nil, format, a...)
}
