package main

import (
	"fmt"
	"os"
)

var isDebug bool

func init() {
	if os.Getenv("DEBUG") != "" {
		isDebug = true
	}
}

// Print a message to standard error if in debug mode.
func debug(args ...interface{}) {
	if isDebug {
		fmt.Fprintln(os.Stderr, args...)
	}
}

// Is the found S3 key lex. greater than the equivalent id key?
func gt(found, expecting string) bool {
	return found > expecting
}

// Return a key that is lex. less than the input, for use as a starting point.
func predecessor(num string) (prev string) {
	return num[:len(num)-1]
}
