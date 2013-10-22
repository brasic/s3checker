package main

import (
	"fmt"
	"os"
	"strings"
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
	return found > format(expecting)
}

// Return a key that is lex. less than the input, for use as a starting point.
func predecessor(num string) (prev string) {
	return num[:len(num)-1]
}

// Transform an id-formatted string into key-format.
func format(num string) string {
	return employerId + "/docs/" + num + ".pdf"
}

// Transform a key-formatted string into id-format.
func deformat(key string) string {
	pieces := strings.Split(key, "/")
	item := pieces[len(pieces)-1]
	return strings.Split(item, ".")[0]
}
