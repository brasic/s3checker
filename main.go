package main

import (
	"bufio"
	"flag"
	"fmt"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"os"
	"sort"
)

var bucket *s3.Bucket
var invertResults bool
var apiCalls int64 = 0

func init() {
	flag.BoolVar(&invertResults, "v", false, "Invert results")
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		usage("Need S3_BUCKET")
	}
	connect(args[0])
}

func main() {
	keysToCheck := readKeys()
	presenceMap := checkBulkKeys(keysToCheck)
	printKeys(presenceMap)
	debug("Calls to S3 API:", apiCalls)
}

// Connect to S3 using the HTTP endpoint for performance.
func connect(bucketName string) {
	auth, err := aws.EnvAuth()
	if err != nil {
		usage("S3 connect failed due to auth issues, exiting!")
	}
	USEast := aws.Region{S3Endpoint: "http://s3.amazonaws.com"}
	bucket = s3.New(auth, USEast).Bucket(bucketName)
}

// Read the list of ids from standard input, validate and return them sorted
// lexicographically.
func readKeys() (keys []string) {
	keys = make([]string, 0)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		keys = append(keys, text)
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	sort.Sort(sort.StringSlice(keys))
	return
}

// Return the keys that were not found given a presence map.
func keysNotFound(presence map[string]bool) (notFound []string) {
	notFound = make([]string, 0)
	for key, present := range presence {
		if !present {
			notFound = append(notFound, key)
		}
	}
	debug("not found by bulk:", notFound)
	return
}

// Print all keys that were not found (or the opposite if invertResults is set)
func printKeys(presence map[string]bool) {
	for key, wasFound := range presence {
		if (!invertResults && !wasFound) || (invertResults && wasFound) {
			fmt.Println(key)
		}
	}
}

func usage(args ...interface{}) {
	if len(args) > 0 {
		fmt.Println(args...)
	}
	println("Usage: " + os.Args[0] + " [-v] S3_BUCKET")
	println("Pass keys to check on standard input.")
	os.Exit(1)
}
