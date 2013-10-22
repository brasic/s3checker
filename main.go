package main

import (
	"bufio"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"os"
	"sort"
	"strconv"
)

var bucket *s3.Bucket
var bucketName string
var employerId string
var apiCalls int64 = 0

const workerCount = 20

func main() {
	processArgs()
	idsToCheck := readIds()
	presenceMap := checkBulkKeys(idsToCheck)
	notFound := keysNotFound(presenceMap)
	checkIndividualKeys(notFound)
	debug("Calls to S3 API:", apiCalls)
}

func processArgs() {
	if len(os.Args) < 3 {
		println("Usage: " + os.Args[0] + " EMPLOYER_ID BUCKET_NAME")
		os.Exit(1)
	}
	employerId = os.Args[1]
	bucketName = os.Args[2]
	connect()
}

// Connect to S3 using the HTTP endpoint for performance.
func connect() {
	auth, err := aws.EnvAuth()
	if err != nil {
		println("S3 connect failed due to auth issues, exiting!")
		os.Exit(1)
	}
	USEast := aws.Region{S3Endpoint: "http://s3.amazonaws.com"}
	bucket = s3.New(auth, USEast).Bucket(bucketName)
}

// Read the list of ids from standard input, validate and return them sorted
// lexicographically.
func readIds() (ids []string) {
	ids = make([]string, 0)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		_, err := strconv.Atoi(text)
		if err != nil {
			panic(err)
		}
		ids = append(ids, text)
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	sort.Sort(sort.StringSlice(ids))
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
