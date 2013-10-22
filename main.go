package main

import (
	"bufio"
	"fmt"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

var bucket *s3.Bucket
var bucketName string
var employerId string
var apiCount int64 = 0

const workerCount = 20

func main() {
	if len(os.Args) < 3 {
		println("Usage: " + os.Args[0] + " EMPLOYER_ID BUCKET_NAME")
		os.Exit(1)
	}
	employerId = os.Args[1]
	bucketName = os.Args[2]
	auth, err := aws.EnvAuth()
	if err != nil {
		println("S3 connect failed due to auth issues, exiting!")
		os.Exit(1)
	}
	USEast := aws.Region{S3Endpoint: "http://s3.amazonaws.com"}
	bucket = s3.New(auth, USEast).Bucket(bucketName)
	ids := readIds()
	presence := checkFiles(ids)
	notFound := keysNotFound(presence)
	debug("not found by bulk:", notFound)
	checkIndividualKeys(notFound)
	debug("Calls to S3 API:", apiCount)
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

func checkFiles(ids []string) (found map[string]bool) {
	found = make(map[string]bool)
	for i, _ := range ids {
		found[format(ids[i])] = false
	}
	allFiles := make([]s3.Key, 0)
	debug("lex. earliest key is", ids[0])
	debug("lex. last key is", ids[len(ids)-1])
	firstSearchKey := predecessor(ids[0])
	lastSearchKey := ids[len(ids)-1]
	path := fmt.Sprintf("%s/docs/", employerId)
	nextMarker := fmt.Sprintf("%s%s.pdf", path, firstSearchKey)
	for {
		debug("querying for prefix", path, "starting with", nextMarker)
		resp, err := bucket.List(path, "/", nextMarker, 1000)
		atomic.AddInt64(&apiCount, 1)
		if err != nil {
			panic(err)
		}
		if len(resp.Contents) < 1 {
			debug("got no responses.")
			break
		}
		for i, _ := range resp.Contents {
			found[resp.Contents[i].Key] = true
		}
		allFiles = append(allFiles, resp.Contents...)
		nextMarker = resp.Contents[len(resp.Contents)-1].Key
		debug("got", len(resp.Contents), "keys, ending with", nextMarker, ". Currently have", len(allFiles), "total")
		if gt(nextMarker, lastSearchKey) {
			debug("ending early,", nextMarker, "is > than our sample's largest member,", lastSearchKey)
			break
		}
		if !resp.IsTruncated {
			debug("Ending GET loop since isTruncated == false")
			break
		}
	}

	debug("Done getting", len(allFiles), "keys")
	return found
}

// Return the keys that were not found given a presence map.
func keysNotFound(presence map[string]bool) (notFound []string) {
	notFound = make([]string, 0)
	for key, present := range presence {
		if !present {
			notFound = append(notFound, key)
		}
	}
	return
}

func debug(args ...interface{}) {
	if os.Getenv("DEBUG") != "" {
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

// A verification function to check a small list of keys for presence.  Meant
// to validate the final list of 'missing' keys to ensure they are all actually
// missing and that our bulk checker didn't mistakenly flag any.
func checkIndividualKeys(keys []string) {
	work := make(chan string)
	quit := make(chan int)
	go startCheckers(work, quit)
	for i, _ := range keys {
		work <- keys[i]
	}

	// Notify the workers that there is no more work and wait for them to ACK.
	for i := 0; i < workerCount; i++ {
		work <- "__DONE__"
	}
	for i := 0; i < workerCount; i++ {
		<-quit
	}

}

func startCheckers(work <-chan string, quit chan<- int) {
	for i := 0; i < workerCount; i++ {
		go func() {
			for {
				item := <-work
				if item == "__DONE__" {
					quit <- 1
					break
				}
				resp, err := bucket.List(item, "", "", 1)
				atomic.AddInt64(&apiCount, 1)
				if err != nil {
					fmt.Println(err)
				}
				if len(resp.Contents) < 1 {
					fmt.Println(deformat(item))
				} else {
					debug("WARNING: bulkChecker said", item, "didn't exist,",
						"but it was found by individualChecker")
				}
			}
		}()
	}
}
