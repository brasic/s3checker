package main

import (
	"fmt"
	"launchpad.net/goamz/s3"
	"sync/atomic"
)

const maxKeysPerCall = 1000
const workerCount = 20

// Given a list of keys, query amazon for their existence 1000 at a time.  This
// is imprecise since instead of asking amazon "which of these 1000 keys do you
// have", we can only say, "give me next 1000 keys alphabetically subsequent to
// this one", then compare with the ones you are looking for.
func checkBulkKeys(ids []string) (found map[string]bool) {
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
		resp, err := bucket.List(path, "/", nextMarker, maxKeysPerCall)
		atomic.AddInt64(&apiCalls, 1)
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

// A verification function to check a small list of keys for presence.  Meant
// to validate the final list of 'missing' keys to ensure they are all actually
// missing and that our bulk checker didn't mistakenly flag any.
func checkIndividualKeys(keys []string) {
	work := make(chan string)
	quit := make(chan int)
	go startIndividualCheckers(work, quit)
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

func startIndividualCheckers(work <-chan string, quit chan<- int) {
	for i := 0; i < workerCount; i++ {
		go func() {
			for {
				item := <-work
				if item == "__DONE__" {
					quit <- 1
					break
				}
				resp, err := bucket.List(item, "", "", 1)
				atomic.AddInt64(&apiCalls, 1)
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
