package main

import (
	"fmt"
	"launchpad.net/goamz/s3"
	"path"
	"sync/atomic"
)

const maxKeysPerCall = 1000

var workerCount = 20

// Given a list of keys, query amazon for their existence 1000 at a time.  This
// is imprecise since instead of asking amazon "which of these 1000 keys do you
// have", we can only say, "give me next 1000 keys alphabetically subsequent to
// this one", then compare with the ones you are looking for.
func checkBulkKeys(keys []string) (found map[string]bool) {
	found = make(map[string]bool)
	for i, _ := range keys {
		found[keys[i]] = false
	}
	allFiles := make([]s3.Key, 0)
	debug("lex. earliest key is", keys[0])
	debug("lex. last key is", keys[len(keys)-1])
	firstSearchKey := predecessor(keys[0])
	lastSearchKey := keys[len(keys)-1]
	prefix := path.Dir(keys[0]) + "/"
	nextMarker := firstSearchKey
	for {
		debug("querying for prefix", prefix, "starting with", nextMarker)
		resp, err := bucket.List(prefix, "/", nextMarker, maxKeysPerCall)
		atomic.AddInt64(&apiCalls, 1)
		if err != nil {
			panic(err)
		}
		if len(resp.Contents) < 1 {
			debug("got no responses.")
			break
		}
		for i, _ := range resp.Contents {
			if _, ok := found[resp.Contents[i].Key]; ok {
				debug("marking", resp.Contents[i].Key, "as found")
				found[resp.Contents[i].Key] = true
			}
		}
		allFiles = append(allFiles, resp.Contents...)
		nextMarker = resp.Contents[len(resp.Contents)-1].Key
		debug("got", len(resp.Contents), "keys, ending with", nextMarker, ". Currently have", len(allFiles), "total")
		if nextMarker > lastSearchKey {
			debug("ending early,", nextMarker, "is > than our sample's largest member,", lastSearchKey)
			break
		}
		if !resp.IsTruncated {
			debug("Ending GET loop since isTruncated == false")
			break
		}
	}

	debug("Done getting", len(allFiles), "keys")
	debug(found)
	return found
}

// A verification function to check a small list of keys for presence.  Meant
// to validate the final list of 'missing' keys to ensure they are all actually
// missing and that our bulk checker didn't mistakenly flag any.
func checkIndividualKeys(keys []string) {
	work := make(chan string)
	quit := make(chan int)
	startCheckers(work, quit)
	for i, _ := range keys {
		work <- keys[i]
	}
	notifyDone(work)
	waitForAcks(quit)
}

func startCheckers(work chan string, quit chan int) {
	for i := 0; i < workerCount; i++ {
		go checkEach(work, quit)
	}
}
func notifyDone(work chan string) {
	for i := 0; i < workerCount; i++ {
		work <- "__DONE__"
	}
}

func waitForAcks(quit chan int) {
	for i := 0; i < workerCount; i++ {
		<-quit
	}
}

func checkEach(work <-chan string, quit chan<- int) {
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
			fmt.Println(item)
		} else {
			debug("WARNING: bulkChecker said", item, "didn't exist,",
				"but it was found by individualChecker")
		}
	}
}
