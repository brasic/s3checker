s3checker
=========

A tool for quickly asking the amazon S3 API which keys out of a large number
are not present.

## Usage

    ./s3checker [-v] BUCKET < keys

where `keys` is a file containing a list of S3 keys with no leading slash.

By default, keys that are not present in the bucket will be returned on
standard out, one per line.  This behavior can be inverted by using the `-v`
flag, so that only keys that are present are returned.

Set the `DEBUG` environment variable to something to get useful debug feedback
on standard error.

## Background

This is meant to process lists of several hundred thousand keys, returning any
that are not present.  It's very inefficient and prohibitively slow to do this
by checking each key individually, but the `ListBucket` API returns 1000 keys
at a time.  If your key distribution is relatively orderly, and the list of
keys you want to check is alphabetically contiguous, the number of API calls
made should be close to the minimum possible (`keys/1000`).
