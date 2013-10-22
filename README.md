s3checker
=========

A tool for quickly asking the amazon S3 API which keys out of a large number
are not present.  This assumes a specific key structure:

    s3://BUCKET/CUSTOMERID/docs/DOCID.pdf

The code would need to be adapted to look for keys matching different patterns.

## Usage

    ./s3checker CUSTOMERID BUCKET < ids

where `ids` is a file containing a list of numeric document ids, one per line.
This will return on standard out any ids whose corresponding formatted keys
were not found.  Set the `DEBUG` environment variable to something to get
useful debug feedback on standard error.

## Background

This is meant to process lists of several hundred thousand keys, returning any
that are not present.  It's very inefficient and prohibitively slow to do this
by checking each key individually, but the `ListBucket` API returns 1000 keys
at a time.  If your key distribution is relatively orderly, and the list of
keys you want to check is alphabetically contiguous, the number of API calls
made can be drastically reduced.  Since this approach can be sloppy, especially
with an alphabetically sparse input list, we individually verify that every key
reported as missing during the initial sweep is actually not present.  If a
large number of input keys are not actually present, this secondary
verification step will dominate the total number of API calls.  You can disable
it with the environment variable `NO_VERIFY`.
