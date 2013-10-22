s3checker
=========

A tool for quickly asking the amazon S3 API which keys out of a large number
are not present.  This assumes a specific key structure:

    s3://BUCKET/CUSTOMERID/docs/DOCID.pdf

The code would need to be adapted to look for keys matching different patterns.

## Usage

    ./s3checker CUSTOMERID BUCKET < ids

where `ids` is a list of document ids in the form mentioned above.  This will
return on standard out any ids whose corresponding formatted keys were not found.
Set the `DEBUG` environment variable to something to get useful debug feedback
on standard error.
