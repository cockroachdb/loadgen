# Build the docker image only after building the kv binary with:
#   CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo .
# This builds a minimal image that even lacks root CA certs, but that's
# fine for most uses since our instructions for securely deploying
# CockroachDB involve creating a new CA cert that has to be distributed
# to clients anyways.
from scratch

MAINTAINER Alex Robinson <alex@cockroachlabs.com>

ADD kv /kv
ENTRYPOINT ["/kv"]
