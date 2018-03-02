#!/usr/bin/env bash

set -euxo pipefail

docker run \
    --workdir=/go/src/github.com/cockroachdb/loadgen \
    --volume="${GOPATH%%:*}/src":/go/src \
    --volume="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)":/go/src/github.com/cockroachdb/loadgen \
    --volume="/tmp/loadgenbin":/go/src/github.com/cockroachdb/loadgen/bin \
    --rm \
    cockroachdb/builder:20180220-200046 make all | go-test-teamcity
