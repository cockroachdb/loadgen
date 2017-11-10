#!/usr/bin/env bash

set -euxo pipefail

VERSION=$(git describe 2>/dev/null || git rev-parse --short HEAD)

echo "Deploying ${VERSION}..."

BUCKET_NAME=cockroach
LATEST_SUFFIX=.LATEST
REPO_NAME=loadgen
SHA=$(git rev-parse HEAD)


function aws {
  docker run --rm \
    -t \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e AWS_DEFAULT_REGION \
    -v "$(pwd):/project" \
    mesosphere/aws-cli \
    $@
}

# push_one_binary takes the path to the binary inside the repo.
# eg: push_one_binary sql/sql.test
# The file will be pushed to: s3://BUCKET_NAME/REPO_NAME/sql.test.SHA
# The binary's sha will be stored in s3://BUCKET_NAME/REPO_NAME/sql.test.LATEST
# The .LATEST file will also redirect to the latest binary when fetching through
# the S3 static-website.
function push_one_binary {
  rel_path=$1
  binary_name=$(basename "$1")

  time aws s3 cp "${rel_path}" s3://${BUCKET_NAME}/${REPO_NAME}/"${binary_name}"."${SHA}"

  # Upload LATEST file.
  latest_file="${binary_name}${LATEST_SUFFIX}"
  echo "${SHA}" > "${latest_file}"

  time aws s3 cp --website-redirect /${REPO_NAME}/"${binary_name}"."${SHA}" "${latest_file}" s3://${BUCKET_NAME}/${REPO_NAME}/"${binary_name}"${LATEST_SUFFIX}
  rm -f "${latest_file}"
}

for proj in kv ycsb tpch tpcc ; do
    docker run \
        --workdir=/go/src/github.com/cockroachdb/loadgen \
        --volume="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)":/go/src/github.com/cockroachdb/loadgen \
        --rm \
        cockroachdb/builder:20170422-212842 make ${proj} STATIC=1
    strip -S ${proj}/${proj}
    push_one_binary ${proj}/${proj}
done
