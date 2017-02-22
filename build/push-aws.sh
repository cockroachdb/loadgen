#!/bin/bash
# Push binaries to AWS.
# This is run by teamcity after successful build/test.
#
# Requisites:
# - binaries must be statically linked by running build/build-static-binaries.sh
# - teamcity must have AWS credentials configured
# - AWS credentials must have S3 write permissions on the bucket
# - the aws cli must be installed on the machine
# - the region must be configured
#
# Ask marc@cockroachlabs.com for the aws credentials file to use.

set -eux

BUCKET_NAME="cockroach"
LATEST_SUFFIX=".LATEST"
REPO_NAME="loadgen"
SHA="$(git rev-parse HEAD)"
# push_one_binary takes the path to the binary inside the repo.
# eg: push_one_binary sql/sql.test
# The file will be pushed to: s3://BUCKET_NAME/REPO_NAME/sql.test.SHA
# The binary's sha will be stored in s3://BUCKET_NAME/REPO_NAME/sql.test.LATEST
# The .LATEST file will also redirect to the latest binary when fetching through
# the S3 static-website.
function push_one_binary {
  rel_path=$1
  binary_name=$(basename $1)

  cd $(dirname $0)/..
  time aws s3 cp ${rel_path} s3://${BUCKET_NAME}/${REPO_NAME}/${binary_name}.${SHA}

  # Upload LATEST file.
  tmpfile=$(mktemp /tmp/cockroach-push.XXXXXX)
  echo ${SHA} > ${tmpfile}
  time aws s3 cp --website-redirect "/${REPO_NAME}/${binary_name}.${SHA}" ${tmpfile} s3://${BUCKET_NAME}/${REPO_NAME}/${binary_name}${LATEST_SUFFIX}
  rm -f ${tmpfile}
}

for proj in kv ycsb tpch ; do
  push_one_binary ${proj}/${proj}
done
