#!/bin/bash
# Build the load generators.

set -eux

for proj in kv ycsb ; do
  time make STATIC=1 ${proj}
  strip -S ${proj}/${proj}
done
