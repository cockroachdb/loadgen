#!/bin/bash
# Build the load generators.

set -eux

for i in blocks ycsb; do
  time make STATIC=1 "${i}"
  strip -S "${i}"/"${i}"
done
