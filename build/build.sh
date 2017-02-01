#!/bin/bash
# Build the load generators.

set -eux

PROJECTS=( blocks ycsb )

for i in "${PROJECTS[@]}"; do
  time make STATIC=1 "${i}"
  strip -S "${i}"/"${i}"
done
