# CockroachDB load generators

This repository contains a few load generators for load testing
CockroachDB (and on occasion, other databases).

# Building

`make build` will build all load generators. The binaries can be found
in the individual directories (`kv/kv`, `tpcc/tpcc`,`'tpch/tpch`,
`ycsb/ycsb`).

# Running

By default, the load generators will run against a local cockroach
cluster running on the default port, but all load generators accept
custom flags. To see the full list of options, run `-h` on each
individual load generator. While they share common options, each one
is different.


# Which load generator should I use?

The different load generators test different aspects of
CockroachDB. If you have a specific use case in mind, one of them
might be more suited for understanding CockroachDB's performance
characteristics. Do note that some load generators have multiple
schemas/ways of using them, but this is a rough overview to get started.

## KV

KV is a key-value load generator that reads and writes to random keys
spread uniformly at random across the cluster. It is useful for
getting an understanding of ideal goodput if your schema was maximally
parallelizable. The `--read-percent` flag affects the frequency of
reads versus writes in the system.


## YCSB

YCSB is a key-value load generator that reads from keys using a Zipf
distribution which makes some keys exponentially more likely to be
read from than others. This simulates internet style workloads where
some keys are "hot". The hot keys are spread uniformly across the
cluster. Writes are spread evenly across the system.

See https://github.com/brianfrankcooper/YCSB for more information on
the YCSB benchmark.


## TPC-C

TPC-C is a load generator that simulates a transaction processing workload
using a rich schema of multiple tables, and many workers
simultaneously running different transactions, running against an
online store (the transactions involve actions such as checking out items,
adding items to cart, adding more inventory of an item to a
warehouse). These transactions contend against each other, and require
more complex SQL features than YCSB or
KV. The [tpcc specification](http://www.tpc.org/tpcc) has a detailed
explanation of the workload.

## TPC-H

TPC-H is a load generator that simulates an analytics workload using a
worklod of multiple tables. TPC-H is a read-only workload, using 22
specific queries (only a subset of which currently run on
CockroachDB). The data must first be generated using [an
external tool](https://github.com/electrum/tpch-dbgen), and then
loaded into CockroachDB.

The load generator can generate different "scalefactors" of data:
scalefactor-1 is roughly 1gb in size of CSVs (translating into about
~1.7gb in CockroachDB counting denormalizations), and scales linearly
for larger scalefactors.


## Rand

Rand generates random data andm inserts it into CockroachDB. It
introspects a given database table for its schema and generates data
to fit that schema, so you must first manually create a database and
table inside CockroachDB with the schema you want.
