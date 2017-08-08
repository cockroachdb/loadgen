// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"database/sql"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codahale/hdrhistogram"
)

type worker struct {
	db      *sql.DB
	latency struct {
		sync.Mutex
		*hdrhistogram.WindowedHistogram
	}
}

func clampLatency(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

// These should add to 100. They're percent likelihoods that each transaction
// type is run. They match the TPCC spec - so probably don't tune these.
const (
	newOrderWeight    = 45
	paymentWeight     = 43
	orderStatusWeight = 4
	deliveryWeight    = 4
	stockLevelWeight  = 4
)

type tpccTx interface {
	run(db *sql.DB, w_id int) (interface{}, error)
	weight() int
}

var txs = []tpccTx{
	orderStatus{},
	newOrder{}, // newOrder must come last as its the default.
}

func (w *worker) run(errCh chan<- error, wg *sync.WaitGroup) {
	for {
		start := time.Now()
		transactionType := rand.Intn(100)
		weightTotal := 0
		var tx tpccTx
		for i := range txs {
			tx = txs[i]
			weightTotal += tx.weight()
			if transactionType < weightTotal {
				break
			}
		}
		if _, err := tx.run(w.db, rand.Intn(*warehouses)); err != nil {
			errCh <- err
			continue
		}
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency).Nanoseconds()
		w.latency.Lock()
		if err := w.latency.Current.RecordValue(elapsed); err != nil {
			log.Fatal(err)
		}
		w.latency.Unlock()
		v := atomic.AddUint64(&numOps, 1)
		if *maxOps > 0 && v >= *maxOps {
			return
		}
	}
}
