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
	"strconv"
	"strings"
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
		byOp []*hdrhistogram.WindowedHistogram
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

type txType int

const (
	newOrderType txType = iota
	paymentType
	orderStatusType
	deliveryType
	stockLevelType
)
const nTxTypes = 5

type tpccTx interface {
	run(db *sql.DB, wID int) (interface{}, error)
}

type tx struct {
	tpccTx
	weight int    // percent likelihood that each transaction type is run
	name   string // display name
	numOps uint64
}

// The weights should add to 100. They match the TPCC spec - so probably don't
// tune these. Keep this in the same order as the const type enum above, since
// it's used as a map from tx type to struct.
var txs = [...]tx{
	newOrderType:    {tpccTx: newOrder{}, weight: 45, name: "tpmC"},
	paymentType:     {tpccTx: payment{}, weight: 43, name: "payment"},
	orderStatusType: {tpccTx: orderStatus{}, weight: 4, name: "orderStatus"},
	deliveryType:    {tpccTx: delivery{}, weight: 4, name: "delivery"},
	stockLevelType:  {tpccTx: stockLevel{}, weight: 4, name: "stockLevel"},
}

func initializeMix() {
	if *overrideMix != "" {
		percentStrs := strings.Split(*overrideMix, ",")
		if len(percentStrs) != 5 {
			log.Fatalf("Invalid percentage mix %s: need 5 percentages", percentStrs)
		}
		totalWeight := 0
		for i, str := range percentStrs {
			weight, err := strconv.Atoi(str)
			if err != nil {
				log.Fatalf("Invalid percentage mix %s: %s is not an integer", percentStrs, str)
			}
			txs[i].weight = weight
			totalWeight += weight
		}
		if totalWeight != 100 {
			log.Fatalf("Invalid percentage mix %s: didn't add up to 100", percentStrs)
		}
	}
}

func newWorker(db *sql.DB, wg *sync.WaitGroup) *worker {
	wg.Add(1)
	w := &worker{db: db}
	w.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)

	w.latency.byOp = make([]*hdrhistogram.WindowedHistogram, nTxTypes)
	for i := 0; i < nTxTypes; i++ {
		w.latency.byOp[i] = hdrhistogram.NewWindowed(1,
			minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	}
	return w
}

func (w *worker) run(errCh chan<- error, wg *sync.WaitGroup) {
	for {
		start := time.Now()
		transactionType := rand.Intn(100)
		weightTotal := 0
		var i int
		var t tx
		for i, t = range txs {
			weightTotal += t.weight
			if transactionType < weightTotal {
				break
			}
		}
		if _, err := t.run(w.db, rand.Intn(*warehouses)); err != nil {
			errCh <- err
			continue
		}
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency).Nanoseconds()
		w.latency.Lock()
		if err := w.latency.Current.RecordValue(elapsed); err != nil {
			log.Fatal(err)
		}
		if err := w.latency.byOp[i].Current.RecordValue(elapsed); err != nil {
			log.Fatal(err)
		}
		w.latency.Unlock()
		atomic.AddUint64(&txs[i].numOps, 1)
		v := atomic.AddUint64(&numOps, 1)
		if *maxOps > 0 && v >= *maxOps {
			return
		}
	}
}
