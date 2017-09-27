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
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/pkg/errors"
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

// Keep this in the same order as the const type enum above, since it's used as a map from tx type
// to struct.
var txs = [...]tx{
	newOrderType:    {tpccTx: newOrder{}, name: "newOrder"},
	paymentType:     {tpccTx: payment{}, name: "payment"},
	orderStatusType: {tpccTx: orderStatus{}, name: "orderStatus"},
	deliveryType:    {tpccTx: delivery{}, name: "delivery"},
	stockLevelType:  {tpccTx: stockLevel{}, name: "stockLevel"},
}

var totalWeight int

func initializeMix() {
	nameToTx := make(map[string]txType)
	for i, tx := range txs {
		nameToTx[tx.name] = txType(i)
	}

	items := strings.Split(*mix, ",")
	for _, item := range items {
		kv := strings.Split(item, "=")
		if len(kv) != 2 {
			log.Fatalf("Invalid mix %s: %s is not a k=v pair", *mix, item)
		}
		txName, weightStr := kv[0], kv[1]

		weight, err := strconv.Atoi(weightStr)
		if err != nil {
			log.Fatalf("Invalid percentage mix %s: %s is not an integer", *mix, weightStr)
		}

		txIdx, ok := nameToTx[txName]
		if !ok {
			log.Fatalf("Invalid percentage mix %s: no such transaction %s", *mix, txName)
		}

		txs[txIdx].weight = weight
		totalWeight += weight
	}
	if *verbose {
		scaleFactor := 100.0 / float64(totalWeight)

		fmt.Printf("Running with mix ")
		for _, tx := range txs {
			fmt.Printf("%s=%.0f%% ", tx.name, float64(tx.weight)*scaleFactor)
		}
		fmt.Printf("\n")
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
		transactionType := rand.Intn(totalWeight)
		weightSum := 0
		var i int
		var t tx
		for i, t = range txs {
			weightSum += t.weight
			if transactionType < weightSum {
				break
			}
		}
		if _, err := t.run(w.db, rand.Intn(*warehouses)); err != nil {
			errCh <- errors.Wrapf(err, "error in %s", t.name)
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
