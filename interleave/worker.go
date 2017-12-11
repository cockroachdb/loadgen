package main

import (
	"database/sql"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codahale/hdrhistogram"
)

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 100 * time.Second
)

// numOps keeps a global count of successful operations.
var numOps uint64

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

func newWorker(db *sql.DB, wg *sync.WaitGroup) *worker {
	wg.Add(1)
	w := &worker{db: db}
	w.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)

	return w
}

func (w *worker) run(wg *sync.WaitGroup) {
	for {
		start := time.Now()

		// Critical path.

		if _, err := w.db.Exec(*query); err != nil {
			log.Fatal(err)
		}

		elapsed := clampLatency(time.Since(start), minLatency, maxLatency).Nanoseconds()
		w.latency.Lock()
		if err := w.latency.Current.RecordValue(elapsed); err != nil {
			log.Fatal(err)
		}
		w.latency.Unlock()

		atomic.AddUint64(&numOps, 1)
	}
}
