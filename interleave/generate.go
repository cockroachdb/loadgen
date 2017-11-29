package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
)

const (
	textChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	batchSize = 500
	textLen   = 42
	intMax    = 42
	decMax    = 42
)

func generateData(db *sql.DB) error {
	// Insert pseudo-random data into tables.
	stderr.Println("Inserting into tables, this may take a while...")
	var wg sync.WaitGroup
	for _, table := range tables {
		writer := newWriter(db, table, &wg)
		go writer.insertData(&wg)
	}

	wg.Wait()

	stderr.Println("Data insertion done.")

	return nil
}

type tableWriter struct {
	pkey  int
	db    *sql.DB
	table tableName
	rnd   *rand.Rand

	columnTypes []columnType

	// Updated per insertion.
	// Scratch space for each individual value.
	scratch []byte
	// Buffer to hold the values generated to append to insert statements.
	valuesBuf []byte
}

func newWriter(db *sql.DB, table tableName, wg *sync.WaitGroup) tableWriter {
	wg.Add(1)
	w := tableWriter{
		db:    db,
		table: table,
	}

	// Generate from the same pseudo-random seed for each table to ensure
	// deterministic results even with concurrency.
	w.rnd = rand.New(rand.NewSource(*seed))
	// Primary key starts at 1.
	w.pkey = 1
	w.scratch = make([]byte, 0, textLen+2)
	w.columnTypes = tableTypes[w.table]
	return w
}

// Meant to be concurrently executed.
func (tw *tableWriter) insertData(wg *sync.WaitGroup) {
	remainingRows := tableRows[tw.table]
	stderr.Printf("Inserting %d rows into table <%s>\n", remainingRows, tw.table)
	curBatch := batchSize
	for remainingRows > 0 {
		if remainingRows < curBatch {
			curBatch = remainingRows
		}

		// Generate a batch of values to insert.
		tw.valuesBuf = tw.genValues(curBatch)
		// Form insert statement and execute.
		if _, err := tw.db.Exec(fmt.Sprintf("%s %s;", insertStmts[tw.table], string(tw.valuesBuf))); err != nil {
			log.Fatal(err)
		}

		remainingRows -= curBatch
	}

	stderr.Printf("Insertion of %d rows into <%s> completed.\n", tableRows[tw.table], tw.table)
	wg.Add(-1)
}

func (tw *tableWriter) genValues(nTuples int) []byte {
	values := tw.valuesBuf[:0]
	// Generate N tuples of values.
	for i := 0; i < nTuples; i++ {
		values = append(values, '(')
		// Generate a tuple of values.
		for i, c := range tw.columnTypes {
			var temp []byte
			switch c {
			case pkeyIntType:
				temp = []byte(strconv.Itoa(tw.pkey))
			case fkeyIntType:
				temp = tw.fKeyInt(i)
			case intType:
				temp = tw.randInt()
			case textType:
				temp = tw.randText()
			case decType:
				temp = tw.randDec()
			default:
				panic("undefined column type")
			}
			values = append(values, temp...)
			values = append(values, ',')
		}
		tw.pkey++

		// Omit the last comma.
		values = values[:len(values)-1]
		values = append(values, ')')
		values = append(values, ',')
	}

	// Omit the last comma.
	return values[:len(values)-1]
}

func (tw *tableWriter) fKeyInt(cidx int) []byte {
	var fkey int
	// Foreign keys are calculated as follows:
	// For a given table grandchild with ancestors
	//    parent
	//	child
	// We first map its id to child_id by modding it by nChild (# of child
	// rows).
	// Specifically, we take
	//    child_id = (id - 1) % nChild + 1
	// This -1, +1 shifting is necessary such that no foreign IDs are 0.
	// For parent_id, we compute it be chaining the mods
	//    parent_id = (child_id - 1) % nParent + 1
	// which reduces to
	//    parent_id = (id - 1) % nChild % nParent + 1
	switch tw.table {
	case productTable, storeTable:
		// Foreign key for merchant ID.
		fkey = (tw.pkey-1)%(*nMerchants) + 1
	case variantTable:
		if cidx == 0 {
			// Foreign key for merchant ID.
			fkey = (tw.pkey-1)%(*nProducts)%(*nMerchants) + 1
		} else if cidx == 1 {
			// Foreign key for product ID.
			fkey = (tw.pkey-1)%(*nProducts) + 1
		} else {
			panic("invalid fkey column index")
		}
	default:
		panic("unsupported table for fkey generation")
	}

	return []byte(strconv.Itoa(fkey))
}

func (tw *tableWriter) randInt() []byte {
	return []byte(strconv.Itoa(tw.rnd.Intn(intMax)))
}

func (tw *tableWriter) randText() []byte {
	scratch := tw.scratch[:0]
	scratch = append(scratch, '\'')
	for i := 0; i < textLen; i++ {
		scratch = append(scratch, textChars[tw.rnd.Intn(len(textChars))])
	}
	scratch = append(scratch, '\'')
	return scratch
}

func (tw *tableWriter) randDec() []byte {
	return []byte(fmt.Sprintf("%.2f", tw.rnd.Float64()*decMax))
}
