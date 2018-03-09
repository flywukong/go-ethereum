// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethdb

import (
	//"strconv"
	//"strings"
	"sync"
	"time"
	
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/boltdb/bolt"
	"github.com/ethereum/go-ethereum/common"
	
)



type BoltDatabase struct {
	fn 				string      // filename for reporting
	db				*bolt.DB 
	boltCache		*BoltCache
	getTimer       metrics.Timer // Timer for measuring the database get request counts and latencies
	putTimer       metrics.Timer // Timer for measuring the database put request counts and latencies
	delTimer       metrics.Timer // Timer for measuring the database delete request counts and latencies
	missMeter      metrics.Meter // Meter for measuring the missed database get requests
	readMeter      metrics.Meter // Meter for measuring the database get request data usage
	writeMeter     metrics.Meter // Meter for measuring the database put request data usage
	compTimeMeter  metrics.Meter // Meter for measuring the total time spent in database compaction
	compReadMeter  metrics.Meter // Meter for measuring the data read during compaction
	compWriteMeter metrics.Meter // Meter for measuring the data written during compaction

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path
}

// NewLDBDatabase returns a LevelDB wrapped object.
func NewBoltDatabase(file string) (*BoltDatabase, error) {
	logger := log.New("database", file)
	

	db, err := bolt.Open(file, 0600, nil)
	// (Re)check for errors and abort if opening of the db failed
	if err != nil {
		return nil, err
	}
	ret := &BoltDatabase{
		fn:  file,
		db:  db,
		log: logger,
	}
	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte("MyBucket"))
		return nil
	})
	
	
	ret.boltCache = &BoltCache{db: ret, c: make(map[string][]byte), size: 0, limit: 50000000}
	return ret, nil
}

// Path returns the path to the database directory.
func (db *BoltDatabase) Path() string {
	return db.fn
}

// Put puts the given key / value to the queue
func (db *BoltDatabase) Put(key []byte, value []byte) error {
	// Measure the database put latency, if requested
	if db.putTimer != nil {
		defer db.putTimer.UpdateSince(time.Now())
	}
	// Generate the data to write to disk, update the meter and write
	//value = rle.Compress(value)

	if db.writeMeter != nil {
		db.writeMeter.Mark(int64(len(value)))
	}
	
	db.boltCache.lock.Lock()
	db.boltCache.c[string(key)] = common.CopyBytes(value)
	db.boltCache.size += len(value)+len(key)
	db.boltCache.lock.Unlock()
	
	if db.boltCache.size >= db.boltCache.limit {
		return db.boltCache.Flush()
	}
	
	return nil
}

func (db *BoltDatabase) Has(key []byte) (ret bool, err error) {
	db.boltCache.lock.RLock()
	defer db.boltCache.lock.RUnlock()
	if db.boltCache.c[string(key)] != nil {
		return true, nil
	}
	
	err = db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		value := b.Get(key)
		if value == nil {
			ret = false
		}else {
			ret = true
		}
		return nil
	})
	
	return ret, err
}

type BoltCache struct {
	db		*BoltDatabase
	c	 	map[string][]byte
	size	int
	limit	int
	lock 	sync.RWMutex
}

func (boltCache *BoltCache) Flush() (err error) {
	boltCache.lock.Lock()
	defer boltCache.lock.Unlock()
	log.Info("Bolt flushing starts, 100mb takes ~1m", "boltCache size", boltCache.size)
	err = boltCache.db.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		
		for key, value := range boltCache.c {
			err = b.Put([]byte(key), value)
		}
		
		
		return err
	})
	log.Info("Bolt flushed to disk", "boltCache size", boltCache.size)
	boltCache.size = 0
	boltCache.c = make(map[string][]byte)
	return err
}

// Get returns the given key if it's present.
func (db *BoltDatabase) Get(key []byte) (dat []byte, err error) {
	// Measure the database get latency, if requested
	if db.getTimer != nil {
		defer db.getTimer.UpdateSince(time.Now())
	}
	
	
	db.boltCache.lock.RLock()
	dat = db.boltCache.c[string(key)]
	db.boltCache.lock.RUnlock()
	
	if dat == nil {
		err = db.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("MyBucket"))
			value := b.Get(key)
			if value != nil {
				dat = common.CopyBytes(value)
			}
			return nil
		})
	}
	if err != nil {
		if db.missMeter != nil {
			db.missMeter.Mark(1)
		}
		return nil, err
	}
	//Update the actually retrieved amount of data
	if db.readMeter != nil {
		db.readMeter.Mark(int64(len(dat)))
	}
	return dat, nil
	//return rle.Decompress(dat)
}

// Delete deletes the key from the queue and database
func (db *BoltDatabase) Delete(key []byte) error {
	// Measure the database delete latency, if requested
	if db.delTimer != nil {
		defer db.delTimer.UpdateSince(time.Now())
	}
	// Execute the actual operation
	db.boltCache.lock.Lock()
	delete(db.boltCache.c, string(key))
	
	//TODO: also subtract len(value)
	db.boltCache.size-=len(key)
	db.boltCache.lock.Unlock()
	
	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		err := b.Delete(key)
		return err
	})
}

type boltIterator struct {
	txn 				*bolt.Tx
	internIterator		*bolt.Cursor
	released			bool
	initialised			bool
	currentKey			[]byte
	currentValue		[]byte
}

func (it *boltIterator) Release() {
	it.txn.Commit()
	it.released = true
	it.currentKey, it.currentValue = nil, nil
}

func (it *boltIterator) Released() bool {
	return it.released
}

func (it *boltIterator) Next() bool {
	if(!it.initialised) {
		it.currentKey, it.currentValue = it.internIterator.First()
		it.initialised = true
	} else {
		it.currentKey, it.currentValue = it.internIterator.Next()
	}
	if it.currentKey != nil {
		return true
	}else {
		return false
	}
}

func (it *boltIterator) Seek(key []byte) {
	it.currentKey, it.currentValue = it.internIterator.Seek(key)
}

func (it *boltIterator) Key() []byte {
	return it.currentKey
}

func (it *boltIterator) Value() []byte {
	return it.currentValue
}

func (db *BoltDatabase) NewIterator() boltIterator {
	txn, _ := db.db.Begin(false)
	b := txn.Bucket([]byte("MyBucket"))
	internIterator := b.Cursor()
	return boltIterator{txn: txn, internIterator: internIterator, released: false, initialised: false}
}

func (db *BoltDatabase) Close() {
	// Stop the metrics collection to avoid internal database races
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.quitChan != nil {
		errc := make(chan error)
		db.quitChan <- errc
		if err := <-errc; err != nil {
			db.log.Error("Metrics collection failed", "err", err)
		}
	}
	db.boltCache.Flush()
	err := db.db.Close()
	if err == nil {
		db.log.Info("Database closed")
	} else {
		db.log.Error("Failed to close database", "err", err)
	}
}

/*
// Meter configures the database metrics collectors and
func (db *BadgerDatabase) Meter(prefix string) {
	// Short circuit metering if the metrics system is disabled
	if !metrics.Enabled {
		return
	}
	// Initialize all the metrics collector at the requested prefix
	db.getTimer = metrics.NewTimer(prefix + "user/gets")
	db.putTimer = metrics.NewTimer(prefix + "user/puts")
	db.delTimer = metrics.NewTimer(prefix + "user/dels")
	db.missMeter = metrics.NewMeter(prefix + "user/misses")
	db.readMeter = metrics.NewMeter(prefix + "user/reads")
	db.writeMeter = metrics.NewMeter(prefix + "user/writes")
	db.compTimeMeter = metrics.NewMeter(prefix + "compact/time")
	db.compReadMeter = metrics.NewMeter(prefix + "compact/input")
	db.compWriteMeter = metrics.NewMeter(prefix + "compact/output")

	// Create a quit channel for the periodic collector and run it
	db.quitLock.Lock()
	db.quitChan = make(chan chan error)
	db.quitLock.Unlock()

	go db.meter(3 * time.Second)
}
*/

/*
func (db *BadgerDatabase) meter(refresh time.Duration) {
	// Create the counters to store current and previous values
	counters := make([][]float64, 2)
	for i := 0; i < 2; i++ {
		counters[i] = make([]float64, 3)
	}
	// Iterate ad infinitum and collect the stats
	for i := 1; ; i++ {
		// Retrieve the database stats
		stats, err := db.db.GetProperty("leveldb.stats")
		if err != nil {
			db.log.Error("Failed to read database stats", "err", err)
			return
		}
		// Find the compaction table, skip the header
		lines := strings.Split(stats, "\n")
		for len(lines) > 0 && strings.TrimSpace(lines[0]) != "Compactions" {
			lines = lines[1:]
		}
		if len(lines) <= 3 {
			db.log.Error("Compaction table not found")
			return
		}
		lines = lines[3:]

		// Iterate over all the table rows, and accumulate the entries
		for j := 0; j < len(counters[i%2]); j++ {
			counters[i%2][j] = 0
		}
		for _, line := range lines {
			parts := strings.Split(line, "|")
			if len(parts) != 6 {
				break
			}
			for idx, counter := range parts[3:] {
				value, err := strconv.ParseFloat(strings.TrimSpace(counter), 64)
				if err != nil {
					db.log.Error("Compaction entry parsing failed", "err", err)
					return
				}
				counters[i%2][idx] += value
			}
		}
		// Update all the requested meters
		if db.compTimeMeter != nil {
			db.compTimeMeter.Mark(int64((counters[i%2][0] - counters[(i-1)%2][0]) * 1000 * 1000 * 1000))
		}
		if db.compReadMeter != nil {
			db.compReadMeter.Mark(int64((counters[i%2][1] - counters[(i-1)%2][1]) * 1024 * 1024))
		}
		if db.compWriteMeter != nil {
			db.compWriteMeter.Mark(int64((counters[i%2][2] - counters[(i-1)%2][2]) * 1024 * 1024))
		}
		// Sleep a bit, then repeat the stats collection
		select {
		case errc := <-db.quitChan:
			// Quit requesting, stop hammering the database
			errc <- nil
			return

		case <-time.After(refresh):
			// Timeout, gather a new set of stats
		}
	}
}
*/
func (db *BoltDatabase) NewBatch() Batch {
	return &boltBatch{db: db}
}

type boltBatch struct {
	db		*BoltDatabase
	size int
}

func (b *boltBatch) Put(key, value []byte) error {
	b.db.boltCache.lock.Lock()
	b.db.boltCache.c[string(key)] = common.CopyBytes(value)
	b.db.boltCache.size += len(value)+len(key)
	b.db.boltCache.lock.Unlock()
	if b.db.boltCache.size >= b.db.boltCache.limit {
		b.db.boltCache.Flush()
	}
	b.size += len(value)
	return nil
}

func (b *boltBatch) Write() error {
	b.size = 0
	if b.db.boltCache.size >= b.db.boltCache.limit {
		return b.db.boltCache.Flush()
	}
	return nil
}

func (b *boltBatch) Discard() {
	b.size = 0
}

func (b *boltBatch) ValueSize() int {
	return b.size
}

func (b *boltBatch) Reset() {
	b.size = 0
}

type table struct {
	db     Database
	prefix string
}

// NewTable returns a Database object that prefixes all keys with a given
// string.
func NewTable(db Database, prefix string) Database {
	return &table{
		db:     db,
		prefix: prefix,
	}
}

func (dt *table) Put(key []byte, value []byte) error {
	return dt.db.Put(append([]byte(dt.prefix), key...), value)
}

func (dt *table) Has(key []byte) (bool, error) {
	return dt.db.Has(append([]byte(dt.prefix), key...))
}

func (dt *table) Get(key []byte) ([]byte, error) {
	return dt.db.Get(append([]byte(dt.prefix), key...))
}

func (dt *table) Delete(key []byte) error {
	return dt.db.Delete(append([]byte(dt.prefix), key...))
}

func (dt *table) Close() {
	// Do nothing; don't close the underlying DB.
}

type tableBatch struct {
	batch  Batch
	prefix string
}

// NewTableBatch returns a Batch object which prefixes all keys with a given string.
func NewTableBatch(db Database, prefix string) Batch {
	return &tableBatch{db.NewBatch(), prefix}
}

func (dt *table) NewBatch() Batch {
	return &tableBatch{dt.db.NewBatch(), dt.prefix}
}

func (tb *tableBatch) Put(key, value []byte) error {
	return tb.batch.Put(append([]byte(tb.prefix), key...), value)
}

func (tb *tableBatch) Write() error {
	return tb.batch.Write()
}

func (tb *tableBatch) ValueSize() int {
	return tb.batch.ValueSize()
}

func (tb *tableBatch) Reset() {
	tb.batch.Reset()
}