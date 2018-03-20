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
	getTimer       metrics.Timer // Timer for measuring the database get request counts and latencies
	putTimer       metrics.Timer // Timer for measuring the database put request counts and latencies
	delTimer       metrics.Timer // Timer for measuring the database delete request counts and latencies
	missMeter      metrics.Meter // Meter for measuring the missed database get requests
	readMeter      metrics.Meter // Meter for measuring the database get request data usage
	writeMeter     metrics.Meter // Meter for measuring the database put request data usage
	batchPutTimer  		metrics.Timer
	batchWriteTimer 	metrics.Timer
	batchWriteMeter		metrics.Meter

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	
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
	
	
	return ret, nil
}

// Path returns the path to the database directory.
func (db *BoltDatabase) Path() string {
	return db.fn
}

// Put puts the given key / value to the queue
func (db *BoltDatabase) Put(key []byte, value []byte) error {
	if db.putTimer != nil {
		defer db.putTimer.UpdateSince(time.Now())
	}

	if db.writeMeter != nil {
		db.writeMeter.Mark(int64(len(value)))
	}
	
	return db.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		err := b.Put([]byte(key), value)
		return err
	})
}

func (db *BoltDatabase) Has(key []byte) (ret bool, err error) {
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


// Get returns the given key if it's present.
func (db *BoltDatabase) Get(key []byte) (dat []byte, err error) {
	// Measure the database get latency, if requested
	if db.getTimer != nil {
		defer db.getTimer.UpdateSince(time.Now())
	}
	
	
	err = db.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		value := b.Get(key)
		if value != nil {
			dat = common.CopyBytes(value)
		}
		return nil
	})
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
	err := db.db.Close()
	if err == nil {
		db.log.Info("Database closed")
	} else {
		db.log.Error("Failed to close database", "err", err)
	}
}

// Meter configures the database metrics collectors and
func (db *BoltDatabase) Meter(prefix string) {
	// Short circuit metering if the metrics system is disabled
	if !metrics.Enabled {
		return
	}
	// Initialize all the metrics collector at the requested prefix
	db.getTimer = metrics.NewRegisteredTimer(prefix+"user/gets", nil)
	db.putTimer = metrics.NewRegisteredTimer(prefix+"user/puts", nil)
	db.delTimer = metrics.NewRegisteredTimer(prefix+"user/dels", nil)
	db.missMeter = metrics.NewRegisteredMeter(prefix+"user/misses", nil)
	db.readMeter = metrics.NewRegisteredMeter(prefix+"user/reads", nil)
	db.writeMeter = metrics.NewRegisteredMeter(prefix+"user/writes", nil)
	db.batchPutTimer = metrics.NewRegisteredTimer(prefix+"user/batchPuts", nil)
	db.batchWriteTimer = metrics.NewRegisteredTimer(prefix+"user/batchWriteTimes", nil)
	db.batchWriteMeter = metrics.NewRegisteredMeter(prefix+"user/batchWrites", nil)
}


func (db *BoltDatabase) NewBatch() Batch {
	return &boltBatch{db: db, b: make(map[string][]byte)}
}

type boltBatch struct {
	db		*BoltDatabase
	b		map[string][]byte
	size int
}

func (b *boltBatch) Put(key, value []byte) error {
	if b.db.batchPutTimer != nil {
		defer b.db.batchPutTimer.UpdateSince(time.Now())
	}
	
	b.b[string(key)] = common.CopyBytes(value)
	b.size += len(value)
	return nil
}

func (b *boltBatch) Write() (err error) {
	if b.db.batchWriteTimer != nil {
		defer b.db.batchWriteTimer.UpdateSince(time.Now())
	}

	if b.db.batchWriteMeter != nil {
		b.db.batchWriteMeter.Mark(int64(b.size))
	}
	
	err = b.db.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("MyBucket"))
		
		for key, value := range b.b {
			err = bucket.Put([]byte(key), value)
		}

		return err
	})
	
	b.size = 0
	b.b = make(map[string][]byte)
	return err
}

func (b *boltBatch) Discard() {
	b.b = make(map[string][]byte)
	b.size = 0
}

func (b *boltBatch) ValueSize() int {
	return b.size
}

func (b *boltBatch) Reset() {
	b.b = make(map[string][]byte)
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