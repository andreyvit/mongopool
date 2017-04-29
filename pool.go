// Package mongopool adds a concurrency limit and automatic reconnection to mgo sessions.
package mongopool

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/mgo.v2"
)

// ErrNotConfigured is returned when the pool is created with an empty URI.
var ErrNotConfigured = errors.New("MongoDB connection has not been configured")

// Printfer allows for any logger that implement Printf
type Printfer interface {
	Printf(format string, v ...interface{})
}

type Options struct {
	// Concurrency determines the number of MongoDB sessions that may be active at the same time. Defaults to 10.
	Concurrency int

	// Set Logger to obtain verbose output from the pool.
	Logger Printfer

	// DebugName is an arbitrary string that identifies this pool for debugging purposes.
	DebugName string

	// If Lazy is true, the pool won't try to connect to MongoDB until the first call to Acquire.
	Lazy bool

	// PingInterval defines how often the connection is verified to be alive when Acquire is called. The ping is skipped if it has already been performed within PingInterval. If the ping fails, the connection is reestablished. Defaults to 10 seconds.
	PingInterval time.Duration

	// If the last connection attempt has failed, will attempt to reconnect after ReconnectInterval. Defaults to 10 seconds.
	ReconnectInterval time.Duration

	// A function to configure a newly established MongoDB session.
	Configure func(sess *mgo.Session)
}

type Pool struct {
	uri  string
	opt  Options
	name string

	obtainc chan<- chan<- connection
	closec  chan struct{}

	mut    sync.Mutex
	cond   *sync.Cond
	closed bool
	free   []*mgo.Database
	nfree  int
	total  int
	max    int
	hits   int
	waits  int

	masterState uint32 // 0 disconnected, 1 connected, access via atomic operations only
}

// Error wraps errors returned by the pool
type Error struct {
	Msg        string
	Underlying error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %v", e.Msg, e.Underlying)
}

type connection struct {
	session *mgo.Session
	err     error
}

// Dial creates a new pool that connects to MongoDB using the specified connection string.
//
// Dial does not block and never fails. It will start connecting in background, and will keep trying to reconnect (with Options.ReconnectInterval intervals) if the initial connection or a subsequent ping fails. You can check IsOnline() to see
//
// If you pass an empty URI to Dial, Acquire will always return ErrNotConfigured. This can be useful if Mongo functionality is optional in your app.
//
// Provide Options.Configure func to customize safety, batching and timeout options of the established session.
//
// Options.Concurrency specifies the limit on the concurrent use of Mongo sessions. The pool will maintain this many sessions via mgo.Session.Copy(). Concurrency defaults to 10.
//
// Before returning a session, the pool will check that it hasn't timed out by calling Session.Ping. The pings are performed only once per Options.PingInterval.
//
func Dial(uri string, opt Options) *Pool {
	if opt.Concurrency == 0 {
		opt.Concurrency = 10
	}
	if opt.PingInterval == 0 {
		opt.PingInterval = 10 * time.Second
	}
	if opt.ReconnectInterval == 0 {
		opt.ReconnectInterval = 10 * time.Second
	}

	obtainc := make(chan chan<- connection, 1)

	pool := &Pool{
		uri:     uri,
		opt:     opt,
		closec:  make(chan struct{}),
		obtainc: obtainc,
		free:    make([]*mgo.Database, opt.Concurrency),
	}
	pool.cond = sync.NewCond(&pool.mut)
	if opt.DebugName != "" {
		pool.name = opt.DebugName
	} else {
		pool.name = fmt.Sprintf("<Pool %p>", pool)
	}

	go pool.dialer(obtainc)

	if !pool.opt.Lazy {
		pool.obtainc <- nil
	}

	return pool
}

// String returns the name of this pool.
func (pool *Pool) String() string {
	return pool.name
}

// StatusString returns a short description of the pool status.
func (pool *Pool) StatusString() string {
	var masterState string
	if pool.IsOnline() {
		masterState = "connected"
	} else {
		masterState = "disconnected"
	}

	return fmt.Sprintf("%s, free=%d, total=%d, max=%d", masterState, pool.nfree, pool.total, pool.max)
}

// IsOnline returns whether the pool currently has a functional MongoDB connection.
func (pool *Pool) IsOnline() bool {
	return atomic.LoadUint32(&pool.masterState) == 1
}

// Close shuts down the pool and close any outstanding free connections.
func (pool *Pool) Close() {
	pool.mut.Lock()
	defer pool.mut.Unlock()

	pool.closed = true
	pool.destroyAll()
}

func (pool *Pool) destroyAll() {
	for i, db := range pool.free {
		if db != nil {
			pool.free[i] = nil
			pool.nfree--
			pool.destroy(db)
		}
	}
}

/*
	Acquire obtains a new session from the pool, establishing one if necessary. If Concurrency sessions are already in use, Acquire blocks until other goroutine calls Release.

	The caller must subsequently call Release with the return value of this method, for example:

		db, err := pool.Acquire()
		if err != nil {
			return err
		}
		defer pool.Release(db)
*/
func (pool *Pool) Acquire() (*mgo.Database, error) {
	// obtain a connection before locking pool.mut, so that we don't stay locked for the entire duration of mgo.Dial
	conn := pool.obtainMaster()
	if conn.err != nil {
		if pool.opt.Logger != nil {
			pool.opt.Logger.Printf("mongopool %s: Acquire failed because the master connection could not be established", pool)
		}
		return nil, conn.err
	}

	pool.mut.Lock()
	defer pool.mut.Unlock()

	if pool.closed {
		panic("cannot acquire from a closed pool")
	}

	if db := pool.dequeue(); db != nil {
		pool.hits++
		if pool.opt.Logger != nil {
			pool.opt.Logger.Printf("mongopool %s: acquired connection from free list (%p), %s", pool, db, pool.StatusString())
		}
		return db, nil
	}

	if pool.total < pool.opt.Concurrency {
		db := conn.session.Copy().DB("")

		pool.total++
		if pool.total > pool.max {
			pool.max = pool.total
		}

		if pool.opt.Logger != nil {
			pool.opt.Logger.Printf("mongopool %s: acquired new connection (%p), %s", pool, db, pool.StatusString())
		}
		return db, nil
	}

	pool.waits++
	for {
		pool.cond.Wait()
		if db := pool.dequeue(); db != nil {
			pool.opt.Logger.Printf("mongopool %s: acquired connection from free list with waiting (%p), %s", pool, db, pool.StatusString())
			return db, nil
		}
	}
}

// Release returns the given session into the pool's free list.
func (pool *Pool) Release(db *mgo.Database) {
	pool.mut.Lock()
	defer pool.mut.Unlock()

	db.Session.Refresh()

	if !pool.enqueue(db) {
		pool.destroy(db)
		return
	}

	if pool.opt.Logger != nil {
		pool.opt.Logger.Printf("mongopool %s: released connection to free list (%p), %s", pool, db, pool.StatusString())
	}

	pool.cond.Signal()
}

// Exec is a tiny wrapper around Acquire and Release. It is recommended to use Acquire and Release directly; please only use Exec if it makes the code easier to read in your case.
func (pool *Pool) Exec(f func(db *mgo.Database) error) error {
	db, err := pool.Acquire()
	if err != nil {
		return err
	}
	defer pool.Release(db)
	return f(db)
}

func (pool *Pool) destroy(db *mgo.Database) {
	if pool.opt.Logger != nil {
		pool.opt.Logger.Printf("mongopool %s: closed connection (%p), %s", pool, db, pool.StatusString())
	}

	if pool.total <= 0 {
		panic("internal error")
	}
	pool.total--

	db.Session.Close()
}

func (pool *Pool) enqueue(db *mgo.Database) bool {
	if pool.closed {
		return false
	}
	for i := range pool.free {
		if pool.free[i] == nil {
			pool.free[i] = db
			pool.nfree++
			return true
		}
	}
	return false
}

func (pool *Pool) dequeue() *mgo.Database {
	if pool.nfree == 0 {
		return nil
	}
	for i, db := range pool.free {
		if db != nil {
			pool.free[i] = nil
			pool.nfree--
			return db
		}
	}
	panic("internal error: nfree invalid")
}

func (pool *Pool) obtainMaster() connection {
	resc := make(chan connection, 1)
	pool.obtainc <- resc
	return <-resc
}

func (pool *Pool) dialer(obtainc <-chan chan<- connection) {
	var master *mgo.Session
	var masterErr error
	var lastPing time.Time
	var dialtm time.Time

	for {
		resc, ok := <-obtainc
		if !ok {
			break
		}

		now := time.Now()

		// Check that the session is alive, unless we've already checked within PingInterval.
		if master != nil && (lastPing.IsZero() || now.Sub(lastPing) >= pool.opt.PingInterval) {
			masterErr = master.Ping()
			lastPing = now
			if masterErr != nil {
				master.Refresh() // reconnect on next attempt
				masterErr = &Error{"mongopool disconnected", masterErr}
			}
			if masterErr == nil {
				atomic.StoreUint32(&pool.masterState, 1)
			} else {
				atomic.StoreUint32(&pool.masterState, 0)
			}
		}

		if master == nil && (dialtm.IsZero() || now.Sub(dialtm) >= pool.opt.ReconnectInterval) {
			if pool.uri != "" {
				master, masterErr = mgo.Dial(pool.uri)
				if masterErr != nil {
					masterErr = &Error{"mongopool failed to connect", masterErr}
				}
			} else {
				master, masterErr = nil, ErrNotConfigured
			}
			dialtm = time.Now()
			if master != nil && pool.opt.Configure != nil {
				pool.opt.Configure(master)
			}
			if masterErr == nil {
				atomic.StoreUint32(&pool.masterState, 1)
			} else {
				atomic.StoreUint32(&pool.masterState, 0)
			}
		}

		if resc != nil {
			resc <- connection{master, masterErr}
		}
	}
}
