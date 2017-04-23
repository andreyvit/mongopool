// mongopool provides a concurrency limit and automatic reconnection for mgo
package mongopool

import (
	"log"
	"sync"
	"time"

	"github.com/andreyvit/sem"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
)

type Config struct {
	URI               string
	DatabaseName      string
	ReconnectInterval time.Duration
	ConcurrencyLimit  int
	Configure         func(sess *mgo.Session)
}

type Pool struct {
	Config

	closed    bool
	connected chan bool

	mut sync.RWMutex
	se  sem.Sem

	db *mgo.Database
}

func New(config Config) *Pool {
	if config.ConcurrencyLimit == 0 {
		config.ConcurrencyLimit = 1
	}
	pool := &Pool{
		Config: config,
		se:     sem.New(config.ConcurrencyLimit),
	}

	if pool.ReconnectInterval > 0 {
		// in case we ever connect in Reconnect, reserve 2 slots here
		pool.connected = make(chan bool, 2)

		go func() {
			var deadline <-chan time.Time

			for {
				select {
				case <-deadline:
					pool.Reconnect()
				case c, ok := <-pool.connected:
					if !ok {
						return
					} else if c {
						deadline = time.After(pool.ReconnectInterval)
					} else {
						deadline = nil
					}
				}
			}
		}()
	}

	return pool
}

func (pool *Pool) Exec(f func(db *mgo.Database) error) error {
	pool.mut.RLock()
	defer pool.mut.RUnlock()

	pool.se.Acquire()
	defer pool.se.Release()

	if pool.closed {
		panic("Attempted Exec on a closed mongopool.Pool")
	}

	if pool.db == nil {
		log.Printf("Connecting to Mongo...")
		err := pool.connect()
		if err != nil {
			return errors.Wrap(err, "cannot connect to Mongo")
		}
		log.Printf("Connected to Mongo.")
	}

	return f(pool.db)
}

func (pool *Pool) Reconnect() {
	pool.mut.Lock()
	defer pool.mut.Unlock()

	if pool.db != nil {
		log.Printf("Disconnecting from Mongo to reconnect later.")
		pool.db.Session.Refresh()
		// pool.disconnect()
	}
}

func (pool *Pool) Close() {
	pool.mut.Lock()
	defer pool.mut.Unlock()

	pool.closed = true
	if pool.db != nil {
		pool.disconnect()
	}
	if pool.connected != nil {
		close(pool.connected)
	}
}

func (pool *Pool) connect() error {
	session, err := mgo.Dial(pool.URI)
	if err != nil {
		return err
	}

	if pool.Configure != nil {
		pool.Configure(session)
	}

	pool.db = session.DB(pool.DatabaseName)
	if pool.connected != nil {
		pool.connected <- true
	}

	return nil
}

func (pool *Pool) disconnect() {
	pool.db.Session.Close()
	pool.db = nil
	if pool.connected != nil {
		pool.connected <- false
	}
}
