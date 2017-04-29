# mongopool

Provides a concurrency limit and automatic reconnection for mgo.

See [docs on godoc.org](https://godoc.org/github.com/andreyvit/mongopool).


## Example

```go
import (
    "log"
    "os"
    "time"

    "github.com/andreyvit/mongopool"
    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
)

func Example() {
    mpool := mongopool.Dial(os.Getenv("MONGO_URI"), mongopool.Options{
        Concurrency: 10,
        Configure: func(session *mgo.Session) {
            session.SetMode(mgo.Monotonic, true)
            session.SetBatch(10000)
        },
    })
    defer mpool.Close()

    for i := 0; i < 100; i++ {
        go handle(i, mpool)
    }
}

func Handle(idx int, mpool *mongopool.Pool) {
    err := handle(idx, mpool)
    if err != nil {
        log.Printf("ERROR (worker %d): %v", idx, err)
    }
}

func handle(idx int, mpool *mongopool.Pool) error {
    db, err := mpool.Acquire()
    if err != nil {
        return err
    }
    defer mpool.Release(db)

    log.Printf("Worker %d proceeding.", idx)

    err = db.C("foo").Insert(bson.M{"i": idx})
    if err != nil {
        return err
    }

    // Slow things down for more informative output.
    time.Sleep(500 * time.Millisecond)

    return nil
}
```
