// package core
// 这是一个B+树的缓存，底层实现用到了mmap。
// 如果是使用vagrant或者是虚拟机，数据库文件不能存放到共享目录里面。
// https://stackoverflow.com/questions/18420473/invalid-argument-for-read-write-mmap
package index

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/mnhkahn/gogogo/logger"
)

type BTree struct {
	name string
	db   *bolt.DB
}

func NewBTree(dbpath string) (*BTree, error) {
	if dbpath == "" {
		return nil, errors.New("dbpath can't be nil.")
	}
	t := new(BTree)
	t.name = dbpath

	var err error
	t.db, err = bolt.Open(dbpath, 0666, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("err: %s, %s", err, dbpath)
	}
	return t, nil
}

func (t *BTree) GetDB() *bolt.DB {
	return t.db
}

func (t *BTree) AddBTree(btname []byte) error {
	tx, err := t.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.CreateBucketIfNotExists(btname)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (t *BTree) Set(btname, key []byte, value []byte) error {
	return t.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(btname)
		if b == nil {
			return fmt.Errorf("Tablename[%v] not found", string(btname))
		}
		err := b.Put(key, value)
		return err
	})
}

func (t *BTree) Delete(btname, key []byte) error {
	return t.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(btname)
		if b == nil {
			return fmt.Errorf("Tablename[%v] not found", string(btname))
		}
		err := b.Delete(key)
		return err
	})
}

func (t *BTree) Search(btname []byte, key []byte) ([]byte, bool, error) {
	var value []byte
	err := t.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(btname)
		value = b.Get(key)

		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return value, len(value) > 0, nil
}

func (t *BTree) Prefix(btname []byte, prefix []byte) ([][]byte, [][]byte, bool, error) {
	var keys, values [][]byte
	err := t.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(btname).Cursor()
		for k, v := b.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = b.Next() {
			keys = append(keys, k)
			values = append(values, v)
		}

		return nil
	})
	if err != nil {
		return nil, nil, false, err
	}
	return keys, values, len(keys) > 0, nil
}

func (t *BTree) DeleteBTree(btname []byte) error {
	logger.Info("clear bucket", string(btname))

	tx, err := t.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = tx.DeleteBucket(btname)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (t *BTree) Len(btname []byte) int {
	len := 0
	t.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(btname)
		len = b.Stats().KeyN
		// b.ForEach(func(k, v []byte) error {
		// 	len++
		// 	return nil
		// })
		return nil
	})

	return len
}

func (t *BTree) Close() error {
	return t.db.Close()
}

func (t *BTree) Keys(btname []byte, len int) [][]byte {
	res := make([][]byte, 0, len)
	i := 0
	t.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(btname)

		b.ForEach(func(k, v []byte) error {
			if i < len {
				res = append(res, k)
				i++

				return nil
			} else {
				return fmt.Errorf("exceed len.")
			}
		})
		return nil
	})
	return res
}

func (t *BTree) Debug(btname []byte) {
	t.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(btname)

		b.ForEach(func(k, v []byte) error {
			logger.Info(string(btname), k, v)
			return nil
		})
		return nil
	})
}
