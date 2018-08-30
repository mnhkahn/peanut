package index

import (
	"sync"

	"github.com/mnhkahn/gogogo/logger"
	"github.com/willf/bitset"
)

type Bitmap struct {
	data   *bitset.BitSet
	btree  *BTree
	btname []byte
	lock   sync.RWMutex
}

func NewBitmap(btname []byte, btree *BTree) (*Bitmap, error) {
	b := new(Bitmap)
	b.btname = btname
	b.btree = btree
	b.data = bitset.New(1)

	err := btree.AddBTree(btname)
	if err != nil {
		return b, err
	}

	byts, exists, err := btree.Search(btname, btname)
	if err != nil {
		logger.Warn(err)
	} else if exists {
		err = b.data.UnmarshalBinary(byts)
		if err != nil {
			logger.Warn(err)
		}
	}

	return b, nil
}

func (b *Bitmap) ClearAll() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.data = b.data.ClearAll()

	err := b.btree.DeleteBTree(b.btname)
	if err != nil {
		return err
	}
	err = b.btree.AddBTree(b.btname)
	return err
}

func (b *Bitmap) Set(i uint32) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.data.Set(uint(i))
	return nil
}

func (b *Bitmap) SetTo(i uint32, value bool) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.data.SetTo(uint(i), value)
	return nil
}

// Count() 才是所占位的长度，Len() 是位的总数
func (b *Bitmap) Len() uint32 {
	b.lock.RLock()
	defer b.lock.RUnlock()

	return uint32(b.data.Count())
}

func (b *Bitmap) Test(i uint32) bool {
	b.lock.RLock()
	defer b.lock.RUnlock()

	return b.data.Test(uint(i))
}

func (b *Bitmap) NextSet(i uint32) (uint32, bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	r, e := b.data.NextSet(uint(i))
	return uint32(r), e
}

func (b *Bitmap) NextClear(i uint32) (uint32, bool) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	r, e := b.data.NextClear(uint(i))
	return uint32(r), e
}

func (b *Bitmap) Uints(status bool) []uint32 {
	res := make([]uint32, 0, b.Len())

	counter := 0
	var i uint32
	var e bool
	if status {
		i, e = b.NextSet(0)
	} else {
		i, e = b.NextClear(0)
	}
	for e {
		counter = counter + 1
		// to avoid exhausting the memory
		if counter > 0x80000 {
			res = append(res, 0)
			break
		}
		res = append(res, i)

		if status {
			i, e = b.NextSet(i + 1)
		} else {
			i, e = b.NextClear(i + 1)
		}
	}
	return res
}

func (b *Bitmap) Backup() error {
	b.lock.RLock()
	defer b.lock.RUnlock()

	logger.Info("backup", string(b.btname), b.Len())

	byts, err := b.data.MarshalBinary()
	if err != nil {
		return err
	}
	err = b.btree.Set(b.btname, b.btname, byts)
	if err != nil {
		return err
	}

	return nil
}

func (b *Bitmap) Close() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.Backup()
}
