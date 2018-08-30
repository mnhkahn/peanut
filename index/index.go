package index

import (
	"encoding/binary"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/huichen/sego"
	"github.com/mnhkahn/gogogo/logger"
)

var (
	documentIndexName = []byte("DocIndex")
	statusIndexName   = []byte("DocId")
	pkIndexName       = []byte("PK")
	titleIndexName    = []byte("Title")
	briefIndexName    = []byte("Brief")
	fullTextIndexName = []byte("FullText")
	tagIndexName      = []byte("Tags")
	categoryIndexName = []byte("Category")
)

type Index struct {
	_index *BTree
	// 主键
	pk       *InvertIndex
	title    *InvertIndex
	brief    *InvertIndex
	fullText *InvertIndex
	tag      *InvertIndex
	category *InvertIndex
	status   *Bitmap

	documentLock sync.Mutex
	documents    *InvertIndex

	segmenter sego.Segmenter
}

func NewIndex(path string) (*Index, error) {
	var err error

	index := new(Index)

	index._index, err = NewBTree(path)
	if err != nil {
		return index, err
	}

	index.status, err = NewBitmap(statusIndexName, index._index)
	if err != nil {
		return index, err
	}

	index.pk, err = NewInvertIndex(pkIndexName, index._index)
	if err != nil {
		return index, err
	}

	index.title, err = NewInvertIndex(titleIndexName, index._index)
	if err != nil {
		return index, err
	}

	index.brief, err = NewInvertIndex(briefIndexName, index._index)
	if err != nil {
		return index, err
	}

	index.fullText, err = NewInvertIndex(fullTextIndexName, index._index)
	if err != nil {
		return index, err
	}

	index.tag, err = NewInvertIndex(tagIndexName, index._index)
	if err != nil {
		return index, err
	}

	index.category, err = NewInvertIndex(categoryIndexName, index._index)
	if err != nil {
		return index, err
	}

	index.documents, err = NewInvertIndex(documentIndexName, index._index)
	if err != nil {
		return index, err
	}

	index.segmenter.LoadDictionary("./dictionary.txt")

	return index, err
}

func (index *Index) GetDB() *bolt.DB {
	return index._index.GetDB()
}

// =============== documents ====================
func (index *Index) extendMaybe(docId uint32) error {
	return nil
}

// =============== documents ====================

func (index *Index) ClearAll() error {
	logger.Info("index clear all.")

	index.pk.ClearAll()
	index.title.ClearAll()
	index.brief.ClearAll()
	index.fullText.ClearAll()
	index.tag.ClearAll()
	index.category.ClearAll()
	index.status.ClearAll()
	index.documents.ClearAll()

	return nil
}

func (index *Index) Close() error {
	return index._index.Close()
}

func (index *Index) Buckets() (map[string]int, error) {
	res := make(map[string]int)

	err := index.GetDB().View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
			switch string(name) {
			case string(documentIndexName):
				res[string(name)] = index.documents.Len()
			case string(statusIndexName):
				res[string(name)] = int(index.status.Len())
			default:
				l := 0
				bucket.ForEach(func(k, v []byte) error {
					l += len(v) / binary.MaxVarintLen32
					return nil
				})
				res[string(name)] = l
			}

			return nil
		})

	})

	return res, err
}
