package index

import (
	"fmt"

	"github.com/mnhkahn/gods/xencoding"
	"github.com/mnhkahn/gods/xsort"
	"github.com/mnhkahn/gogogo/logger"
)

type InvertIndex struct {
	btname []byte
	btree  *BTree
}

func NewInvertIndex(btname []byte, btree *BTree) (*InvertIndex, error) {
	ii := new(InvertIndex)
	ii.btname = btname
	ii.btree = btree
	if btree == nil {
		return nil, fmt.Errorf("btree can't be nil")
	}
	err := btree.AddBTree(ii.btname)
	if err != nil {
		return nil, err
	}
	return ii, nil
}

func (t *InvertIndex) DeleteByKey(key []byte) error {
	return t.btree.Delete(t.btname, key)
}

func (t *InvertIndex) SetUIntsInt16(key []uint32, value int16) error {
	return t.btree.Set(t.btname, xencoding.Uints2Bytes(key), xencoding.Int162Bytes(value))
}

func (t *InvertIndex) SetUIntUints(key uint32, value []uint32) error {
	return t.btree.Set(t.btname, xencoding.Uint2Bytes(key), xencoding.Uints2Bytes(value))
}

func (t *InvertIndex) SetUIntUint(key uint32, value uint32) error {
	return t.btree.Set(t.btname, xencoding.Uint2Bytes(key), xencoding.Uint2Bytes(value))
}

func (t *InvertIndex) appendUints(docIds []uint32, value ...uint32) ([]uint32, bool) {
	hasNewDoc := false
	for _, v := range value {
		if xsort.SearchUIntsExists(docIds, v) == -1 {
			hasNewDoc = true
			value = append(docIds, v)
		}
	}
	if hasNewDoc {
		xsort.UInt32s(value)
	}
	return value, hasNewDoc
}

func (t *InvertIndex) AppendUintUints(key uint32, value ...uint32) error {
	if len(value) == 0 {
		return fmt.Errorf("append uints is nil")
	}

	docIds, exists, err := t.SearchUintUints(key)
	if err != nil {
		return err
	}

	hasNewDoc := false
	if exists {
		value, hasNewDoc = t.appendUints(docIds, value...)
	}

	if !exists || hasNewDoc {
		err = t.btree.Set(t.btname, xencoding.Uint2Bytes(key), xencoding.Uints2Bytes(value))
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *InvertIndex) DeleteUint(key uint32) error {
	return t.btree.Delete(t.btname, xencoding.Uint2Bytes(key))
}

func (t *InvertIndex) DeleteUintUints(key uint32, value ...uint32) error {
	if len(value) == 0 {
		return fmt.Errorf("delete uints is nil")
	}

	docIds, exists, err := t.SearchUintUints(key)
	if err != nil {
		return err
	}

	if exists {
		newDocIds := make([]uint32, len(docIds))
		copy(newDocIds, docIds)
		for _, v := range value {
			if pos := xsort.SearchUInts(newDocIds, v); pos != -1 {
				if pos < len(newDocIds) {
					newDocIds = append(newDocIds[:pos], newDocIds[pos+1:]...)
				} else {
					logger.Warn("delete error", key, v, pos, len(newDocIds), len(docIds))
				}
			}
		}
		t.btree.Set(t.btname, xencoding.Uint2Bytes(key), xencoding.Uints2Bytes(newDocIds))
	}

	return nil
}

func (t *InvertIndex) SetUInt64Uints(key uint64, value ...uint32) error {
	return t.btree.Set(t.btname, xencoding.Uint642Bytes(key), xencoding.Uints2Bytes(value))
}

func (t *InvertIndex) AppendUint64Uints(key uint64, value ...uint32) error {
	if len(value) == 0 {
		return fmt.Errorf("append uints is nil")
	}

	docIds, exists, err := t.SearchUint64Uints(key)
	if err != nil {
		return err
	}

	hasNewDoc := false
	if exists {
		value, hasNewDoc = t.appendUints(docIds, value...)
	}

	if !exists || hasNewDoc {
		err = t.btree.Set(t.btname, xencoding.Uint642Bytes(key), xencoding.Uints2Bytes(value))
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *InvertIndex) DeleteUint64Uints(key uint64, value ...uint32) error {
	if len(value) == 0 {
		return fmt.Errorf("delete uints is nil")
	}

	docIds, exists, err := t.SearchUint64Uints(key)
	if err != nil {
		return err
	}

	if exists {
		newDocIds := make([]uint32, len(docIds))
		// copy return value will be the minimum of len(src) and len(dst).
		copy(newDocIds, docIds)
		for _, v := range value {
			if pos := xsort.SearchUInts(newDocIds, v); pos != -1 {
				if pos < len(newDocIds) {
					newDocIds = append(newDocIds[:pos], newDocIds[pos+1:]...)
				} else {
					logger.Warn("delete error", key, v, pos)
				}
			}
		}
		t.SetUInt64Uints(key, newDocIds...)
	}

	return nil
}

func (t *InvertIndex) AppendBytesUints(key []byte, value ...uint32) error {
	if len(value) == 0 {
		return fmt.Errorf("append uints is nil")
	}

	docIds, exists, err := t.SearchBytesUints(key)
	if err != nil {
		return err
	}

	hasNewDoc := false
	if exists {
		value, hasNewDoc = t.appendUints(docIds, value...)
	}

	if !exists || hasNewDoc {
		err = t.btree.Set(t.btname, key, xencoding.Uints2Bytes(value))
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *InvertIndex) DeleteBytesUints(key []byte, value ...uint32) error {
	if len(value) == 0 {
		return fmt.Errorf("delete uints is nil")
	}

	docIds, exists, err := t.SearchBytesUints(key)
	if err != nil {
		return err
	}

	if exists {
		newDocIds := make([]uint32, 0, len(docIds))
		copy(newDocIds, docIds)
		for _, v := range value {
			if pos := xsort.SearchUInts(newDocIds, v); pos != -1 {
				newDocIds = append(newDocIds[:pos], newDocIds[pos+1:]...)
			}
		}
		t.btree.Set(t.btname, key, xencoding.Uints2Bytes(newDocIds))
	}

	return nil
}

func (t *InvertIndex) SearchUintUint(key uint32) (uint32, bool, error) {
	value, exists, err := t.btree.Search(t.btname, xencoding.Uint2Bytes(key))
	if err != nil || !exists {
		return 0, false, err
	}
	return xencoding.Bytes2Uint(value), true, nil
}

func (t *InvertIndex) SearchUintUints(key uint32) ([]uint32, bool, error) {
	value, exists, err := t.btree.Search(t.btname, xencoding.Uint2Bytes(key))
	if err != nil || !exists {
		return nil, false, err
	}
	return xencoding.Bytes2Uints(value), true, nil
}

func (t *InvertIndex) SearchUint64Uints(key uint64) ([]uint32, bool, error) {
	value, exists, err := t.btree.Search(t.btname, xencoding.Uint642Bytes(key))
	if err != nil || !exists {
		return nil, false, err
	}
	return xencoding.Bytes2Uints(value), true, nil
}

func (t *InvertIndex) SearchBytesUints(key []byte) ([]uint32, bool, error) {
	value, exists, err := t.btree.Search(t.btname, key)
	if err != nil || !exists {
		return nil, false, err
	}
	return xencoding.Bytes2Uints(value), true, nil
}

func (t *InvertIndex) SearchUintsInt16(key []uint32) (int16, bool, error) {
	value, exists, err := t.btree.Search(t.btname, xencoding.Uints2Bytes(key))
	if err != nil || !exists {
		return 0, false, err
	}
	return xencoding.Bytes2Int16(value), true, nil
}

func (t *InvertIndex) PrefixKeys(pre uint32) ([][]byte, bool, error) {
	keys, _, exists, err := t.btree.Prefix(t.btname, xencoding.Uint2Bytes(pre))
	if err != nil || !exists {
		return nil, false, err
	}

	res := make([][]byte, len(keys))
	for i, key := range keys {
		res[i] = make([]byte, len(key))
		copy(res[i], key)
	}
	return res, true, nil
}

func (t *InvertIndex) Len() int {
	return t.btree.Len(t.btname)
}

func (t *InvertIndex) Keys(len int) [][]byte {
	return t.btree.Keys(t.btname, len)
}

func (t *InvertIndex) ClearAll() error {
	err := t.btree.DeleteBTree(t.btname)
	if err != nil {
		return err
	}
	err = t.btree.AddBTree(t.btname)
	return err
}

func (t *InvertIndex) Debug() {
	t.btree.Debug(t.btname)
}

func (t *InvertIndex) SetUIntBytes(key uint32, value []byte) error {
	return t.btree.Set(t.btname, xencoding.Uint2Bytes(key), value)
}

func (t *InvertIndex) SearchUIntBytes(key uint32) ([]byte, bool, error) {
	value, exists, err := t.btree.Search(t.btname, xencoding.Uint2Bytes(key))
	if err != nil || !exists {
		return nil, false, err
	}
	return value, true, nil
}

func (t *InvertIndex) DeleteUIntBytes(key uint32) error {
	return t.btree.Delete(t.btname, xencoding.Uint2Bytes(key))
}
