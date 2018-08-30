package index

import (
	"fmt"

	"github.com/vmihailenco/msgpack"

	"github.com/huichen/sego"
	"github.com/mnhkahn/gogogo/logger"
	"github.com/mnhkahn/gogogo/panicer"
	"github.com/mnhkahn/peanut/util"
)

func (index *Index) createCurIdIfNotExists(pk string) (uint32, error) {
	return index.getCurId(), nil
}

func (index *Index) getCurId() uint32 {
	curId, valid := index.status.NextClear(0)
	if valid {
		return curId
	}
	return index.status.Len()
}

func (index *Index) AddDocument(doc *Document) error {
	defer panicer.RecoverDebug(doc.PK)

	if doc == nil {
		return fmt.Errorf("document is nil")
	}

	var err error

	// ========== Lock =============
	index.documentLock.Lock()
	docId, err := index.createCurIdIfNotExists(doc.PK)
	logger.Debugf("add document doc: %d, %v, %v", docId, doc, err)
	if err != nil {
		index.documentLock.Unlock()
		return err
	}
	index.status.Set(docId)
	err = index.pk.AppendBytesUints([]byte(doc.PK), docId)
	if err != nil {
		index.documentLock.Unlock()
		return err
	}

	if err = index.extendMaybe(docId); err != nil {
		index.documentLock.Unlock()
		return fmt.Errorf("error: %s docId: %v", err.Error(), doc)
	}

	b, _ := msgpack.Marshal(doc)
	err = index.documents.SetUIntBytes(docId, b)
	if err != nil {
		return err
	}

	index.documentLock.Unlock()
	// ========== Lock =============

	// add title index
	for _, t := range sego.SegmentsToSlice(index.segmenter.Segment([]byte(doc.Title)), false) {
		logger.Infof("sego %s %s", t, doc.Title)
		index.title.AppendBytesUints([]byte(t), docId)
	}

	for _, t := range sego.SegmentsToSlice(index.segmenter.Segment([]byte(doc.Brief)), false) {
		index.brief.AppendBytesUints([]byte(t), docId)
	}

	for _, t := range sego.SegmentsToSlice(index.segmenter.Segment([]byte(doc.FullText)), false) {
		index.fullText.AppendBytesUints([]byte(t), docId)
	}

	for _, tag := range doc.Tags {
		index.tag.AppendBytesUints(util.StrToLowerBytes(tag), docId)
	}

	index.category.AppendBytesUints(util.StrToLowerBytes(doc.Category), docId)

	return index.Commit()
}

func (index *Index) Commit() error {
	return index.status.Backup()
}
