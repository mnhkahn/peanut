package index

import (
	"fmt"

	"github.com/huichen/sego"

	"github.com/vmihailenco/msgpack"

	"github.com/mnhkahn/gogogo/logger"

	"github.com/mnhkahn/gods/xsort"
)

// Search ...
func (index *Index) Search(pars *Param) (int, []*Document, error) {
	total, docIds, err := index.SearchDocIds(pars)
	if err != nil {
		return 0, nil, err
	}

	docs := index.ToDocuments(docIds...)
	return total, docs, nil
}

// SearchDocIds ...
func (index *Index) SearchDocIds(param *Param) (int, []uint32, error) {
	if param == nil {
		return 0, nil, fmt.Errorf("param can't be nil")
	}

	index.CheckParam(param)

	res := make([]uint32, 0)
	mergeIds := make([][]uint32, 0, 4)

	if len(param.PKs) > 0 {
		pkDocIds, err := index.SearchPks(param.PKs...)
		if err != nil {
			return 0, nil, err
		}
		mergeIds = append(mergeIds, pkDocIds)
	}

	if param.Query != "" {
		querys := sego.SegmentsToSlice(index.segmenter.Segment([]byte(param.Query)), false)
		keyWordIds, err := index.SearchKeyWords(querys)
		if err != nil {
			return 0, nil, err
		}

		mergeIds = append(mergeIds, keyWordIds)
	}

	if len(param.Tags) > 0 {
		tagDocIds, err := index.SearchTag(param.Tags...)
		if err != nil {
			return 0, nil, err
		}
		mergeIds = append(mergeIds, tagDocIds)
	}

	if param.Category != "" {
		categoryDocIds, err := index.SearchCategory(param.Category)
		if err != nil {
			return 0, nil, err
		}
		mergeIds = append(mergeIds, categoryDocIds)
	}

	res = xsort.MergeAndUints(mergeIds...)

	total := len(res)
	// sort
	res = index.SortDocIds(param, res)
	// page & size
	res = index.PageSizeDocIds(res, param.Offset, param.Size)

	return total, res, nil
}

// CheckParam check if param is error.
// Offset default value is 0.
// Size default value is 10.
func (index *Index) CheckParam(param *Param) {
	if param.Offset < 0 {
		param.Offset = 0
	}
	if param.Size <= 0 || param.Size > 100 {
		param.Size = 100
	}
}

// SearchPks ...
func (index *Index) SearchPks(pks ...string) ([]uint32, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	res := make([]uint32, 0, len(pks))
	for _, pk := range pks {
		docIds, exists, err := index.pk.SearchBytesUints([]byte(pk))
		if err != nil {
			return res, err
		} else if exists {
			if len(docIds) != 1 {
				logger.Debugf("pk #%s has no pk index. %v.", pk, docIds)
			} else {
				res = append(res, docIds[0])
			}
		}
	}

	return res, nil
}

// SearchKeyWords search by keywords. If query is english, it should be lower case.
func (index *Index) SearchKeyWords(queries []string) ([]uint32, error) {
	if len(queries) == 0 {
		return nil, nil
	}

	res := make([][]uint32, 0, len(queries))
	for _, query := range queries {
		// search in title
		docIds, exists, err := index.title.SearchBytesUints([]byte(query))
		if err != nil {
			return nil, err
		} else if exists {
			res = append(res, docIds)
		}

		// search in brief
		docIds, exists, err = index.brief.SearchBytesUints([]byte(query))
		if err != nil {
			return nil, err
		} else if exists {
			res = append(res, docIds)
		}

		// search in full text
		docIds, exists, err = index.fullText.SearchBytesUints([]byte(query))
		if err != nil {
			return nil, err
		} else if exists {
			res = append(res, docIds)
		}
	}

	return xsort.MergeOrUints(res...), nil
}

// SearchTag ...
func (index *Index) SearchTag(tags ...string) ([]uint32, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	res := make([]uint32, 0, len(tags))
	for _, t := range tags {
		docIds, exists, err := index.tag.SearchBytesUints([]byte(t))
		if err != nil {
			return res, err
		} else if exists {
			res = append(res, docIds...)
			xsort.UInt32s(res)
		}
	}
	return res, nil
}

// SearchCategory ...
func (index *Index) SearchCategory(category ...string) ([]uint32, error) {
	if len(category) == 0 {
		return nil, nil
	}
	res := make([]uint32, 0, len(category))
	for _, t := range category {
		docIds, exists, err := index.category.SearchBytesUints([]byte(t))
		if err != nil {
			return res, err
		} else if exists {
			res = append(res, docIds...)
			xsort.UInt32s(res)
		}
	}
	return res, nil
}

// ToDocuments ...
func (index *Index) ToDocuments(docIds ...uint32) []*Document {
	res := make([]*Document, 0, len(docIds))
	for _, docId := range docIds {
		byts, exists, err := index.documents.SearchUIntBytes(docId)
		if !exists || err != nil {
			logger.Warn(err)
		}
		d := new(Document)
		msgpack.Unmarshal(byts, d)
		res = append(res, d)
	}

	return res
}

// PageSizeDocIds ...
func (index *Index) PageSizeDocIds(docIds []uint32, offset, size int) []uint32 {
	le := len(docIds)
	if le <= 1 {
		return docIds
	}

	if offset < le {
		if offset+size <= le {
			return docIds[offset : offset+size]
		} else {
			return docIds[offset:]
		}
	}
	return []uint32{}
}
