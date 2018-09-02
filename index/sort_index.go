// Package index
package index

import (
	"sort"
	"strings"
)

const (
	ASC  = true
	DESC = false
)

type indexSort struct {
	sort   Sorter
	docIds []uint32
	_index *Index
}

// Len ...
func (is indexSort) Len() int {
	return len(is.docIds)
}

// Less ...
func (is indexSort) Less(i, j int) bool {
	docs := is._index.ToDocuments(is.docIds[i], is.docIds[j])
	a := docs[0]
	b := docs[1]

	switch strings.ToLower(is.sort.Field) {
	case "pv":
		return If(a.PV == b.PV, func() bool { return a.PK > b.PK }, func() bool { return a.PV < b.PV })
	default:
		return If(a.PubDate == b.PubDate, func() bool { return a.PK > b.PK }, func() bool { return a.PubDate < b.PubDate })
	}
}

// Swap ...
func (is indexSort) Swap(i, j int) {
	is.docIds[i], is.docIds[j] = is.docIds[j], is.docIds[i]
}

// SortDocIds ...
func (index *Index) SortDocIds(param *Param, docIds []uint32) []uint32 {
	if len(docIds) <= 1 {
		return docIds
	}

	if param.Sort.Asc {
		sort.Sort(indexSort{param.Sort, docIds, index})
	} else {
		sort.Sort(sort.Reverse(indexSort{param.Sort, docIds, index}))
	}

	return docIds
}

// If ...
// https://my.oschina.net/chai2010/blog/202870
func If(expr bool, f1, f2 func() bool) bool {
	if expr {
		return f1()
	}
	return f2()
}
