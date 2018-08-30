// Package index
package index

import "github.com/mnhkahn/gods/xsort"

// SortDocIds ...
func (index *Index) SortDocIds(param *Param, docIds []uint32) []uint32 {
	if len(docIds) <= 1 {
		return docIds
	}

	xsort.SortUintLess(docIds, func(i, j int) bool {
		docs := index.ToDocuments(docIds[i], docIds[j])
		a := docs[0]
		b := docs[1]

		if a.PubDate == b.PubDate {
			return a.PK > b.PK
		} else {
			return a.PubDate > b.PubDate
		}
	})
	return docIds
}
