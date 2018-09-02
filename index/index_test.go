// Package index
package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexTitle(t *testing.T) {
	index, err := NewIndex("/tmp/a.db")
	defer index.Close()
	assert.Nil(t, err)

	err = index.ClearAll()
	assert.Nil(t, err)

	err = index.AddDocument(&Document{
		PK:    "http://blog.cyeam.com/json/2014/08/04/go_json",
		Title: "Golang——json数据处理",
	})
	assert.Nil(t, err)

	cnt, res, err := index.Search(&Param{
		Query: "golang",
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, 1, cnt)
}

func TestIndexPk(t *testing.T) {
	index, err := NewIndex("/tmp/a.db")
	defer index.Close()
	assert.Nil(t, err)

	err = index.ClearAll()
	assert.Nil(t, err)

	err = index.AddDocument(&Document{
		PK:    "http://blog.cyeam.com/json/2014/08/04/go_json",
		Title: "Golang——json数据处理",
	})
	assert.Nil(t, err)

	cnt, res, err := index.Search(&Param{
		PKs: []string{"http://blog.cyeam.com/json/2014/08/04/go_json"},
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, 1, cnt)
}

func TestIndexBrief(t *testing.T) {
	index, err := NewIndex("/tmp/a.db")
	defer index.Close()

	assert.Nil(t, err)

	err = index.ClearAll()
	assert.Nil(t, err)

	err = index.AddDocument(&Document{
		PK:    "http://blog.cyeam.com/json/2014/08/04/go_json",
		Brief: "关于Unicode的介绍和Golang的处理方法。",
	})
	assert.Nil(t, err)

	cnt, res, err := index.Search(&Param{
		Query: "golang",
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, 1, cnt)
}

func TestIndexFullText(t *testing.T) {
	index, err := NewIndex("/tmp/a.db")
	defer index.Close()

	assert.Nil(t, err)

	err = index.ClearAll()
	assert.Nil(t, err)

	err = index.AddDocument(&Document{
		PK:       "http://blog.cyeam.com/json/2014/08/04/go_json",
		FullText: "关于Unicode的介绍和Golang的处理方法。",
	})
	assert.Nil(t, err)

	cnt, res, err := index.Search(&Param{
		Query: "golang",
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, 1, cnt)
}

func TestIndexTag(t *testing.T) {
	index, err := NewIndex("/tmp/a.db")
	defer index.Close()

	assert.Nil(t, err)

	err = index.ClearAll()
	assert.Nil(t, err)

	err = index.AddDocument(&Document{
		PK:   "http://blog.cyeam.com/json/2014/08/04/go_json",
		Tags: []string{"Golang", "Json", "Unicode"},
	})
	assert.Nil(t, err)

	cnt, res, err := index.Search(&Param{
		Tags: []string{"golang"},
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, 1, cnt)
}

func TestIndexCategory(t *testing.T) {
	index, err := NewIndex("/tmp/a.db")
	defer index.Close()

	assert.Nil(t, err)

	err = index.ClearAll()
	assert.Nil(t, err)

	err = index.AddDocument(&Document{
		PK:       "http://blog.cyeam.com/json/2014/08/04/go_json",
		Category: "Golang",
		Title:    "Golang——json数据处理",
	})
	assert.Nil(t, err)

	cnt, res, err := index.Search(&Param{
		Category: "golang",
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, 1, cnt)
}

func TestIndex_SortDocIds(t *testing.T) {
	index, err := NewIndex("/tmp/a.db")
	defer index.Close()

	assert.Nil(t, err)

	err = index.ClearAll()
	assert.Nil(t, err)

	err = index.AddDocument(&Document{
		PK:       "1",
		Category: "Golang",
		Title:    "Golang——json数据处理",
		PubDate:  2,
	})
	assert.Nil(t, err)
	err = index.AddDocument(&Document{
		PK:       "2",
		Category: "Golang",
		Title:    "Golang——json数据处理",
		PubDate:  2,
	})
	assert.Nil(t, err)
	err = index.AddDocument(&Document{
		PK:       "3",
		Category: "Golang",
		Title:    "Golang——json数据处理",
		PubDate:  3,
	})
	assert.Nil(t, err)

	cnt, res, err := index.Search(&Param{
		Category: "golang",
	})
	assert.Nil(t, err)
	resPks := toPks(res)
	assert.Equal(t, []string{"3", "2", "1"}, resPks)
	assert.Equal(t, 3, cnt)
}

func toPks(res []*Document) []string {
	resPks := []string{}
	for _, rrr := range res {
		resPks = append(resPks, rrr.PK)
	}
	return resPks
}

func TestIndex_PageSize(t *testing.T) {
	index, err := NewIndex("/tmp/a.db")
	defer index.Close()

	assert.Nil(t, err)

	err = index.ClearAll()
	assert.Nil(t, err)

	err = index.AddDocument(&Document{
		PK:       "1",
		Category: "Golang",
		Title:    "Golang——json数据处理",
		PubDate:  2,
	})
	assert.Nil(t, err)
	err = index.AddDocument(&Document{
		PK:       "2",
		Category: "Golang",
		Title:    "Golang——json数据处理",
		PubDate:  2,
	})
	assert.Nil(t, err)
	err = index.AddDocument(&Document{
		PK:       "3",
		Category: "Golang",
		Title:    "Golang——json数据处理",
		PubDate:  3,
	})
	assert.Nil(t, err)

	cnt, res, err := index.Search(&Param{
		Category: "golang",
	})
	assert.Nil(t, err)
	resPks := []string{}
	for _, rrr := range res {
		resPks = append(resPks, rrr.PK)
	}
	assert.Equal(t, []string{"3", "2", "1"}, resPks)
	assert.Equal(t, 3, cnt)
	assert.Equal(t, 3, len(res))

	cnt, res, err = index.Search(&Param{
		Category: "golang",
		Offset:   1,
		Size:     1,
	})
	assert.Nil(t, err)
	assert.Equal(t, "2", res[0].PK)
	assert.Equal(t, 3, cnt)
	assert.Equal(t, 1, len(res))
}

func TestAddTwoTimes(t *testing.T) {
	index, err := NewIndex("/tmp/a.db")
	defer index.Close()
	assert.Nil(t, err)

	err = index.ClearAll()
	assert.Nil(t, err)

	err = index.AddDocument(&Document{
		PK:    "http://blog.cyeam.com/json/2014/08/04/go_json",
		Title: "Golang——json数据处理",
	})
	assert.Nil(t, err)
	err = index.AddDocument(&Document{
		PK:    "http://blog.cyeam.com/json/2014/08/04/go_json",
		Title: "Golang——json数据处理",
	})
	assert.Nil(t, err)

	cnt, res, err := index.Search(&Param{
		Query: "golang",
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, 1, cnt)
}

func TestSort(t *testing.T) {
	index, err := NewIndex("/tmp/a.db")
	defer index.Close()
	assert.Nil(t, err)

	err = index.ClearAll()
	assert.Nil(t, err)

	err = index.AddDocument(&Document{
		PK:      "a",
		Title:   "Golang——json数据处理",
		PubDate: 1,
		PV:      3,
	})
	assert.Nil(t, err)
	err = index.AddDocument(&Document{
		PK:      "b",
		Title:   "Golang——json数据处理",
		PubDate: 1,
		PV:      1,
	})
	assert.Nil(t, err)
	err = index.AddDocument(&Document{
		PK:      "c",
		Title:   "Golang——json数据处理",
		PubDate: 3,
		PV:      2,
	})
	assert.Nil(t, err)

	cnt, res, err := index.SearchAll(&Param{Offset: 0, Size: 10, Sort: Sorter{"PubDate", DESC}})
	assert.Nil(t, err)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, 3, cnt)

	pks := toPks(res)
	assert.Equal(t, []string{"c", "a", "b"}, pks)

	cnt, res, err = index.SearchAll(&Param{Offset: 0, Size: 10, Sort: Sorter{"PubDate", ASC}})
	assert.Nil(t, err)
	assert.Equal(t, 3, len(res))
	assert.Equal(t, 3, cnt)

	pks = toPks(res)
	assert.Equal(t, []string{"b", "a", "c"}, pks)
}
