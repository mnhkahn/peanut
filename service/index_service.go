// Package service
package service

import "github.com/mnhkahn/peanut/index"

var DefaultIndex *index.Index

func init() {
	var err error
	DefaultIndex, err = index.NewIndex("./peanut.db")
	if err != nil {
		panic(err)
	}
}
