package geda

import (
	"strconv"

	"github.com/zheng-ji/goSnowFlake"
)

var iw *goSnowFlake.IdWorker

func init() {
	var err error
	iw, err = goSnowFlake.NewIdWorker(1)
	if err != nil {
		panic(err)
	}
}

func UniqueID() string {
	id, _ := iw.NextId()
	return strconv.FormatInt(id, 10)
}
