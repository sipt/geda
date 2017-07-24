package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/sipt/geda"
	"github.com/sipt/geda/kafka"
	"github.com/zheng-ji/goSnowFlake"
)

func main() {
	trans, err := kafka.NewKafkaTransport(geda.NewJsonEncoder(), nil, []string{"192.168.10.218:9092"})
	if err != nil {
		panic(err)
	}
	iw, err := goSnowFlake.NewIdWorker(1)
	if err != nil {
		panic(err)
	}
	id, _ := iw.NextId()
	endpoint := geda.NewEndpoint(strconv.FormatInt(id, 10), trans, geda.DefaultBufferSize)
	go func(endpoint geda.IEndpoint) {
		var e *geda.Element
		for {
			e = <-endpoint.Output()
			if e != nil {
				bytes, _ := json.Marshal(e)
				fmt.Println(string(bytes))
			}
		}
	}(endpoint)
	var topic string = "order"
	endpoint.Subscribe(&geda.Element{
		Title: topic,
	})
	endpoint.Publish(&geda.Element{
		Title: "order",
		Type:  "order confirm",
	})
	endpoint.Cancel(&geda.Element{
		Title: topic,
		Type:  "order confirm",
	})
	endpoint.Confirm(&geda.Element{
		Title: topic,
		Type:  "order confirm",
	})
	time.Sleep(2 * time.Second)
}
