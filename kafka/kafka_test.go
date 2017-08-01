package kafka

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/sipt/geda"
)

func TestProducer(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	bus := make(geda.Bus, 100)
	trans, err := NewKafkaTransport(geda.NewJsonEncoder(), nil, []string{"192.168.1.157:9092"}, bus)
	if err != nil {
		t.Error(err)
	}
	pBus, cBus := make(geda.Bus), make(geda.Bus)
	trans.Subscribe(&geda.Element{
		Topic: "test",
	}, cBus)
	count := 5
	go func(trans *KafkaTransport) {
		for index := 0; index < count; index++ {
			trans.Publish(&geda.Element{
				Topic: "test",
				Type:  "confirm",
				ID:    "123123123",
				Data:  fmt.Sprint("value index :", index),
			}, pBus)
		}
	}(trans)
	go func() {
		for {
			bytes, _ := json.Marshal(<-pBus)
			fmt.Println("p: ", string(bytes))
		}
	}()
	go func() {
		for {
			bytes, _ := json.Marshal(<-cBus)
			fmt.Println("c: ", string(bytes))
		}
	}()
	time.Sleep(5 * time.Second)
	trans.Crash()
}
