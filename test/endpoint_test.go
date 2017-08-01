package test

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/sipt/geda"
	"github.com/sipt/geda/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/zheng-ji/goSnowFlake"
)

var topic = "test"

func TestEndpoint(t *testing.T) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)

	trans, err := kafka.NewKafkaTransport(geda.NewJsonEncoder(), nil, []string{"192.168.1.190:9092", "192.168.1.190:9093", "192.168.1.190:9094"})
	if err != nil {
		panic(err)
	}
	iw, err := goSnowFlake.NewIdWorker(1)
	if err != nil {
		panic(err)
	}
	var (
		count, sendCount, receiveCount = 10, 0, 0
		sendCountChan                  = make(chan bool, count)
		receiveCountChan               = make(chan bool, count)
	)

	id, _ := iw.NextId()
	idStr := strconv.FormatInt(id, 10)
	endpoint := geda.NewEndpoint(topic, trans, geda.DefaultBufferSize)
	unsubscribe := endpoint.Subscribe(&geda.Element{
		Topic:   topic,
		GroupID: "group-1",
	})
	defer func() {
		unsubscribe()
	}()
	elem := &geda.Element{
		Topic:  topic,
		Type:   geda.TypeEvent,
		Name:   "order pay",
		Status: geda.StatusRegisted,
	}
	go func(endpoint geda.IEndpoint, id string) {
		for e := range endpoint.SubscribeBus() {
			if e == nil || e.Topic != elem.Topic || e.Type != elem.Type || e.Status != elem.Status || e.Name != elem.Name {
				t.Error("value failed")
			} else {
				receiveCountChan <- true
			}
		}
	}(endpoint, idStr)

	for index := 0; index < count; index++ {
		go func(e geda.IEndpoint, index int) {
			element := &geda.Element{
				Topic:  elem.Topic,
				Type:   elem.Type,
				Name:   elem.Name,
				Status: elem.Status,
			}
			bus := make(geda.Bus, 1)
			id := e.Register(element, bus)
			elem := <-bus
			if elem.Err != nil {
				log.WithField("index", index).WithField("id", id).Error(elem.Err.Error())
				t.Error(elem.Err.Error())
			} else {
				sendCountChan <- true
				log.WithField("index", index).WithField("id", id).Info("repaly")
			}
		}(endpoint, index)
	}
	go func() {
		for range sendCountChan {
			sendCount++
		}
	}()
	go func() {
		for range receiveCountChan {
			receiveCount++
		}
	}()
	time.Sleep(2 * time.Second)
	if sendCount != count {
		t.Errorf("producer failed: %d, success: %d", count-sendCount, sendCount)
	} else if receiveCount != count {
		t.Errorf("consumer failed: %d, success: %d", count-receiveCount, receiveCount)
	}
}
