package geda

// import (
// 	"os"

// 	log "github.com/sirupsen/logrus"
// )

// func init() {
// 	log.SetFormatter(&log.JSONFormatter{})
// 	log.SetOutput(os.Stdout)
// 	log.SetLevel(log.InfoLevel)
// }

//IEndpoint endpoint interface
type IEndpoint interface {
	Cancel(*Element, Bus)
	Confirm(*Element, Bus)
	Register(*Element, Bus) string
	Subscribe(*Element) (Unsubscribe func())
	SubscribeBus() <-chan *Element
	Crash(id string)
}

const (
	DefaultBufferSize = 100
)

//NewEndpoint new endpoint
func NewEndpoint(topic string, t ITransport, bufferSize int) IEndpoint {
	e := &Endpoint{
		t:           t,
		Topic:       topic,
		subcribeBus: make(Bus, bufferSize),
		publishBus:  make(Bus, bufferSize),
		busStore:    make(map[string]Bus, bufferSize),
		store:       NewMap(bufferSize),
	}
	t.Init(e.subcribeBus, e.publishBus)
	go e.distribute()
	return e
}

//Endpoint 端点
type Endpoint struct {
	t           ITransport
	Topic       string
	subcribeBus Bus
	publishBus  Bus
	busStore    map[string]Bus
	store       *Map
}

//SubscribeBus 获取输出chan
func (e *Endpoint) SubscribeBus() <-chan *Element {
	return e.subcribeBus
}

//PublishBus 获取公开事件时的错误信息
func (e *Endpoint) PublishBus() <-chan *Element {
	return e.publishBus
}

//Cancel 取消事件
func (e *Endpoint) Cancel(elem *Element, bus Bus) {
	elem.Status = StatusCanceled
	elem.Topic = e.Topic
	if bus != nil {
		e.store.Put(elem.ID, bus)
	}
	e.t.Publish(elem)
}

//Confirm 确认事件
func (e *Endpoint) Confirm(elem *Element, bus Bus) {
	if bus != nil {
		e.store.Put(elem.ID, bus)
	}
	elem.Status = StatusComfirmed
	elem.Topic = e.Topic
	e.t.Publish(elem)
}

//Subscribe 订阅事件
func (e *Endpoint) Subscribe(elem *Element) func() {
	elem.Topic = e.Topic
	return e.t.Subscribe(elem)
}

//Register 发布事件
func (e *Endpoint) Register(elem *Element, bus Bus) string {
	elem.Status = StatusRegisted
	elem.Topic = e.Topic
	elem.ID = UniqueID()
	if bus != nil {
		e.store.Put(elem.ID, bus)
	}
	e.t.Publish(elem)
	return elem.ID
}

//Crash 回收空间
func (e *Endpoint) Crash(id string) {
	e.store.Remove(id)
}

func (e *Endpoint) distribute() {
	for elem := range e.publishBus {
		v := e.store.Get(elem.ID)
		if v != nil {
			bus, ok := v.(Bus)
			if ok && bus != nil {
				bus <- elem
			}
		}
	}
}
