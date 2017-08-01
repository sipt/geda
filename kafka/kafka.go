package kafka

import (
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/sipt/geda"
	log "github.com/sirupsen/logrus"
)

const (
	TimeOut = 2
)

var addresses []string

//NewKafkaTransport 实例化一个Transport
func NewKafkaTransport(encoder geda.IEncoding, config *sarama.Config, addrs []string) (*KafkaTransport, error) {
	if config == nil {
		config = sarama.NewConfig()
		config.Producer.Timeout = TimeOut * time.Second
		config.Producer.Return.Successes = true
		config.Producer.Flush.Messages = 1
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	}
	addresses = addrs

	transport := &KafkaTransport{
		Producer: newProducer(config, addrs),
		Encoder:  encoder,
	}
	return transport, nil
}

func newCustomer(config *sarama.Config, addrs []string) sarama.Consumer {
	return nil
}

func newProducer(config *sarama.Config, addrs []string) sarama.AsyncProducer {
	p, err := sarama.NewAsyncProducer(addrs, config)
	if err != nil {
		panic(err)
	}
	return p
}

//KafkaTransport kafka transport
type KafkaTransport struct {
	Producer    sarama.AsyncProducer
	Encoder     geda.IEncoding
	ConsumerBus geda.Bus
	ProducerBus geda.Bus
}

//Init 初始化transport
func (k *KafkaTransport) Init(consumerBus, producerBus geda.Bus) {
	k.ConsumerBus = consumerBus
	k.ProducerBus = producerBus
	go func() {
		for err := range k.Producer.Errors() {
			elem := k.decodeProducerMessage(err.Msg)
			elem.Err = err
			log.WithFields(log.Fields{
				"ID":     elem.ID,
				"Topic":  elem.Topic,
				"Name":   elem.Name,
				"Type":   elem.Type,
				"Status": elem.Status,
			}).Error(err.Error())
			if k.ProducerBus != nil {
				k.ProducerBus <- elem
			}
		}
	}()
	go func() {
		for msg := range k.Producer.Successes() {
			r := k.decodeProducerMessage(msg)
			if k.ProducerBus != nil {
				k.ProducerBus <- r
			}
		}
	}()
}

//Subscribe 用于订阅一个事件
func (k *KafkaTransport) Subscribe(e *geda.Element) func() {
	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest //初始从最新的offset开始

	consumer, err := cluster.NewConsumer(addresses, e.GroupID, []string{e.Topic}, config)
	if err != nil {
		panic(err)
	}
	go func() {
		r := &geda.Element{
			GroupID: e.GroupID,
			Topic:   e.Topic,
			Type:    e.Type,
			ID:      e.ID,
			Status:  e.Status,
		}
		errorChan := consumer.Errors()
		notiChan := consumer.Notifications()
		for {
			select {
			case err := <-errorChan:
				if err != nil {
					e.Err = err
					log.WithFields(log.Fields{
						"Topic": e.Topic,
					}).Error(err.Error())
					if k.ConsumerBus != nil {
						k.ConsumerBus <- r
					}
				}
			case <-notiChan:
			}
		}
	}()
	go func() {
		for msg := range consumer.Messages() {
			r := k.decodeConsumerMessage(msg)
			if k.ConsumerBus != nil {
				k.ConsumerBus <- r
			}
			consumer.MarkOffset(msg, "") // mark message as processed
			bytes, _ := json.Marshal(msg)
			log.Debug(string(bytes))
		}
	}()
	return func() {
		consumer.Close()
	}
}

//Publish 用于公布一个事件
func (k *KafkaTransport) Publish(e *geda.Element) {
	bytes, err := k.Encoder.Encode(e)
	if err != nil {
		e.Err = err
		log.WithFields(log.Fields{
			"ID":     e.ID,
			"Topic":  e.Topic,
			"Name":   e.Name,
			"Type":   e.Type,
			"Status": e.Status,
		}).Error(err.Error())
		if k.ProducerBus != nil {
			k.ProducerBus <- e
		}
		return
	}
	k.Producer.Input() <- &sarama.ProducerMessage{
		Topic:     e.Topic,
		Key:       sarama.ByteEncoder(e.Name),
		Value:     sarama.ByteEncoder(bytes),
		Timestamp: time.Now(),
	}
}

//Crash crash
func (k *KafkaTransport) Crash() {
	k.Producer.AsyncClose()
}

func (k *KafkaTransport) decodeConsumerMessage(msg *sarama.ConsumerMessage) *geda.Element {
	if len(msg.Value) <= 0 {
		return nil
	}
	return k.decodeBytes(msg.Value)
}

func (k *KafkaTransport) decodeProducerMessage(msg *sarama.ProducerMessage) *geda.Element {
	if msg.Value.Length() <= 0 {
		return nil
	}

	bytes, err := msg.Value.Encode()
	if err != nil {
		r := &geda.Element{}
		r.Err = err
		return r
	}
	return k.decodeBytes(bytes)
}

func (k *KafkaTransport) decodeBytes(bytes []byte) *geda.Element {
	r := &geda.Element{}
	err := k.Encoder.Decode(bytes, &r)
	if err != nil {
		r.Err = err
	}
	return r
}
