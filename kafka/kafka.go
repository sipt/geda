package kafka

import (
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sipt/geda"
)

const (
	Addresses = "192.168.1.157:9092"
	TimeOut   = 5
)

// asyncProducer 异步生产者
// 并发量大时，必须采用这种方式
func NewKafkaTransport(encoder geda.IEncoding, config *sarama.Config) (*KafkaTransport, error) {
	if config == nil {
		config = sarama.NewConfig()
		config.Producer.Return.Successes = true //必须有这个选项
		config.Producer.Timeout = TimeOut * time.Second
	}
	return &KafkaTransport{
		Producer: newProducer(config),
		Consumer: newCustomer(config),
		Encoder:  encoder,
	}, nil
}

func newCustomer(config *sarama.Config) sarama.Consumer {
	c, err := sarama.NewConsumer(strings.Split(Addresses, ","), config)
	if err != nil {
		panic(err)
	}
	return c
}

func newProducer(config *sarama.Config) sarama.AsyncProducer {
	p, err := sarama.NewAsyncProducer(strings.Split(Addresses, ","), config)
	if err != nil {
		panic(err)
	}
	return p
}

//KafkaTransport kafka transport
type KafkaTransport struct {
	Producer sarama.AsyncProducer
	Consumer sarama.Consumer
	Encoder  geda.IEncoding
}

//Subscribe 用于订阅一个事件
func (k *KafkaTransport) Subscribe(e *geda.Element, bus geda.Bus) {
	partitionConsumer, err := k.Consumer.ConsumePartition(e.Title, 0, sarama.OffsetNewest)
	if err != nil {
		e.Err = err
	}
	if bus != nil {
		bus <- e
	}

	//必须有这个匿名函数内容
	go func(c sarama.PartitionConsumer, e *geda.Element, bus geda.Bus) {
		errors := c.Errors()
		messages := c.Messages()
		for {
			select {
			case err := <-errors:
				e.Err = err
				if bus != nil {
					bus <- e
				}
			case msg := <-messages:
				if len(msg.Value) <= 0 && bus != nil {
					bus <- nil
					continue
				}
				err = k.Encoder.Decode(msg.Value, e)
				if err != nil {
					e.Err = err
				}
				if bus != nil {
					bus <- e
				}
			}
		}
	}(partitionConsumer, e, bus)
}

//Publish 用于公布一个事件
func (k *KafkaTransport) Publish(e *geda.Element, bus geda.Bus) {
	bytes, err := k.Encoder.Encode(e)
	if err != nil {
		e.Err = err
	}
	if bus != nil {
		bus <- e
	}
	k.Producer.Input() <- &sarama.ProducerMessage{
		Topic: e.Title,
		Value: sarama.ByteEncoder(bytes),
	}
	go func(p sarama.AsyncProducer) {
		errors := k.Producer.Errors()
		success := k.Producer.Successes()
		for {
			select {
			case err := <-errors:
				e.Err = err
				if bus != nil {
					bus <- e
				}
			case <-success:
				if bus != nil {
					bus <- nil
				}
			}
		}
	}(k.Producer)
}
