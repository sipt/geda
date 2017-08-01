package geda

import (
	"encoding/json"
)

//ITransport 事件分布以及订阅端点
type ITransport interface {
	//Init 初始化transport
	Init(Bus, Bus)
	//Subscribe 用于订阅一个事件
	Subscribe(*Element) (unsubscribe func())
	//Publish 用于公开一个事件
	Publish(*Element)
	//Crash crash
	Crash()
}

//IEncoding 编码解码
type IEncoding interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

//NewJsonEncoder 新建一个json encoder
func NewJsonEncoder() IEncoding {
	return new(jsonEncoder)
}

type jsonEncoder struct{}

func (*jsonEncoder) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}
func (*jsonEncoder) Decode(bytes []byte, v interface{}) error {
	return json.Unmarshal(bytes, v)
}
