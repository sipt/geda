package geda

import "encoding/json"

const (
	//CommondCancel 取消事件
	CommondCancel = "cancel"
	//CommondConfirm 确认事件
	CommondConfirm = "confirm"
	//CommondPublish 公开事件
	CommondPublish = "publish"
	//CommondFinish 结束事件
	CommondFinish = "finish"
)

//Element 数据交换单元
//用于数据订阅以及发布时数据部
type Element struct {
	Title   string      `json:",omitempty"`
	Type    string      `json:",omitempty"`
	ID      string      `json:",omitempty"`
	Commond string      `json:",omitempty"`
	Data    interface{} `json:",omitempty"`
	Err     error       `json:"-"`
}

//Bus 总线定阅后用于等待返回
type Bus chan *Element

//ITransport 事件分布以及订阅端点
type ITransport interface {
	//Subscribe 用于订阅一个事件
	Subscribe(*Element, Bus)
	//Publish 用于分开一个事件
	Publish(*Element, Bus)
}

//IEncoding 编码解码
type IEncoding interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
}

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
