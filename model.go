package geda

import "time"

const (
	//CommondCancel 取消事件
	CommondCancel = "cancel"
	//CommondConfirm 确认事件
	CommondConfirm = "confirm"
	//CommondPublish 公开事件
	CommondPublish = "publish"
	//CommondFinish 结束事件
	CommondFinish = "finish"

	//StatusRegisted 已注册
	StatusRegisted = "registed"
	//StatusRefused 已拒绝
	StatusRefused = "refused"
	//StatusComfirmed 已确认
	StatusComfirmed = "confirmed"
	//StatusCanceled 已取消
	StatusCanceled = "canceled"

	//TypeEvent 事件
	TypeEvent = "event"
	//TypeCommond 命令
	TypeCommond = "commond"
	//TypeQuery 查询
	TypeQuery = "query"
)

//Element 数据交换单元
//用于数据订阅以及发布时数据部
type Element struct {
	GroupID string `json:",omitempty"`

	//Topic 所发布事件的类型
	Topic string `json:",omitempty"`

	//Name 所发布事件的子类型
	Name string `json:",omitempty"`

	//ID 唯一标识
	ID string `json:",omitempty"`

	//Type 类型{"commond","event","query"}
	Type string `json:",omitempty"`

	//Status 状态{"registed", "refused", "confirmed"}
	Status string `json:",omitempty"`

	//Data 数据
	Data interface{} `json:",omitempty"`

	//Err 错误信息
	Err error `json:",omitempty"`

	//ParentID 母事件ID，用于链式追踪
	ParentID string `json:",omitempty"`

	CreateTime time.Time `json:"create_time"`
}

//Bus 总线定阅后用于等待返回
type Bus chan *Element

func checkElementParams(elem *Element, flag int) error {
	return nil
}

//NewMap 新建一个Map
func NewMap(bufferSize int) *Map {
	m := &Map{
		store: make(map[interface{}]interface{}, bufferSize),
		mapChan: make(chan struct {
			op    string
			key   interface{}
			value interface{}
		}, bufferSize),
	}
	go m.operation()
	return m
}

//Map 自定义map支持并发
type Map struct {
	store   map[interface{}]interface{}
	mapChan chan struct {
		op    string
		key   interface{}
		value interface{}
	}
}

//Put 插入
func (m *Map) Put(key, value interface{}) {
	m.mapChan <- struct {
		op    string
		key   interface{}
		value interface{}
	}{
		op:    "put",
		key:   key,
		value: value,
	}
}

//Remove 删除
func (m *Map) Remove(key interface{}) {
	m.mapChan <- struct {
		op    string
		key   interface{}
		value interface{}
	}{
		op:  "remove",
		key: key,
	}
}

//Get 获取值
func (m *Map) Get(id interface{}) interface{} {
	return m.store[id]
}

func (m *Map) operation() {
	for s := range m.mapChan {
		switch s.op {
		case "put":
			m.store[s.key] = s.value
		case "remove":
			delete(m.store, s.key)
		}
	}
}

//Crash 回收
func (m *Map) Crash() {
	close(m.mapChan)
}
