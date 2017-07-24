package geda

//IEndpoint endpoint interface
type IEndpoint interface {
	Cancel(*Element)
	Confirm(*Element)
	Subscribe(*Element)
	Publish(*Element)
	Output() <-chan *Element
}

const (
	DefaultBufferSize = 100
)

//NewEndpoint new endpoint
func NewEndpoint(id string, t ITransport, bufferSize int) IEndpoint {
	return &Endpoint{
		t:   t,
		ID:  id,
		bus: make(Bus, bufferSize),
	}
}

//Endpoint 端点
type Endpoint struct {
	t   ITransport
	ID  string
	bus Bus
}

//Output 获取输出chan
func (e *Endpoint) Output() <-chan *Element {
	return e.bus
}

//Cancel 取消事件
func (e *Endpoint) Cancel(elem *Element) {
	elem.Commond = CommondCancel
	elem.ID = e.ID
	// e.t.Publish(elem, e.bus)
	e.t.Publish(elem, nil)
}

//Confirm 确认事件
func (e *Endpoint) Confirm(elem *Element) {
	elem.Commond = CommondConfirm
	elem.ID = e.ID
	// e.t.Publish(elem, e.bus)
	e.t.Publish(elem, nil)
}

//Subscribe 订阅事件
func (e *Endpoint) Subscribe(elem *Element) {
	elem.ID = e.ID
	e.t.Subscribe(elem, e.bus)
}

//Publish 发布事件
func (e *Endpoint) Publish(elem *Element) {
	elem.Commond = CommondPublish
	elem.ID = e.ID
	// e.t.Publish(elem, e.bus)
	e.t.Publish(elem, nil)
}
