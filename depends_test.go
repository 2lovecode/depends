package depends

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type ServiceAData struct {
	Message string
	Query string
}
type ServiceA struct {
	CommonService
}

func NewServiceA() *ServiceA {
	return &ServiceA{}
}

func (s *ServiceA) Name() string {
	return "service_a"
}

func (s * ServiceA) Run(ctx context.Context) error {
	time.Sleep(10 * time.Millisecond)
	s.GetDataContainer().Set(s.Name(), ServiceAData{
		Message:"I am service a",
		Query: ctx.Value("q").(string),
	})
	return nil
}

func (s * ServiceA) Decode(receiver interface{}) error {
	data := ServiceAData{}
	if d, ok := s.GetDataContainer().Get(s.Name()).(ServiceAData); ok {
		data = d
	}
	if _, ok := receiver.(*ServiceAData); ok {
		*(receiver.(*ServiceAData)) = data
	}
	return nil
}


type ServiceBData struct {
	Message string
	Query string
}
type ServiceB struct {
	CommonService
}

func NewServiceB() *ServiceB {
	return &ServiceB{}
}

func (s *ServiceB) Name() string {
	return "service_b"
}

func (s * ServiceB) Run(ctx context.Context) error {
	time.Sleep(20 * time.Millisecond)
	s.GetDataContainer().Set(s.Name(), ServiceBData{
		Message:"I am service b",
		Query: ctx.Value("q").(string),
	})
	return nil
}

func (s * ServiceB) Decode(receiver interface{}) error {
	data := ServiceBData{}
	if d, ok := s.GetDataContainer().Get(s.Name()).(ServiceBData); ok {
		data = d
	}
	if _, ok := receiver.(*ServiceBData); ok {
		*(receiver.(*ServiceBData)) = data
	}
	return nil
}


type ServiceCData struct {
	Message string
	Query string
}
type ServiceC struct {
	CommonService
}

func NewServiceC() *ServiceC {
	return &ServiceC{}
}

func (s *ServiceC) Name() string {
	return "service_c"
}

func (s * ServiceC) Run(ctx context.Context) error {
	time.Sleep(15 * time.Millisecond)
	s.GetDataContainer().Set(s.Name(), ServiceCData{
		Message:"I am service c",
		Query: ctx.Value("q").(string),
	})
	return nil
}

func (s * ServiceC) Decode(receiver interface{}) error {
	data := ServiceCData{}
	if d, ok := s.GetDataContainer().Get(s.Name()).(ServiceCData); ok {
		data = d
	}
	if _, ok := receiver.(*ServiceCData); ok {
		*(receiver.(*ServiceCData)) = data
	}
	return nil
}

func TestDepends_Execute(t *testing.T) {
	for i := 0; i < 4; i++ {
		ctx := context.WithValue(context.TODO(), "q", "test")

		sA := NewServiceA()
		sB := NewServiceB()
		sC := NewServiceC()

		hd := NewDepends(100 * time.Millisecond)

		hd.Register(sA)
		hd.Register(sB)
		hd.Register(sC)

		hd.AddDepend(sC, []IService{sB})
		hd.AddDepend(sB, []IService{sA})

		hd.Execute(ctx)

		sAData := ServiceAData{}
		sBData := ServiceBData{}
		sCData := ServiceCData{}

		sA.Decode(&sAData)
		sB.Decode(&sBData)
		sC.Decode(&sCData)

		fmt.Println(sAData, sBData, sCData)
	}
}

func BenchmarkDepends_Execute(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx := context.WithValue(context.TODO(), "q", "test")

		sA := NewServiceA()
		sB := NewServiceB()
		sC := NewServiceC()

		hd := NewDepends(100 * time.Millisecond)

		hd.Register(sA)
		hd.Register(sB)
		hd.Register(sC)


		hd.AddDepend(sC, []IService{sB})
		hd.AddDepend(sB, []IService{sA})

		hd.Execute(ctx)
		sAData := ServiceAData{}
		sBData := ServiceBData{}
		sCData := ServiceCData{}


		sA.Decode(&sAData)
		sB.Decode(&sBData)
		sC.Decode(&sCData)

		fmt.Println(sAData, sBData, sCData)
	}
}
