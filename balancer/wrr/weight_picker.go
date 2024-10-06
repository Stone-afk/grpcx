package wrr

import (
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"math"
	"sync"
)

var (
	_ balancer.Picker    = (*Picker)(nil)
	_ base.PickerBuilder = (*PickerBuilder)(nil)
)

type GetWeightFunc func(ci base.SubConnInfo) uint32

type Option func(b *PickerBuilder)

func WithGetWeightFunc(fn GetWeightFunc) Option {
	return func(b *PickerBuilder) {
		b.getWeightFunc = fn
	}
}

func defaultGetWeight(ci base.SubConnInfo) uint32 {
	md, ok := ci.Address.Metadata.(map[string]any)
	if !ok {
		return 10
	}
	weightVal := md["weight"]
	weight, _ := weightVal.(uint32)
	return weight
}

type PickerBuilder struct {
	getWeightFunc GetWeightFunc
}

func NewPickerBuilder(opts ...Option) *PickerBuilder {
	res := &PickerBuilder{
		getWeightFunc: defaultGetWeight,
	}
	for _, opt := range opts {
		opt(res)
	}
	return res
}

func (b *PickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	connections := make([]*conn, 0, len(info.ReadySCs))
	for con, conInfo := range info.ReadySCs {
		weight := b.getWeightFunc(conInfo)
		connections = append(connections, &conn{
			SubConn:         con,
			connInfo:        conInfo,
			weight:          weight,
			currentWeight:   weight,
			efficientWeight: weight,
			name:            conInfo.Address.Addr,
		})
	}
	return &Picker{
		connections: connections,
	}
}

type Picker struct {
	connections []*conn
	mutex       sync.Mutex
}

func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if len(p.connections) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	var totalWeight uint32
	var maxConn *conn
	for _, con := range p.connections {
		con.mutex.Lock()
		totalWeight += con.efficientWeight
		con.currentWeight += con.efficientWeight
		if maxConn == nil || con.currentWeight > maxConn.currentWeight {
			maxConn = con
		}
		con.mutex.Unlock()
	}
	if maxConn == nil {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	maxConn.mutex.Lock()
	maxConn.currentWeight -= totalWeight
	maxConn.mutex.Unlock()
	return balancer.PickResult{
		SubConn: maxConn,
		Done: func(info balancer.DoneInfo) {
			maxConn.mutex.Lock()
			defer maxConn.mutex.Unlock()
			if info.Err != nil && maxConn.weight == 0 {
				return
			}
			if info.Err == nil && maxConn.efficientWeight == math.MaxUint32 {
				return
			}
			if info.Err != nil {
				maxConn.efficientWeight--
			} else {
				maxConn.efficientWeight++
			}
		},
	}, nil
}

type conn struct {
	balancer.SubConn
	mutex           sync.Mutex
	connInfo        base.SubConnInfo
	name            string
	weight          uint32
	efficientWeight uint32
	currentWeight   uint32
}
