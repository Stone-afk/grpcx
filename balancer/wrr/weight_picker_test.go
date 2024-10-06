package wrr

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
	"testing"
)

func TestWeightBalancer_PickerBuilder(t *testing.T) {
	testCases := []struct {
		name        string
		b           *PickerBuilder
		buildInfoFn func() base.PickerBuildInfo
		wantWeights map[string]uint32
	}{
		{
			name: "default",
			buildInfoFn: func() base.PickerBuildInfo {
				readySCs := make(map[balancer.SubConn]base.SubConnInfo, 3)
				k1 := &mockSubConn{id: 1}
				k2 := &mockSubConn{id: 2}
				k3 := &mockSubConn{id: 3}
				readySCs[k1] = base.SubConnInfo{
					Address: resolver.Address{
						Addr:     "weight-1",
						Metadata: map[string]any{"weight": uint32(15)},
					},
				}
				readySCs[k2] = base.SubConnInfo{
					Address: resolver.Address{
						Addr:     "weight-2",
						Metadata: map[string]any{"weight": uint32(20)},
					},
				}
				readySCs[k3] = base.SubConnInfo{
					Address: resolver.Address{
						Addr:     "weight-3",
						Metadata: map[string]any{"weight": uint32(25)},
					},
				}
				return base.PickerBuildInfo{
					ReadySCs: readySCs,
				}
			},
			b:           NewPickerBuilder(),
			wantWeights: map[string]uint32{"weight-1": 15, "weight-2": 20, "weight-3": 25},
		},
		{
			name: "address attributes",
			buildInfoFn: func() base.PickerBuildInfo {
				readySCs := make(map[balancer.SubConn]base.SubConnInfo, 3)
				k1 := &mockSubConn{id: 1}
				k2 := &mockSubConn{id: 2}
				k3 := &mockSubConn{id: 3}
				readySCs[k1] = base.SubConnInfo{
					Address: resolver.Address{
						Addr:       "weight-1",
						Attributes: attributes.New("weight", uint32(15)),
					},
				}
				readySCs[k2] = base.SubConnInfo{
					Address: resolver.Address{
						Addr:       "weight-2",
						Attributes: attributes.New("weight", uint32(20)),
					},
				}
				readySCs[k3] = base.SubConnInfo{
					Address: resolver.Address{
						Addr:       "weight-3",
						Attributes: attributes.New("weight", uint32(25)),
					},
				}
				return base.PickerBuildInfo{
					ReadySCs: readySCs,
				}
			},
			b: NewPickerBuilder(WithGetWeightFunc(func(ci base.SubConnInfo) uint32 {
				weight := ci.Address.Attributes.Value("weight").(uint32)
				return weight
			})),
			wantWeights: map[string]uint32{"weight-1": 15, "weight-2": 20, "weight-3": 25},
		},
	}
	for _, tc := range testCases {
		tt := tc
		t.Run(tt.name, func(t *testing.T) {
			p := tt.b.Build(tt.buildInfoFn()).(*Picker)
			targetWeights := make(map[string]uint32, len(p.connections))
			for _, c := range p.connections {
				targetWeights[c.name] = c.weight
			}
			assert.Equal(t, targetWeights, tt.wantWeights)
		})
	}
}

func TestWeightBalancer_Pick(t *testing.T) {
	b := &Picker{
		connections: []*conn{
			{
				name:            "weight-5",
				weight:          5,
				efficientWeight: 5,
				currentWeight:   5,
			},
			{
				name:            "weight-4",
				weight:          4,
				efficientWeight: 4,
				currentWeight:   4,
			},
			{
				name:            "weight-3",
				weight:          3,
				efficientWeight: 3,
				currentWeight:   3,
			},
		},
	}
	pickRes, err := b.Pick(balancer.PickInfo{})
	require.NoError(t, err)
	assert.Equal(t, "weight-5", pickRes.SubConn.(*conn).name)

	pickRes, err = b.Pick(balancer.PickInfo{})
	require.NoError(t, err)
	assert.Equal(t, "weight-4", pickRes.SubConn.(*conn).name)

	pickRes, err = b.Pick(balancer.PickInfo{})
	require.NoError(t, err)
	assert.Equal(t, "weight-3", pickRes.SubConn.(*conn).name)

	pickRes, err = b.Pick(balancer.PickInfo{})
	require.NoError(t, err)
	assert.Equal(t, "weight-5", pickRes.SubConn.(*conn).name)

	pickRes, err = b.Pick(balancer.PickInfo{})
	require.NoError(t, err)
	assert.Equal(t, "weight-4", pickRes.SubConn.(*conn).name)

	pickRes.Done(balancer.DoneInfo{})
	// 断言这里面 efficient weight 是变化了的
}

var _ balancer.SubConn = (*mockSubConn)(nil)

type mockSubConn struct{ id int }

func (m *mockSubConn) UpdateAddresses(addresses []resolver.Address) {
	return
}

func (m *mockSubConn) Connect() {
	return
}

func (m *mockSubConn) GetOrBuildProducer(builder balancer.ProducerBuilder) (p balancer.Producer, close func()) {
	return
}

func (m *mockSubConn) Shutdown() {
	return
}
