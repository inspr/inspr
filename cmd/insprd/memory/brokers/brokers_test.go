package brokers

import (
	"reflect"
	"sync"
	"testing"

	"inspr.dev/inspr/cmd/sidecars"
	apimodels "inspr.dev/inspr/pkg/api/models"
	"inspr.dev/inspr/pkg/meta/brokers"
)

var kafkaStructMock = sidecars.KafkaConfig{
	BootstrapServers: "",
	AutoOffsetReset:  "",
	KafkaInsprAddr:   "",
	SidecarImage:     "",
}

func TestBrokersMemoryManager_Get(t *testing.T) {
	tests := []struct {
		name    string
		want    *apimodels.BrokersDI
		wantErr bool
	}{
		{
			name: "getall from empty brokerMM",
			want: &apimodels.BrokersDI{
				Available: []string{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetBrokers()
			bmm := GetBrokerMemory()
			got, err := bmm.Get()

			if (err != nil) != tt.wantErr {
				t.Errorf("BrokersMemoryManager.GetAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BrokersMemoryManager.GetAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBrokersMemoryManager_get(t *testing.T) {
	tests := []struct {
		name    string
		bmm     *brokerMemoryManager
		want    *brokers.Brokers
		wantErr bool
	}{
		{
			name: "get from instanciated singleton",
			bmm: &brokerMemoryManager{
				broker: &brokers.Brokers{
					Available: brokers.BrokerStatusArray{
						"brk1": nil,
						"brk2": nil,
						"brk3": nil,
					},
					Default: "brk1",
				},
			},
			want: &brokers.Brokers{
				Available: brokers.BrokerStatusArray{
					"brk1": nil,
					"brk2": nil,
					"brk3": nil,
				},
				Default: "brk1",
			},
			wantErr: false,
		},
		{
			name: "get from nil singleton memory",
			bmm: &brokerMemoryManager{
				broker: nil,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetBrokers()
			got, err := tt.bmm.get()
			if (err != nil) != tt.wantErr {
				t.Errorf("BrokersMemoryManager.get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BrokersMemoryManager.get() = %v, want %v", got, tt.want)
			}
		})
	}
}

type mockedConfigs struct {
	broker string
}

func (mb mockedConfigs) Broker() string {
	return mb.broker
}

func TestBrokersMemoryManager_Create_and_SetDefault(t *testing.T) {
	resetBrokers()

	tests := []struct {
		name    string
		bmm     *brokerMemoryManager
		exec    func(bmm Manager) error
		wantErr bool
	}{
		{
			name: "invalid create - broker not supported",
			bmm:  &brokerMemoryManager{},
			exec: func(bmm Manager) error {
				return bmm.Create(mockedConfigs{broker: "brk1"})
			},
			wantErr: true,
		},
		{
			name: "valid create",
			bmm:  &brokerMemoryManager{},
			exec: func(bmm Manager) error {
				return bmm.Create(&kafkaStructMock)
			},
		},
		{
			name: "invalid create - broker already exists",
			bmm:  &brokerMemoryManager{},
			exec: func(bmm Manager) error {
				return bmm.Create(&kafkaStructMock)
			},
			wantErr: true,
		},
		{
			name: "invalid setdefault",
			bmm:  &brokerMemoryManager{},
			exec: func(bmm Manager) error {
				return bmm.SetDefault("brk1")
			},
			wantErr: true,
		},
		{
			name: "valid setdefault",
			bmm:  &brokerMemoryManager{},
			exec: func(bmm Manager) error {
				return bmm.SetDefault(brokers.Kafka)
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bmm := GetBrokerMemory()
			if tt.exec != nil {
				if err := tt.exec(bmm); (err != nil) != tt.wantErr {
					t.Errorf("BrokersMemoryManager method error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}
		})
	}
}

func resetBrokers() {
	brokerMemory = nil
}

func Test_brokerMemoryManager_Configs(t *testing.T) {
	type fields struct {
		factory SidecarManager
		broker  *brokers.Brokers
	}
	type args struct {
		broker string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    brokers.BrokerConfiguration
		wantErr bool
	}{
		{
			name: "valid config get",
			fields: fields{
				factory: nil,
				broker: &brokers.Brokers{
					Default: "brk1",
					Available: brokers.BrokerStatusArray{
						"brk1": nil,
					},
				},
			},
			args: args{
				broker: "brk1",
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "invalid config get",
			fields: fields{
				factory: nil,
				broker: &brokers.Brokers{
					Default: "brk1",
					Available: brokers.BrokerStatusArray{
						"brk1": nil,
					},
				},
			},
			args: args{
				broker: "brk2",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bmm := &brokerMemoryManager{
				factory:   tt.fields.factory,
				broker:    tt.fields.broker,
				available: sync.Mutex{},
				def:       sync.Mutex{},
			}
			got, err := bmm.Configs(tt.args.broker)
			if (err != nil) != tt.wantErr {
				t.Errorf("brokerMemoryManager.Configs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("brokerMemoryManager.Configs() = %v, want %v", got, tt.want)
			}
		})
	}
}
