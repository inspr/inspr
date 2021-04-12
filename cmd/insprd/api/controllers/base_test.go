package controller

import (
	"reflect"
	"testing"

	"gitlab.inspr.dev/inspr/core/cmd/insprd/memory"
	"gitlab.inspr.dev/inspr/core/cmd/insprd/memory/fake"
	"gitlab.inspr.dev/inspr/core/cmd/insprd/operators"
	ofake "gitlab.inspr.dev/inspr/core/cmd/insprd/operators/fake"
)

func TestServer_Init(t *testing.T) {
	type args struct {
		mm memory.Manager
		op operators.OperatorInterface
	}
	tests := []struct {
		name string
		s    *Server
		args args
	}{
		{
			name: "successful_server_init",
			s:    &Server{},
			args: args{
				mm: fake.MockMemoryManager(nil),
				op: ofake.NewFakeOperator(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.Init(tt.args.mm, tt.args.op)
			if !reflect.DeepEqual(tt.s.MemoryManager, fake.MockMemoryManager(nil)) {
				t.Errorf("TestServer_Init() = %v, want %v", tt.s.MemoryManager, nil)
			}
		})
	}
}