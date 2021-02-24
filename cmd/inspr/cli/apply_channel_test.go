package cli

import (
	"errors"
	"testing"

	"gitlab.inspr.dev/inspr/core/pkg/controller"
	"gitlab.inspr.dev/inspr/core/pkg/controller/mocks"
	"gitlab.inspr.dev/inspr/core/pkg/ierrors"
	"gitlab.inspr.dev/inspr/core/pkg/meta"
	"gopkg.in/yaml.v2"
)

func TestNewApplyChannel(t *testing.T) {
	chanWithoutNameBytes, _ := yaml.Marshal(meta.Channel{})
	chanDefaultBytes, _ := yaml.Marshal(meta.Channel{Meta: meta.Metadata{Name: "mock"}})
	type args struct {
		c controller.ChannelInterface
		b []byte
	}
	tests := []struct {
		name string
		args args
		want error
	}{
		{
			name: "default_test",
			args: args{
				c: mocks.NewChannelMock(nil),
				b: chanDefaultBytes,
			},
			want: nil,
		},
		{
			name: "channel_without_name",
			args: args{
				c: mocks.NewChannelMock(nil),
				b: chanWithoutNameBytes,
			},
			want: ierrors.NewError().Message("channel without name").Build(),
		},
		{
			name: "error_testing",
			args: args{
				c: mocks.NewChannelMock(errors.New("new error")),
				b: chanDefaultBytes,
			},
			want: errors.New("new error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewApplyChannel(tt.args.c)

			r := got(tt.args.b, nil)

			if r != nil && tt.want != nil {
				if r.Error() != tt.want.Error() {
					t.Errorf("NewApplyChannel() = %v, want %v", r.Error(), tt.want.Error())
				}
			} else {
				if r != tt.want {
					t.Errorf("NewApplyChannel() = %v, want %v", r, tt.want)
				}
			}
		})
	}
}
