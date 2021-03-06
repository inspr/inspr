package cli

import (
	"errors"
	"testing"

	"gopkg.in/yaml.v2"
	cliutils "inspr.dev/inspr/pkg/cmd/utils"
	"inspr.dev/inspr/pkg/ierrors"
	"inspr.dev/inspr/pkg/meta"
)

func TestNewApplyChannel(t *testing.T) {
	prepareToken(t)
	chanWithoutNameBytes, _ := yaml.Marshal(meta.Channel{})
	chanDefaultBytes, _ := yaml.Marshal(meta.Channel{Meta: meta.Metadata{Name: "mock"}})
	type args struct {
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
				b: chanDefaultBytes,
			},
			want: nil,
		},
		{
			name: "channel_without_name",
			args: args{
				b: chanWithoutNameBytes,
			},
			want: ierrors.New("channel without name"),
		},
		{
			name: "error_testing",
			args: args{
				b: chanDefaultBytes,
			},
			want: errors.New("new error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cliutils.SetMockedClient(tt.want)
			got := NewApplyChannel()

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
