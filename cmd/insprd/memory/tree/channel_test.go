package tree

import (
	"reflect"
	"testing"

	"gitlab.inspr.dev/inspr/core/cmd/insprd/memory"
	"gitlab.inspr.dev/inspr/core/pkg/ierrors"
	"gitlab.inspr.dev/inspr/core/pkg/meta"
)

func TestTreeMemoryManager_Channels(t *testing.T) {
	type fields struct {
		root *meta.App
	}
	tests := []struct {
		name   string
		fields fields
		want   memory.ChannelMemory
	}{
		{
			name: "It should return a pointer to ChannelMemoryManager.",
			fields: fields{
				root: getMockChannels(),
			},
			want: &ChannelMemoryManager{
				root: getMockChannels(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmm := &TreeMemoryManager{
				root: tt.fields.root,
			}
			if got := tmm.Channels(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TreeMemoryManager.Channels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChannelMemoryManager_GetChannel(t *testing.T) {
	type fields struct {
		root   *meta.App
		appErr error
		mockA  bool
		mockC  bool
		mockCT bool
	}
	type args struct {
		context string
		chName  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *meta.Channel
		wantErr bool
	}{
		{
			name: "It should return a valid Channel",
			fields: fields{
				root:   getMockChannels(),
				appErr: nil,
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				context: "",
				chName:  "channel1",
			},
			wantErr: false,
			want: &meta.Channel{
				Meta: meta.Metadata{
					Name:   "channel1",
					Parent: "",
				},
				Spec: meta.ChannelSpec{},
			},
		},
		{
			name: "It should return a invalid Channel on a valid App",
			fields: fields{
				root:   getMockChannels(),
				appErr: nil,
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				context: "",
				chName:  "channel3",
			},
			wantErr: true,
		},
		{
			name: "It should return a invalid Channel on a invalid App",
			fields: fields{
				root:   getMockChannels(),
				appErr: ierrors.NewError().NotFound().Build(),
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				context: "invalid.context",
				chName:  "channel1",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setTree(&TreeMockManager{
				root:   tt.fields.root,
				appErr: tt.fields.appErr,
				mockC:  tt.fields.mockC,
				mockA:  tt.fields.mockA,
				mockCT: tt.fields.mockCT,
			})
			chh := GetTreeMemory().Channels()
			got, err := chh.GetChannel(tt.args.context, tt.args.chName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChannelMemoryManager.GetChannel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChannelMemoryManager.GetChannel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChannelMemoryManager_CreateChannel(t *testing.T) {
	type fields struct {
		root   *meta.App
		appErr error
		mockA  bool
		mockC  bool
		mockCT bool
	}
	type args struct {
		context string
		ch      *meta.Channel
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    *meta.Channel
	}{
		{
			name: "It should create a new Channel on a valid App",
			fields: fields{
				root:   getMockChannels(),
				appErr: nil,
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				context: "",
				ch: &meta.Channel{
					Meta: meta.Metadata{
						Name:   "channel3",
						Parent: "",
					},
					Spec: meta.ChannelSpec{},
				},
			},
			wantErr: false,
			want: &meta.Channel{
				Meta: meta.Metadata{
					Name:   "channel3",
					Parent: "",
				},
				Spec: meta.ChannelSpec{},
			},
		},
		{
			name: "It should not create a new Channel because it already exists",
			fields: fields{
				root:   getMockChannels(),
				appErr: nil,
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				context: "",
				ch: &meta.Channel{
					Meta: meta.Metadata{
						Name:   "channel1",
						Parent: "",
					},
					Spec: meta.ChannelSpec{},
				},
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "It should not create a new Channel because the context is invalid",
			fields: fields{
				root:   getMockChannels(),
				appErr: ierrors.NewError().NotFound().Build(),
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				context: "invalid.context",
				ch: &meta.Channel{
					Meta: meta.Metadata{
						Name:   "channel3",
						Parent: "",
					},
					Spec: meta.ChannelSpec{},
				},
			},
			wantErr: true,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setTree(&TreeMockManager{
				root:   tt.fields.root,
				appErr: tt.fields.appErr,
				mockC:  tt.fields.mockC,
				mockA:  tt.fields.mockA,
				mockCT: tt.fields.mockCT,
			})
			chh := GetTreeMemory().Channels()
			err := chh.CreateChannel(tt.args.context, tt.args.ch)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChannelMemoryManager.CreateChannel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil {
				got, err := chh.GetChannel(tt.args.context, tt.want.Meta.Name)
				if (err != nil) || !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ChannelMemoryManager.GetChannel() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestChannelMemoryManager_DeleteChannel(t *testing.T) {
	type fields struct {
		root   *meta.App
		appErr error
		mockA  bool
		mockC  bool
		mockCT bool
	}
	type args struct {
		context string
		chName  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    *meta.Channel
	}{
		{
			name: "It should delete a Channel on a valid App",
			fields: fields{
				root:   getMockChannels(),
				appErr: nil,
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				context: "",
				chName:  "channel1",
			},
			wantErr: false,
			want:    nil,
		},
		{
			name: "It should not delete the channel, because it does not exist",
			fields: fields{
				root:   getMockChannels(),
				appErr: nil,
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				chName:  "channel3",
				context: "",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "It shoud not delete the Channel because the context is invalid.",
			fields: fields{
				root:   nil,
				appErr: ierrors.NewError().NotFound().Build(),
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				context: "invalid.context",
				chName:  "channel1",
			},
			wantErr: true,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setTree(&TreeMockManager{
				root:   tt.fields.root,
				appErr: tt.fields.appErr,
				mockC:  tt.fields.mockC,
				mockA:  tt.fields.mockA,
				mockCT: tt.fields.mockCT,
			})
			chh := GetTreeMemory().Channels()
			if err := chh.DeleteChannel(tt.args.context, tt.args.chName); (err != nil) != tt.wantErr {
				t.Errorf("ChannelMemoryManager.DeleteChannel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, _ := chh.GetChannel(tt.args.context, tt.args.chName)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChannelMemoryManager.GetChannel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChannelMemoryManager_UpdateChannel(t *testing.T) {
	type fields struct {
		root   *meta.App
		appErr error
		mockA  bool
		mockC  bool
		mockCT bool
	}
	type args struct {
		context string
		ch      *meta.Channel
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    *meta.Channel
	}{
		{
			name: "It should update a Channel on a valid App",
			fields: fields{
				root:   getMockChannels(),
				appErr: nil,
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				context: "",
				ch: &meta.Channel{
					Meta: meta.Metadata{
						Name: "channel1",
						Annotations: map[string]string{
							"update1": "update1",
						},
						Parent: "",
					},
					Spec: meta.ChannelSpec{},
				},
			},
			wantErr: false,
			want: &meta.Channel{
				Meta: meta.Metadata{
					Name: "channel1",
					Annotations: map[string]string{
						"update1": "update1",
					},
					Parent: "",
				},
				Spec: meta.ChannelSpec{},
			},
		},
		{
			name: "It should not update a Channel because it does not exist",
			fields: fields{
				root:   getMockChannels(),
				appErr: nil,
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				context: "",
				ch: &meta.Channel{
					Meta: meta.Metadata{
						Name: "channel3",
						Annotations: map[string]string{
							"update1": "update1",
						},
						Parent: "",
					},
					Spec: meta.ChannelSpec{},
				},
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "It should not update a Channel because the context is invalid",
			fields: fields{
				root:   getMockChannels(),
				appErr: ierrors.NewError().NotFound().Build(),
				mockA:  true,
				mockC:  false,
				mockCT: true,
			},
			args: args{
				context: "invalid.context",
				ch: &meta.Channel{
					Meta: meta.Metadata{
						Name: "channel1",
						Annotations: map[string]string{
							"update1": "update1",
						},
						Parent: "",
					},
					Spec: meta.ChannelSpec{},
				},
			},
			wantErr: true,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setTree(&TreeMockManager{
				root:   tt.fields.root,
				appErr: tt.fields.appErr,
				mockC:  tt.fields.mockC,
				mockA:  tt.fields.mockA,
				mockCT: tt.fields.mockCT,
			})
			chh := GetTreeMemory().Channels()
			if err := chh.UpdateChannel(tt.args.context, tt.args.ch); (err != nil) != tt.wantErr {
				t.Errorf("ChannelMemoryManager.UpdateChannel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil {
				got, err := chh.GetChannel(tt.args.context, tt.want.Meta.Name)
				if (err != nil) || !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ChannelMemoryManager.GetChannel() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func getMockChannels() *meta.App {
	root := meta.App{
		Meta: meta.Metadata{
			Name:        "",
			Reference:   "",
			Annotations: map[string]string{},
			Parent:      "",
			SHA256:      "",
		},
		Spec: meta.AppSpec{
			Node: meta.Node{},
			Apps: map[string]*meta.App{
				"app1": {},
				"app2": {},
			},
			Channels: map[string]*meta.Channel{
				"channel1": {
					Meta: meta.Metadata{
						Name:   "channel1",
						Parent: "",
					},
					Spec: meta.ChannelSpec{},
				},
				"channel2": {
					Meta: meta.Metadata{
						Name:   "channel2",
						Parent: "",
					},
					Spec: meta.ChannelSpec{},
				},
			},
			ChannelTypes: map[string]*meta.ChannelType{},
			Boundary: meta.AppBoundary{
				Input:  []string{},
				Output: []string{},
			},
		},
	}
	return &root
}
