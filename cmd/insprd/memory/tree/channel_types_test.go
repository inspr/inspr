package tree

import (
	"reflect"
	"testing"

	"gitlab.inspr.dev/inspr/core/cmd/insprd/memory"
	"gitlab.inspr.dev/inspr/core/pkg/ierrors"
	"gitlab.inspr.dev/inspr/core/pkg/meta"
	metautils "gitlab.inspr.dev/inspr/core/pkg/meta/utils"
)

func TestMemoryManager_ChannelTypes(t *testing.T) {
	type fields struct {
		root *meta.App
	}
	tests := []struct {
		name   string
		fields fields
		want   memory.ChannelTypeMemory
	}{
		{
			name: "creating a ChannelTypeMemortMannager",
			fields: fields{
				root: getMockChannelTypes(),
			},
			want: &ChannelTypeMemoryManager{
				&MemoryManager{
					root: getMockChannelTypes(),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmm := &MemoryManager{
				root: tt.fields.root,
			}
			if got := tmm.ChannelTypes(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MemoryManager.ChannelTypes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChannelTypeMemoryManager_GetChannelType(t *testing.T) {
	type fields struct {
		root   *meta.App
		appErr error
		mockA  bool
		mockC  bool
		mockCT bool
	}
	type args struct {
		context string
		ctName  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *meta.ChannelType
		wantErr bool
	}{
		{
			name: "Getting a valid ChannelType on a valid app",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: nil,
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				context: "",
				ctName:  "ct1",
			},
			wantErr: false,
			want: &meta.ChannelType{
				Meta: meta.Metadata{
					Name:        "ct1",
					Reference:   "ct1",
					Annotations: map[string]string{},
					Parent:      "",
					UUID:        "",
				},
				Schema: "",
			},
		},
		{
			name: "Getting a invalid ChannelType on a valid app",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: nil,
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				context: "",
				ctName:  "ct4",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "Getting any ChannelType on a invalid app",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: ierrors.NewError().NotFound().Build(),
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				context: "invalid.context",
				ctName:  "ct42",
			},
			wantErr: true,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setTree(&MockManager{
				MemoryManager: &MemoryManager{
					root: tt.fields.root,
				},
				appErr: tt.fields.appErr,
				mockC:  tt.fields.mockC,
				mockA:  tt.fields.mockA,
				mockCT: tt.fields.mockCT,
			})
			ctm := GetTreeMemory().ChannelTypes()
			got, err := ctm.Get(tt.args.context, tt.args.ctName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChannelTypeMemoryManager.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChannelTypeMemoryManager.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChannelTypeMemoryManager_Create(t *testing.T) {
	type fields struct {
		root   *meta.App
		appErr error
		mockA  bool
		mockC  bool
		mockCT bool
	}
	type args struct {
		ct      *meta.ChannelType
		context string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    *meta.ChannelType
	}{
		{
			name: "Creating a new ChannelType on a valid app",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: nil,
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				ct: &meta.ChannelType{
					Meta: meta.Metadata{
						Name:        "ct4",
						Reference:   "ct1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Schema: "",
				},
				context: "",
			},
			wantErr: false,
			want: &meta.ChannelType{
				Meta: meta.Metadata{
					Name:        "ct4",
					Reference:   "ct1",
					Annotations: map[string]string{},
					Parent:      "",
					UUID:        "",
				},
				Schema: "",
			},
		},
		{
			name: "Trying to create an old ChannelType on a valid app",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: nil,
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				ct: &meta.ChannelType{
					Meta: meta.Metadata{
						Name:        "ct1",
						Reference:   "ct1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Schema: "",
				},
				context: "",
			},
			wantErr: true,
			want: &meta.ChannelType{
				Meta: meta.Metadata{
					Name:        "ct1",
					Reference:   "ct1",
					Annotations: map[string]string{},
					Parent:      "",
					UUID:        "",
				},
				Schema: "",
			},
		},
		{
			name: "Trying to create an ChannelType on a invalid app",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: ierrors.NewError().NotFound().Build(),
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				ct: &meta.ChannelType{
					Meta: meta.Metadata{
						Name:        "ct1",
						Reference:   "ct1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Schema: "",
				},
				context: "invalid.context",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "Invalid name - doesn't create channel type",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: nil,
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				ct: &meta.ChannelType{
					Meta: meta.Metadata{
						Name:        "-ct3-",
						Reference:   "ct1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Schema: "",
				},
				context: "",
			},
			wantErr: true,
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setTree(&MockManager{
				MemoryManager: &MemoryManager{
					root: tt.fields.root,
				},
				appErr: tt.fields.appErr,
				mockC:  tt.fields.mockC,
				mockA:  tt.fields.mockA,
				mockCT: tt.fields.mockCT,
			})
			ctm := GetTreeMemory().ChannelTypes()
			err := ctm.Create(tt.args.context, tt.args.ct)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChannelTypeMemoryManager.Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil {
				got, err := ctm.Get(tt.args.context, tt.want.Meta.Name)
				if !tt.wantErr {
					if !metautils.ValidateUUID(got.Meta.UUID) {
						t.Errorf("ChannelTypeMemoryManager.Create() invalid UUID, uuid=%v", got.Meta.UUID)
					}
				}
				if (err != nil) || !metautils.CompareWithoutUUID(got, tt.want) {
					t.Errorf("ChannelTypeMemoryManager.Create() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestChannelTypeMemoryManager_Delete(t *testing.T) {
	type fields struct {
		root   *meta.App
		appErr error
		mockA  bool
		mockC  bool
		mockCT bool
	}
	type args struct {
		context string
		ctName  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    *meta.ChannelType
	}{
		{
			name: "Deleting a valid ChannelType on a valid app",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: nil,
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				ctName:  "ct1",
				context: "",
			},
			wantErr: false,
			want:    nil,
		},
		{
			name: "Deleting a invalid ChannelType on a valid app",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: nil,
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				ctName:  "ct4",
				context: "",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "Deleting any ChannelType on a invalid app",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: ierrors.NewError().NotFound().Build(),
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				context: "invalid.context",
				ctName:  "ct42",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "It should not delete the channelType because it's been used by a channel",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: nil,
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				context: "",
				ctName:  "ct3",
			},
			wantErr: true,
			want: &meta.ChannelType{
				Meta: meta.Metadata{
					Name:        "ct3",
					Reference:   "ct3",
					Annotations: map[string]string{},
					Parent:      "",
					UUID:        "",
				},
				ConnectedChannels: []string{"channel1"},
				Schema:            "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setTree(&MockManager{
				MemoryManager: &MemoryManager{
					root: tt.fields.root,
				},
				appErr: tt.fields.appErr,
				mockC:  tt.fields.mockC,
				mockA:  tt.fields.mockA,
				mockCT: tt.fields.mockCT,
			})
			ctm := GetTreeMemory().ChannelTypes()
			if err := ctm.Delete(tt.args.context, tt.args.ctName); (err != nil) != tt.wantErr {
				t.Errorf("ChannelTypeMemoryManager.Delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got, _ := ctm.Get(tt.args.context, tt.args.ctName)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ChannelTypeMemoryManager.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChannelTypeMemoryManager_Update(t *testing.T) {
	type fields struct {
		root   *meta.App
		appErr error
		mockA  bool
		mockC  bool
		mockCT bool
	}
	type args struct {
		ct      *meta.ChannelType
		context string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    *meta.ChannelType
	}{
		{
			name: "Updating a valid ChannelType on a valid app",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: nil,
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				ct: &meta.ChannelType{
					Meta: meta.Metadata{
						Name:        "ct1",
						Reference:   "ct1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Schema: string([]byte{0, 1, 0, 1}),
				},
				context: "",
			},
			wantErr: false,
			want: &meta.ChannelType{
				Meta: meta.Metadata{
					Name:        "ct1",
					Reference:   "ct1",
					Annotations: map[string]string{},
					Parent:      "",
					UUID:        "",
				},
				Schema: string([]byte{0, 1, 0, 1}),
			},
		},
		{
			name: "Updating a invalid ChannelType on a valid app",
			fields: fields{
				root:   getMockChannelTypes(),
				appErr: nil,
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				ct: &meta.ChannelType{
					Meta: meta.Metadata{
						Name:        "ct42",
						Reference:   "ct42",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Schema: "",
				},
				context: "",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "Updating any ChannelType on a invalid app",
			fields: fields{
				root:   nil,
				appErr: ierrors.NewError().NotFound().Build(),
				mockA:  true,
				mockC:  true,
				mockCT: false,
			},
			args: args{
				ct: &meta.ChannelType{
					Meta: meta.Metadata{
						Name:        "ct3",
						Reference:   "ct1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Schema: "",
				},
				context: "invalid.context",
			},
			wantErr: true,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setTree(&MockManager{
				MemoryManager: &MemoryManager{
					root: tt.fields.root,
				},
				appErr: tt.fields.appErr,
				mockC:  tt.fields.mockC,
				mockA:  tt.fields.mockA,
				mockCT: tt.fields.mockCT,
			})
			ctm := GetTreeMemory().ChannelTypes()
			if err := ctm.Update(tt.args.context, tt.args.ct); (err != nil) != tt.wantErr {
				t.Errorf("ChannelTypeMemoryManager.Update() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil {
				got, err := ctm.Get(tt.args.context, tt.want.Meta.Name)
				if (err != nil) || !metautils.CompareWithUUID(got, tt.want) {
					t.Errorf("ChannelTypeMemoryManager.Get() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func getMockChannelTypes() *meta.App {
	root := meta.App{
		Meta: meta.Metadata{
			Name:        "",
			Reference:   "",
			Annotations: map[string]string{},
			Parent:      "",
			UUID:        "",
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
					Spec: meta.ChannelSpec{
						Type: "ct3",
					},
				},
			},
			ChannelTypes: map[string]*meta.ChannelType{
				"ct1": {
					Meta: meta.Metadata{
						Name:        "ct1",
						Reference:   "ct1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Schema: "",
				},
				"ct2": {
					Meta: meta.Metadata{
						Name:        "ct2",
						Reference:   "ct2",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Schema: "",
				},
				"ct3": {
					Meta: meta.Metadata{
						Name:        "ct3",
						Reference:   "ct3",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					ConnectedChannels: []string{"channel1"},
					Schema:            "",
				},
			},
			Boundary: meta.AppBoundary{
				Input:  []string{},
				Output: []string{},
			},
		},
	}
	return &root
}
