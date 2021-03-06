package tree

import (
	"reflect"
	"testing"

	"inspr.dev/inspr/cmd/sidecars"
	apimodels "inspr.dev/inspr/pkg/api/models"
	"inspr.dev/inspr/pkg/meta"
	metautils "inspr.dev/inspr/pkg/meta/utils"
	"inspr.dev/inspr/pkg/utils"
)

func Test_validAppStructure(t *testing.T) {
	type fields struct {
		root *meta.App
	}
	type args struct {
		app       meta.App
		parentApp meta.App
		brokers   *apimodels.BrokersDI
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "All valid structures",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"some_broker"},
					Default:   "some_broker",
				},
				app: meta.App{
					Meta: meta.Metadata{
						Name:        "app5",
						Reference:   "app2.app5",
						Annotations: map[string]string{},
						Parent:      "app2",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name:        "nodeApp5",
								Reference:   "app5.nodeApp5",
								Annotations: map[string]string{},
								Parent:      "app2",
								UUID:        "",
							},
							Spec: meta.NodeSpec{
								Image: "imageNodeApp5",
							},
						},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1app2"},
								Output: []string{"ch2app2"},
							},
						},
					},
				},
				parentApp: *getMockApp().Spec.Apps["app2"],
			},
		},
		{
			name: "invalidapp name - empty",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"some_broker"},
					Default:   "some_broker",
				},
				app: meta.App{
					Meta: meta.Metadata{
						Name:        "",
						Reference:   "app2.app4",
						Annotations: map[string]string{},
						Parent:      "app2",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name:        "nodeApp4",
								Reference:   "app4.nodeApp4",
								Annotations: map[string]string{},
								Parent:      "app3",
								UUID:        "",
							},
							Spec: meta.NodeSpec{
								Image: "imageNodeApp3",
							},
						},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1app2"},
								Output: []string{"ch2app2"},
							},
						},
					},
				},
				parentApp: *getMockApp().Spec.Apps["app2"],
			},
			wantErr: true,
		},
		{
			name: "invalidapp substructure",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"some_broker"},
					Default:   "some_broker",
				},
				app: meta.App{
					Meta: meta.Metadata{
						Name:        "app5",
						Reference:   "app2.app5",
						Annotations: map[string]string{},
						Parent:      "app2",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name:        "nodeApp5",
								Reference:   "app5.nodeApp5",
								Annotations: map[string]string{},
								Parent:      "app5",
								UUID:        "",
							},
							Spec: meta.NodeSpec{
								Image: "imageNodeApp5",
							},
						},
						Apps: map[string]*meta.App{
							"invalidApp": {},
						},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1app2"},
								Output: []string{"ch2app2"},
							},
						},
					},
				},
				parentApp: *getMockApp().Spec.Apps["app2"],
			},
			wantErr: true,
		},
		{
			name: "invalidapp - parent has Node structure",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"some_broker"},
					Default:   "some_broker",
				},
				app: meta.App{
					Meta: meta.Metadata{
						Name:        "app4",
						Reference:   "app2.app4",
						Annotations: map[string]string{},
						Parent:      "app2",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name:        "nodeApp4",
								Reference:   "app4.nodeApp4",
								Annotations: map[string]string{},
								Parent:      "app3",
								UUID:        "",
							},
							Spec: meta.NodeSpec{
								Image: "imageNodeApp3",
							},
						},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1app1"},
								Output: []string{"ch2app1"},
							},
						},
					},
				},
				parentApp: *getMockApp().Spec.Apps["appNode"],
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			amm := &AppMemoryManager{
				treeMemoryManager: &treeMemoryManager{
					root: tt.fields.root,
					tree: tt.fields.root,
				},
			}

			err := amm.validAppStructure(&tt.args.app, &tt.args.parentApp, tt.args.brokers)
			if tt.wantErr && (err == nil) {
				t.Errorf("validAppStructure(): wanted error but received 'nil'")
				return
			}

			if !tt.wantErr && (err != nil) {
				t.Errorf("validAppStructure() error: %v", reflect.TypeOf(err))
			}
		})
	}
}

func Test_nodeIsEmpty(t *testing.T) {
	type args struct {
		node meta.Node
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Node is empty",
			args: args{
				node: meta.Node{},
			},
			want: true,
		},
		{
			name: "Node isn't empty",
			args: args{
				node: meta.Node{
					Meta: meta.Metadata{
						Name:        "nodeApp1",
						Reference:   "app1.nodeApp1",
						Annotations: map[string]string{},
						Parent:      "app1",
						UUID:        "",
					},
					Spec: meta.NodeSpec{
						Image: "imageNodeApp1",
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nodeIsEmpty(tt.args.node); got != tt.want {
				t.Errorf("nodeIsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getParentApp(t *testing.T) {
	type fields struct {
		root *meta.App
	}
	type args struct {
		sonQuery string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *meta.App
		wantErr bool
	}{
		{
			name: "Parent is the root",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				sonQuery: "app1",
			},
			wantErr: false,
			want:    getMockApp(),
		},
		{
			name: "Parent is another app",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				sonQuery: "app2.app3",
			},
			wantErr: false,
			want:    getMockApp().Spec.Apps["app2"],
		},
		{
			name: "invalidquery",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				sonQuery: "invalid.query",
			},
			wantErr: true,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmm := &treeMemoryManager{
				root: tt.fields.root,
				tree: tt.fields.root,
			}
			got, err := getParentApp(tt.args.sonQuery, tmm)
			if (err != nil) != tt.wantErr {
				t.Errorf("getParentApp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !metautils.CompareWithoutUUID(got, tt.want) {
				t.Errorf("getParentApp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkAndUpdates(t *testing.T) {
	type args struct {
		app     *meta.App
		brokers *apimodels.BrokersDI
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "valid channel structure - it shouldn't return a error",
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"some_broker"},
					Default:   "some_broker",
				},
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app1",
						Reference:   "app1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{},
						Apps: map[string]*meta.App{
							"thenewapp": {
								Meta: meta.Metadata{
									Name:        "thenewapp",
									Reference:   "app1.thenewapp",
									Annotations: map[string]string{},
									Parent:      "app1",
									UUID:        "",
								},
								Spec: meta.AppSpec{
									Apps:     map[string]*meta.App{},
									Channels: map[string]*meta.Channel{},
									Types:    map[string]*meta.Type{},
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input:  []string{"ch1app1"},
											Output: []string{},
										},
									},
								},
							},
						},
						Channels: map[string]*meta.Channel{
							"ch1app1": {
								Meta: meta.Metadata{
									Name:   "ch1app1",
									Parent: "",
								},
								ConnectedApps: []string{"thenewapp"},
								Spec: meta.ChannelSpec{
									Type: "newType",
								},
							},
							"ch2app1": {
								Meta: meta.Metadata{
									Name:   "ch2app1",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
						},
						Types: map[string]*meta.Type{
							"newType": {
								Meta: meta.Metadata{
									Name:        "newType",
									Reference:   "app1.newType",
									Annotations: map[string]string{},
									Parent:      "app1",
									UUID:        "",
								},
							},
						},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{},
								Output: []string{},
							},
						},
					},
				},
			},
		},
		{
			name: "invalid channel: using non-existent type",
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"some_broker"},
					Default:   "some_broker",
				},
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app1",
						Reference:   "app1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{},
						Apps: map[string]*meta.App{
							"thenewapp": {
								Meta: meta.Metadata{
									Name:        "thenewapp",
									Reference:   "app1.thenewapp",
									Annotations: map[string]string{},
									Parent:      "app1",
									UUID:        "",
								},
								Spec: meta.AppSpec{
									Apps:     map[string]*meta.App{},
									Channels: map[string]*meta.Channel{},
									Types:    map[string]*meta.Type{},
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input:  []string{"ch1app1"},
											Output: []string{},
										},
									},
								},
							},
						},
						Channels: map[string]*meta.Channel{
							"ch1app1": {
								Meta: meta.Metadata{
									Name:   "ch1app1",
									Parent: "",
								},
								ConnectedApps: []string{"thenewapp"},
								Spec: meta.ChannelSpec{
									Type: "invalidType",
								},
							},
							"ch2app1": {
								Meta: meta.Metadata{
									Name:   "ch2app1",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
						},
						Types: map[string]*meta.Type{
							"newType": {
								Meta: meta.Metadata{
									Name:        "newType",
									Reference:   "app1.newType",
									Annotations: map[string]string{},
									Parent:      "app1",
									UUID:        "",
								},
							},
						},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{},
								Output: []string{},
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid channel structure - it should return a name channel error",
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"some_broker"},
					Default:   "some_broker",
				},
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app1",
						Reference:   "app1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{},
						Apps: map[string]*meta.App{
							"thenewapp": {
								Meta: meta.Metadata{
									Name:        "thenewapp",
									Reference:   "app1.thenewapp",
									Annotations: map[string]string{},
									Parent:      "app1",
									UUID:        "",
								},
								Spec: meta.AppSpec{
									Apps:     map[string]*meta.App{},
									Channels: map[string]*meta.Channel{},
									Types:    map[string]*meta.Type{},
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input:  []string{"ch1app1"},
											Output: []string{},
										},
									},
								},
							},
						},
						Channels: map[string]*meta.Channel{
							"invalid.channel.name": {
								Meta: meta.Metadata{
									Name:   "ch1app1",
									Parent: "",
								},
								ConnectedApps: []string{"thenewapp"},
								Spec: meta.ChannelSpec{
									Type: "newType",
								},
							},
							"ch2app1": {
								Meta: meta.Metadata{
									Name:   "ch2app1",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
						},
						Types: map[string]*meta.Type{
							"newType": {
								Meta: meta.Metadata{
									Name:        "newType",
									Reference:   "app1.newType",
									Annotations: map[string]string{},
									Parent:      "app1",
									UUID:        "",
								},
							},
						},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{},
								Output: []string{},
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "valid channel structure - it shouldn't return a error",
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"some_broker"},
					Default:   "some_broker",
				},
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app1",
						Reference:   "app1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{},
						Apps: map[string]*meta.App{
							"thenewapp": {
								Meta: meta.Metadata{
									Name:        "thenewapp",
									Reference:   "app1.thenewapp",
									Annotations: map[string]string{},
									Parent:      "app1",
									UUID:        "",
								},
								Spec: meta.AppSpec{
									Apps:     map[string]*meta.App{},
									Channels: map[string]*meta.Channel{},
									Types:    map[string]*meta.Type{},
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input:  []string{"ch1app1"},
											Output: []string{},
										},
									},
								},
							},
						},
						Channels: map[string]*meta.Channel{
							"ch1app1": {
								Meta: meta.Metadata{
									Name:   "ch1app1",
									Parent: "",
								},
								ConnectedApps: []string{"thenewapp"},
								Spec: meta.ChannelSpec{
									Type: "newType",
								},
							},
							"ch2app1": {
								Meta: meta.Metadata{
									Name:   "ch2app1",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
						},
						Types: map[string]*meta.Type{
							"invalid.type": {
								Meta: meta.Metadata{
									Name:        "newType",
									Reference:   "app1.newType",
									Annotations: map[string]string{},
									Parent:      "app1",
									UUID:        "",
								},
							},
						},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{},
								Output: []string{},
							},
						},
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkAndUpdates(tt.args.app, tt.args.brokers)
			if tt.wantErr && (err == nil) {
				t.Errorf("checkAndUpdates(): wanted error but received 'nil'")
				return
			}

			if !tt.wantErr && (err != nil) {
				t.Errorf("checkAndUpdates() error: %v", err)
			}
		})
	}
}

func TestAppMemoryManager_addAppInTree(t *testing.T) {
	type fields struct {
		MemoryManager *treeMemoryManager
		root          *meta.App
		appErr        error
		mockA         bool
		mockC         bool
		mockCT        bool
	}
	type args struct {
		app       *meta.App
		parentApp string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *meta.App
	}{
		{
			name: "single level injection",
			args: args{
				app: &meta.App{
					Meta: meta.Metadata{
						Name:   "singleLevelInjection",
						Parent: "",
					},
				},
				parentApp: "",
			},
			fields: fields{
				root: getMockApp(),
			},
			want: &meta.App{
				Meta: meta.Metadata{
					Name:   "singleLevelInjection",
					Parent: "",
				},
			},
		},

		{
			name: "authentication injection",
			args: args{
				app: &meta.App{
					Meta: meta.Metadata{
						Name:   "singleLevelInjection",
						Parent: "",
					},
				},
				parentApp: "",
			},
			fields: fields{
				root: &meta.App{
					Spec: meta.AppSpec{
						Auth: meta.AppAuth{
							Scope:       "",
							Permissions: utils.StringArray{"permission1", "permission2"},
						},
					},
				},
			},
			want: &meta.App{
				Meta: meta.Metadata{
					Name:   "singleLevelInjection",
					Parent: "",
				},
				Spec: meta.AppSpec{
					Auth: meta.AppAuth{
						Scope:       "",
						Permissions: utils.StringArray{"permission1", "permission2"},
					},
				},
			},
		},
		{
			name: "authentication keeping",
			args: args{
				app: &meta.App{
					Meta: meta.Metadata{
						Name:   "singleLevelInjection",
						Parent: "",
					},
					Spec: meta.AppSpec{
						Auth: meta.AppAuth{
							Scope:       "scope",
							Permissions: utils.StringArray{"permission12"},
						},
					},
				},
				parentApp: "",
			},
			fields: fields{
				root: &meta.App{
					Spec: meta.AppSpec{
						Auth: meta.AppAuth{
							Scope:       "",
							Permissions: utils.StringArray{"permission1", "permission2"},
						},
					},
				},
			},
			want: &meta.App{
				Meta: meta.Metadata{
					Name:   "singleLevelInjection",
					Parent: "",
				},
				Spec: meta.AppSpec{
					Auth: meta.AppAuth{
						Scope:       "scope",
						Permissions: utils.StringArray{"permission12"},
					},
				},
			},
		},

		{
			name: "multilevel authentication keeping",
			args: args{
				app: &meta.App{
					Meta: meta.Metadata{
						Name:   "singleLevelInjection",
						Parent: "",
					},
					Spec: meta.AppSpec{
						Apps: map[string]*meta.App{
							"son1": {
								Meta: meta.Metadata{
									Name: "son1",
								},
							},
						},
						Auth: meta.AppAuth{
							Scope:       "scope",
							Permissions: utils.StringArray{"permission12"},
						},
					},
				},
				parentApp: "",
			},
			fields: fields{
				root: &meta.App{
					Spec: meta.AppSpec{
						Auth: meta.AppAuth{
							Scope:       "",
							Permissions: utils.StringArray{"permission1", "permission2"},
						},
					},
				},
			},
			want: &meta.App{
				Meta: meta.Metadata{
					Name:   "singleLevelInjection",
					Parent: "",
				},
				Spec: meta.AppSpec{
					Auth: meta.AppAuth{
						Scope:       "scope",
						Permissions: utils.StringArray{"permission12"},
					},
					Apps: map[string]*meta.App{
						"son1": {
							Meta: meta.Metadata{
								Name: "son1",
							},
							Spec: meta.AppSpec{
								Auth: meta.AppAuth{
									Scope:       "scope",
									Permissions: utils.StringArray{"permission12"},
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := &MockManager{
				treeMemoryManager: &treeMemoryManager{
					root: tt.fields.root,
					tree: tt.fields.root,
				},
				appErr: tt.fields.appErr,
				mockC:  tt.fields.mockC,
				mockA:  tt.fields.mockA,
				mockCT: tt.fields.mockCT,
			}
			amm := mem.Apps().(*AppMemoryManager)
			parentApp, _ := amm.Get(tt.args.parentApp)
			amm.addAppInTree(tt.args.app, parentApp)
		})
	}
}

func TestAppMemoryManager_updateUUID(t *testing.T) {

	type args struct {
		app       *meta.App
		parentStr string
		tree      *meta.App
		want      *meta.App
	}
	tests := []struct {
		name   string
		args   args
		update bool
	}{
		{
			name: "new dapp",
			args: args{
				app: &meta.App{
					Meta: meta.Metadata{
						Name: "dapp1",
					},
					Spec: meta.AppSpec{
						Channels: map[string]*meta.Channel{
							"channel1": {
								Meta: meta.Metadata{Name: "channel1"},
							},
						},
						Types: map[string]*meta.Type{
							"channeltype1": {
								Meta: meta.Metadata{Name: "channel1"},
							},
						},

						Aliases: map[string]*meta.Alias{
							"alias1": {
								Meta: meta.Metadata{Name: "channel1"},
							},
						},
					},
				},
				parentStr: "",
				tree:      &meta.App{},
			},
			update: false,
		},
		{
			name: "updating dapp",
			args: args{
				app: &meta.App{
					Meta: meta.Metadata{
						Name: "dapp1",
					},
					Spec: meta.AppSpec{
						Channels: map[string]*meta.Channel{
							"channel1": {
								Meta: meta.Metadata{Name: "channel1"},
							},
						},
						Types: map[string]*meta.Type{
							"channeltype1": {
								Meta: meta.Metadata{Name: "channel1"},
							},
						},

						Aliases: map[string]*meta.Alias{
							"alias1": {
								Meta: meta.Metadata{Name: "channel1"},
							},
						},
					},
				},
				parentStr: "",
				want: &meta.App{
					Meta: meta.Metadata{
						Name: "dapp1",
						UUID: "123456",
					},
					Spec: meta.AppSpec{
						Channels: map[string]*meta.Channel{
							"channel1": {
								Meta: meta.Metadata{Name: "channel1"},
							},
						},
						Types: map[string]*meta.Type{
							"channeltype1": {
								Meta: meta.Metadata{Name: "channel1"},
							},
						},

						Aliases: map[string]*meta.Alias{
							"alias1": {
								Meta: meta.Metadata{Name: "channel1"},
							},
						},
					},
				},
				tree: &meta.App{
					Spec: meta.AppSpec{
						Apps: map[string]*meta.App{
							"dapp1": {
								Meta: meta.Metadata{
									Name: "dapp1",
									UUID: "123456",
								},
								Spec: meta.AppSpec{
									Channels: map[string]*meta.Channel{
										"channel1": {
											Meta: meta.Metadata{Name: "channel1"},
										},
									},
									Types: map[string]*meta.Type{
										"channeltype1": {
											Meta: meta.Metadata{Name: "channel1"},
										},
									},

									Aliases: map[string]*meta.Alias{
										"alias1": {
											Meta: meta.Metadata{Name: "channel1"},
										},
									},
								},
							},
						},
					},
				},
			},
			update: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			amm := &AppMemoryManager{
				treeMemoryManager: &treeMemoryManager{
					root: tt.args.tree,
					tree: tt.args.tree,
				},
			}
			amm.updateUUID(tt.args.app, tt.args.parentStr)
			if !tt.update {
				metautils.RecursiveValidateUUIDS("", tt.args.app, t)
			} else if !reflect.DeepEqual(tt.args.app, tt.args.want) {
				t.Error("chaged uuid")
			}
		})
	}
}

func Test_validAliases(t *testing.T) {
	appTest := meta.App{
		Meta: meta.Metadata{
			Name: "app",
		},
		Spec: meta.AppSpec{
			Aliases: map[string]*meta.Alias{
				"valid.alias1": {
					Resource: "ch1",
				},
				"valid.alias2": {
					Resource: "ch2",
				},
				"invalid.alias1": {
					Resource: "ch3",
				},
				"invalid.alias2": {
					Resource: "ch4",
				},
			},
			Channels: map[string]*meta.Channel{
				"ch1": {
					Meta: meta.Metadata{
						Name:   "ch1",
						Parent: "",
					},
				},
			},
			Boundary: meta.AppBoundary{
				Channels: meta.Boundary{
					Output: []string{"ch2"},
				},
			},
		},
	}
	type fields struct {
		root *meta.App
	}
	type args struct {
		app *meta.App
	}
	tests := []struct {
		fields  fields
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test alias validation",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				app: &appTest,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			amm := &AppMemoryManager{
				treeMemoryManager: &treeMemoryManager{
					root: tt.fields.root,
					tree: tt.fields.root,
				},
			}
			err := amm.validAliases(tt.args.app)
			if tt.wantErr && (err == nil) {
				t.Errorf("validAliases(): wanted error but received 'nil'")
				return
			}

			if !tt.wantErr && (err != nil) {
				t.Errorf("validAliases() error: %v", err)
			}
		})
	}
}

var kafkaStructMock = sidecars.KafkaConfig{
	BootstrapServers: "",
	AutoOffsetReset:  "",
	KafkaInsprAddr:   "",
	SidecarImage:     "",
}

func TestSelectBrokerFromPriorityList(t *testing.T) {
	type args struct {
		brokerList []string
		brokers    *apimodels.BrokersDI
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Should return the first available broker",
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"some_broker"},
					Default:   "some_broker",
				},
				brokerList: []string{"some_broker"},
			},
			want: "some_broker",
		},
		{
			name: "Should return the default broker",
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"some_broker"},
					Default:   "some_broker",
				},
				brokerList: []string{"fakeBroker"},
			},
			want:    "some_broker",
			wantErr: false,
		},
		{
			name: "Should return the default broker when priority list is empty",
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"some_broker"},
					Default:   "some_broker",
				},
				brokerList: []string{},
			},
			want: "some_broker",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SelectBrokerFromPriorityList(tt.args.brokerList, tt.args.brokers)

			if !tt.wantErr && (err != nil) {
				t.Errorf("SelectBrokerFromPriorityList() error %v", err)
				return
			}

			if !tt.wantErr && (got != tt.want) {
				t.Errorf("SelectBrokerFromPriorityList() got %v, want %v", got, tt.want)
			}

			if tt.wantErr && (err == nil) {
				t.Errorf("SelectBrokerFromPriorityList() wanted error but got 'nil'")
				return
			}
		})
	}
}

func Test_attachRoutes(t *testing.T) {
	type args struct {
		app *meta.App
	}
	tests := []struct {
		name string
		args args
		want *meta.App
	}{
		{
			args: args{
				app: &meta.App{
					Spec: meta.AppSpec{
						Apps: map[string]*meta.App{
							"a1": {
								Spec: meta.AppSpec{
									Node: meta.Node{
										Meta: meta.Metadata{
											UUID: "node",
										},
									},
								},
							},
							"a2": {
								Spec: meta.AppSpec{
									Node: meta.Node{
										Meta: meta.Metadata{
											UUID: "node",
										},
										Spec: meta.NodeSpec{
											Endpoints: utils.StringArray{"eda", "edb"},
										},
									},
								},
							},
						},
					},
				},
			},
			name: "valid route resolution",
			want: &meta.App{
				Spec: meta.AppSpec{
					Apps: map[string]*meta.App{
						"a1": {
							Spec: meta.AppSpec{
								Node: meta.Node{
									Meta: meta.Metadata{
										Name: "a1",
										UUID: "node",
									},
								},
							},
						},
						"a2": {
							Spec: meta.AppSpec{
								Node: meta.Node{
									Meta: meta.Metadata{
										Name: "a2",
										UUID: "node",
									},
									Spec: meta.NodeSpec{
										Endpoints: utils.StringArray{"eda", "edb"},
									},
								},
							},
						},
					},
					Routes: map[string]*meta.RouteConnection{
						"a2": {
							Address:   "http://node-node:0",
							Endpoints: utils.StringArray{"eda", "edb"},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attachRoutes(tt.args.app)
			if !testRoutes(tt.args.app, tt.want) {
				t.Errorf("Test_attachRoutes() got = \n%v, want = \n%v", tt.args.app, tt.want)
			}
		})
	}
}

func testRoutes(got, want *meta.App) bool {
	for name, wantChild := range want.Spec.Apps {
		gotChild, ok := got.Spec.Apps[name]
		if !ok {
			return false
		}
		if !testRoutes(gotChild, wantChild) {
			return false
		}
	}

	for route, wantData := range want.Spec.Routes {
		gotData, routeMatch := got.Spec.Routes[route]
		addMatch := wantData.Address == gotData.Address
		edpMatch := wantData.Endpoints.Equal(gotData.Endpoints)

		if !routeMatch || !addMatch || !edpMatch {
			return false
		}
	}
	return true
}
