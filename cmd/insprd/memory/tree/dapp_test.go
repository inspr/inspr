package tree

import (
	"fmt"
	"reflect"
	"testing"

	"inspr.dev/inspr/cmd/insprd/memory/brokers"
	"inspr.dev/inspr/cmd/sidecars"
	apimodels "inspr.dev/inspr/pkg/api/models"
	"inspr.dev/inspr/pkg/meta"
	metautils "inspr.dev/inspr/pkg/meta/utils"
	"inspr.dev/inspr/pkg/meta/utils/diff"
	"inspr.dev/inspr/pkg/utils"
)

func getMockApp() *meta.App {
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
				"appNode": {
					Meta: meta.Metadata{
						Name:        "appNode",
						Reference:   "appNode",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name:        "appNode",
								Reference:   "appNode.appNode",
								Annotations: map[string]string{},
								Parent:      "appNode",
								UUID:        "",
							},
							Spec: meta.NodeSpec{
								Image: "imageNodeAppNode",
							},
						},
						Apps: map[string]*meta.App{},
						Channels: map[string]*meta.Channel{
							"ch1app1": {
								Meta: meta.Metadata{
									Name:   "ch1app1",
									Parent: "",
								},
								ConnectedApps: []string{"thenewapp"},
								Spec:          meta.ChannelSpec{},
							},
							"ch2app1": {
								Meta: meta.Metadata{
									Name:   "ch2app1",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
						},
						Types: map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1"},
								Output: []string{"ch2"},
							},
						},
					},
				},
				"app1": {
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
								Spec:          meta.ChannelSpec{},
							},
							"ch2app1": {
								Meta: meta.Metadata{
									Name:   "ch2app1",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
						},
						Types: map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1"},
								Output: []string{"ch2"},
							},
						},
					},
				},
				"app2": {
					Meta: meta.Metadata{
						Name:        "app2",
						Reference:   "app2",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{},
						Apps: map[string]*meta.App{
							"app3": {
								Meta: meta.Metadata{
									Name:        "app3",
									Reference:   "app2.app3",
									Annotations: map[string]string{},
									Parent:      "app2",
									UUID:        "",
								},
								Spec: meta.AppSpec{
									Node: meta.Node{
										Meta: meta.Metadata{
											Name:        "app3",
											Reference:   "app3.nodeApp2",
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
							"app4": {
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
											Name:        "app4",
											Reference:   "app4.nodeApp4",
											Annotations: map[string]string{},
											Parent:      "app2.app4",
											UUID:        "",
										},
										Spec: meta.NodeSpec{
											Image: "imageNodeApp4",
										},
									},
									Apps: map[string]*meta.App{},
									Channels: map[string]*meta.Channel{
										"ch1app4": {
											Meta: meta.Metadata{
												Name:   "ch1app4",
												Parent: "app4",
											},
											Spec: meta.ChannelSpec{
												Type: "ctapp4",
											},
										},
										"ch2app4": {
											Meta: meta.Metadata{
												Name:   "ch2app4",
												Parent: "",
											},
											Spec: meta.ChannelSpec{},
										},
									},
									Types: map[string]*meta.Type{
										"ctapp4": {
											Meta: meta.Metadata{
												Name:        "ctUpdate1",
												Reference:   "app1.ctUpdate1",
												Annotations: map[string]string{},
												Parent:      "app1",
												UUID:        "",
											},
										},
									},
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input:  []string{"ch1app2"},
											Output: []string{"ch2app2"},
										},
									},
								},
							},
						},
						Channels: map[string]*meta.Channel{
							"ch1app2": {
								Meta: meta.Metadata{
									Name:   "ch1app2",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
							"ch2app2": {
								Meta: meta.Metadata{
									Name:   "ch2app2",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
						},
						Types: map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1"},
								Output: []string{"ch2"},
							},
						},
					},
				},
				"bound": {
					Meta: meta.Metadata{
						Name:        "bound",
						Reference:   "bound",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name:        "bound",
								Reference:   "bound.bound",
								Annotations: map[string]string{},
								Parent:      "",
								UUID:        "",
							},
							Spec: meta.NodeSpec{
								Image: "imageNodeAppNode",
							},
						},
						Apps: map[string]*meta.App{
							"bound2": {
								Meta: meta.Metadata{
									Name:        "bound2",
									Reference:   "bound.bound2",
									Annotations: map[string]string{},
									Parent:      "bound",
									UUID:        "",
								},
								Spec: meta.AppSpec{
									Node: meta.Node{
										Meta: meta.Metadata{
											Name:        "bound2",
											Reference:   "bound.bound2",
											Annotations: map[string]string{},
											Parent:      "bound",
											UUID:        "",
										},
										Spec: meta.NodeSpec{
											Image: "imageNodeAppNode",
										},
									},
									Apps: map[string]*meta.App{
										"bound3": {
											Meta: meta.Metadata{
												Name:        "bound3",
												Reference:   "bound.bound2.bound3",
												Annotations: map[string]string{},
												Parent:      "bound.bound2",
												UUID:        "",
											},
											Spec: meta.AppSpec{
												Node: meta.Node{
													Meta: meta.Metadata{
														Name:        "bound3",
														Reference:   "bound.bound2.bound3",
														Annotations: map[string]string{},
														Parent:      "bound.bound2",
														UUID:        "",
													},
													Spec: meta.NodeSpec{
														Image: "imageNodeAppNode",
													},
												},
												Apps:     map[string]*meta.App{},
												Channels: map[string]*meta.Channel{},
												Types:    map[string]*meta.Type{},
												Boundary: meta.AppBoundary{
													Channels: meta.Boundary{
														Input:  []string{"alias1"},
														Output: []string{"alias2"},
													},
												},
											},
										},
									},
									Channels: map[string]*meta.Channel{},
									Types:    map[string]*meta.Type{},
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input:  []string{"alias1"},
											Output: []string{"alias2"},
										},
									},
								},
							},
							"boundNP": {
								Meta: meta.Metadata{
									Name:        "boundNP",
									Reference:   "invalid.path",
									Annotations: map[string]string{},
									Parent:      "invalid.path",
									UUID:        "",
								},
								Spec: meta.AppSpec{
									Node: meta.Node{
										Meta: meta.Metadata{
											Name:        "boundNP",
											Reference:   "invalid.path",
											Annotations: map[string]string{},
											Parent:      "bound",
											UUID:        "",
										},
										Spec: meta.NodeSpec{
											Image: "imageNodeAppNode",
										},
									},
									Apps: map[string]*meta.App{
										"boundNP2": {
											Meta: meta.Metadata{
												Name:        "boundNP2",
												Reference:   "bound.boundNP.boundNP2",
												Annotations: map[string]string{},
												Parent:      "bound.boundNP",
												UUID:        "",
											},
											Spec: meta.AppSpec{
												Node: meta.Node{
													Meta: meta.Metadata{
														Name:        "boundNP2",
														Reference:   "bound.boundNP.boundNP2",
														Annotations: map[string]string{},
														Parent:      "bound.boundNP",
														UUID:        "",
													},
													Spec: meta.NodeSpec{
														Image: "imageNodeAppNode",
													},
												},
												Apps:     map[string]*meta.App{},
												Channels: map[string]*meta.Channel{},
												Types:    map[string]*meta.Type{},
												Boundary: meta.AppBoundary{
													Channels: meta.Boundary{
														Input:  []string{"alias1"},
														Output: []string{"alias2"},
													},
												},
											},
										},
									},
									Channels: map[string]*meta.Channel{},
									Types:    map[string]*meta.Type{},
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input:  []string{"alias1"},
											Output: []string{"alias2"},
										},
									},
								},
							},
							"bound4": {
								Meta: meta.Metadata{
									Name:        "bound4",
									Reference:   "bound.bound4",
									Annotations: map[string]string{},
									Parent:      "bound",
									UUID:        "",
								},
								Spec: meta.AppSpec{
									Node: meta.Node{
										Meta: meta.Metadata{
											Name:        "bound4",
											Reference:   "bound.bound4",
											Annotations: map[string]string{},
											Parent:      "bound",
											UUID:        "",
										},
										Spec: meta.NodeSpec{
											Image: "imageNodeAppNode",
										},
									},
									Apps:     map[string]*meta.App{},
									Channels: map[string]*meta.Channel{},
									Types:    map[string]*meta.Type{},
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input:  []string{"ch1"},
											Output: []string{"alias3"},
										},
									},
								},
							},
							"bound5": {
								Meta: meta.Metadata{
									Name:        "bound5",
									Reference:   "bound.bound5",
									Annotations: map[string]string{},
									Parent:      "bound",
									UUID:        "",
								},
								Spec: meta.AppSpec{
									Node: meta.Node{
										Meta: meta.Metadata{
											Name:        "bound5",
											Reference:   "bound.bound5",
											Annotations: map[string]string{},
											Parent:      "bound",
											UUID:        "",
										},
										Spec: meta.NodeSpec{
											Image: "imageNodeAppNode",
										},
									},
									Apps:     map[string]*meta.App{},
									Channels: map[string]*meta.Channel{},
									Types:    map[string]*meta.Type{},
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input:  []string{"ch1"},
											Output: []string{"alias4"},
										},
									},
								},
							},
							"bound6": {
								Meta: meta.Metadata{
									Name:        "bound6",
									Reference:   "bound.bound6",
									Annotations: map[string]string{},
									Parent:      "bound",
									UUID:        "",
								},
								Spec: meta.AppSpec{
									Node: meta.Node{
										Meta: meta.Metadata{
											Name:        "bound6",
											Reference:   "bound.bound6",
											Annotations: map[string]string{},
											Parent:      "bound",
											UUID:        "",
										},
										Spec: meta.NodeSpec{
											Image: "imageNodeAppNode",
										},
									},
									Apps: map[string]*meta.App{
										"bound7": {
											Meta: meta.Metadata{
												Name:        "bound7",
												Reference:   "bound.bound6",
												Annotations: map[string]string{},
												Parent:      "bound.bound6",
												UUID:        "",
											},
											Spec: meta.AppSpec{
												Node: meta.Node{
													Meta: meta.Metadata{
														Name:        "bound6",
														Reference:   "bound.bound6",
														Annotations: map[string]string{},
														Parent:      "bound",
														UUID:        "",
													},
													Spec: meta.NodeSpec{
														Image: "imageNodeAppNode",
													},
												},
												Apps:     map[string]*meta.App{},
												Channels: map[string]*meta.Channel{},
												Types:    map[string]*meta.Type{},
												Boundary: meta.AppBoundary{
													Channels: meta.Boundary{
														Input:  []string{"bdch1"},
														Output: []string{"alias3"},
													},
												},
											},
										},
									},
									Channels: map[string]*meta.Channel{},
									Types:    map[string]*meta.Type{},
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input:  []string{"bdch1"},
											Output: []string{"alias3"},
										},
									},
									Aliases: map[string]*meta.Alias{
										"bound8.alias": {
											Resource: "notch",
										},
									},
								},
							},
						},
						Channels: map[string]*meta.Channel{
							"bdch1": {
								Meta: meta.Metadata{
									Name:   "bdch1",
									Parent: "",
									UUID:   "uuid-bdch1",
								},
								ConnectedApps: []string{},
								Spec:          meta.ChannelSpec{},
							},
							"bdch2": {
								Meta: meta.Metadata{
									Name:   "bdch2",
									Parent: "",
									UUID:   "uuid-bdch2",
								},
								Spec: meta.ChannelSpec{},
							},
						},
						Types: map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1"},
								Output: []string{"ch2"},
							},
						},
						Aliases: map[string]*meta.Alias{
							"bound2.alias1": {
								Resource: "bdch1",
							},
							"bound2.alias2": {
								Resource: "bdch2",
							},
							"bound4.alias3": {
								Resource: "bdch2",
							},
							"bound6.alias3": {
								Resource: "bdch2",
							},
						},
					},
				},
				"connectedApp": {
					Meta: meta.Metadata{
						Name: "connectedApp",
					},
					Spec: meta.AppSpec{
						Apps: map[string]*meta.App{
							"noAliasSon": {
								Meta: meta.Metadata{
									Name:   "noAliasSon",
									Parent: "connectedApp",
								},
								Spec: meta.AppSpec{
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input: utils.StringArray{
												"channel1",
											},
											Output: utils.StringArray{
												"channel2",
											},
										},
									},
								},
							},
							"aliasSon": {
								Meta: meta.Metadata{
									Name:   "aliasSon",
									Parent: "connectedApp",
								},
								Spec: meta.AppSpec{
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input: utils.StringArray{
												"alias1",
											},
											Output: utils.StringArray{
												"alias2S",
											},
										},
									},
								},
							},
						},
						Channels: map[string]*meta.Channel{
							"channel1": {
								Meta: meta.Metadata{
									Name: "channel1",
									UUID: "uuid-channel1",
								},
								ConnectedApps: utils.StringArray{
									"noAliasSon",
								},
							},
							"channel2": {
								Meta: meta.Metadata{
									Name: "channel2",
									UUID: "uuid-channel2",
								},
								ConnectedApps: utils.StringArray{
									"noAliasSon",
								},
							},
						},
						Aliases: map[string]*meta.Alias{
							"aliasSon.alias1": {
								Resource: "channel1",
							},
							"aliasSon.alias2": {
								Resource: "channel2",
							},
						},
					},
				},
				"appForParentInjection": {
					Meta: meta.Metadata{
						Name: "appForParentInjection",
					},
					Spec: meta.AppSpec{
						Apps: make(map[string]*meta.App),
					},
				},
			},
			Channels: map[string]*meta.Channel{
				"ch1": {
					Meta: meta.Metadata{
						Name:   "ch1",
						Parent: "",
						UUID:   "uuid-ch1",
					},
					Spec: meta.ChannelSpec{
						Type: "ct1",
					},
				},
				"ch2": {
					Meta: meta.Metadata{
						Name:   "ch2",
						Parent: "",
						UUID:   "uuid-ch2",
					},
					Spec: meta.ChannelSpec{
						Type: "ct2",
					},
				},
			},
			Types: map[string]*meta.Type{
				"ct1": {
					Meta: meta.Metadata{
						Name:        "ct1",
						Reference:   "root.ct1",
						Annotations: map[string]string{},
						Parent:      "root",
						UUID:        "uuid-ct1",
					},
					Schema: "",
				},
				"ct2": {
					Meta: meta.Metadata{
						Name:        "ct2",
						Reference:   "root.ct2",
						Annotations: map[string]string{},
						Parent:      "root",
						UUID:        "uuid-ct2",
					},
					Schema: "",
				},
			},
			Boundary: meta.AppBoundary{
				Channels: meta.Boundary{
					Input:  []string{},
					Output: []string{},
				},
			},
		},
	}
	return &root
}

func TestMemoryManager_Apps(t *testing.T) {
	type fields struct {
		root *meta.App
	}
	tests := []struct {
		name   string
		fields fields
		want   AppMemory
	}{
		{
			name: "creating a AppMemoryManager",
			fields: fields{
				root: getMockApp(),
			},
			want: &AppMemoryManager{
				&treeMemoryManager{
					root: getMockApp(),
				},
				logger,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmm := &treeMemoryManager{
				root: tt.fields.root,
			}
			if got := tmm.Apps(); !metautils.CompareWithUUID(got.(*AppMemoryManager).root, tt.want.(*AppMemoryManager).root) {
				t.Errorf("MemoryManager.Apps() = %v", got)
			}
		})
	}
}

func TestAppMemoryManager_GetApp(t *testing.T) {
	type fields struct {
		root *meta.App
	}
	type args struct {
		query string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *meta.App
		wantErr bool
	}{
		{
			name: "Getting root app",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				query: "",
			},
			wantErr: false,
			want:    getMockApp(),
		},
		{
			name: "Getting a root's child app",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				query: "app1",
			},
			wantErr: false,
			want:    getMockApp().Spec.Apps["app1"],
		},
		{
			name: "Getting app inside non-root app",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				query: "app2.app3",
			},
			wantErr: false,
			want:    getMockApp().Spec.Apps["app2"].Spec.Apps["app3"],
		},
		{
			name: "Using invalid query",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				query: "app2.app9",
			},
			wantErr: true,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := &treeMemoryManager{
				root: tt.fields.root,
				tree: tt.fields.root,
			}
			amm := mem.Apps()
			got, err := amm.Get(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("AppMemoryManager.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !metautils.CompareWithoutUUID(got, tt.want) {
				t.Errorf("AppMemoryManager.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAppMemoryManager_Create(t *testing.T) {
	type fields struct {
		root *meta.App
	}
	type args struct {
		app         *meta.App
		context     string
		searchQuery string
		brokers     *apimodels.BrokersDI
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		wantErr       bool
		want          *meta.App
		checkFunction func(t *testing.T, tmm *treeMemoryManager)
	}{
		{
			name: "Creating app inside of root",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "",
				searchQuery: "appCr1",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "appCr1",
						Reference:   "appCr1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node:     meta.Node{},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{},
								Output: []string{},
							},
						},
					},
				},
			},
			wantErr: false,
			want: &meta.App{
				Meta: meta.Metadata{
					Name:        "appCr1",
					Reference:   "appCr1",
					Annotations: map[string]string{},
					Parent:      "",
					UUID:        "",
				},
				Spec: meta.AppSpec{
					Node:     meta.Node{},
					Apps:     map[string]*meta.App{},
					Channels: map[string]*meta.Channel{},
					Types:    map[string]*meta.Type{},
					Boundary: meta.AppBoundary{
						Channels: meta.Boundary{
							Input:  []string{},
							Output: []string{},
						},
					},
				},
			},
		},
		{
			name: "Creating app inside of non-root app",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "app2",
				searchQuery: "app2.appCr2-1",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "appCr2-1",
						Reference:   "",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node:     meta.Node{},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{},
								Output: []string{},
							},
						},
					},
				},
			},
			wantErr: false,
			want: &meta.App{
				Meta: meta.Metadata{
					Name:        "appCr2-1",
					Reference:   "",
					Annotations: map[string]string{},
					Parent:      "app2",
					UUID:        "",
				},
				Spec: meta.AppSpec{
					Node:     meta.Node{},
					Apps:     map[string]*meta.App{},
					Channels: map[string]*meta.Channel{},
					Types:    map[string]*meta.Type{},
					Boundary: meta.AppBoundary{
						Channels: meta.Boundary{
							Input:  []string{},
							Output: []string{},
						},
					},
				},
			},
		},
		{
			name: "Creating app with invalid context",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "invalidCtx",
				searchQuery: "invalidCtx.invalidApp",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "invalidApp",
						Reference:   "",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node:     meta.Node{},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
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
			want:    nil,
		},
		{
			name: "Invalid - Creating app inside of app with Node",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "appNode",
				searchQuery: "appNode.appInvalidWithNode",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "appInvalidWithNode",
						Reference:   "",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node:     meta.Node{},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
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
			want:    nil,
		},
		{
			name: "Creating app with conflicting name",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "",
				searchQuery: "app2",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app2",
						Reference:   "",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node:     meta.Node{},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
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
			want:    nil,
		},
		{
			name: "Creating app with existing name but not in the same context",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "app2",
				searchQuery: "app2.app2",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app2",
						Reference:   "",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node:     meta.Node{},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{},
								Output: []string{},
							},
						},
					},
				},
			},
			wantErr: false,
			want: &meta.App{
				Meta: meta.Metadata{
					Name:        "app2",
					Reference:   "",
					Annotations: map[string]string{},
					Parent:      "app2",
					UUID:        "",
				},
				Spec: meta.AppSpec{
					Node:     meta.Node{},
					Apps:     map[string]*meta.App{},
					Channels: map[string]*meta.Channel{},
					Types:    map[string]*meta.Type{},
					Boundary: meta.AppBoundary{
						Channels: meta.Boundary{
							Input:  []string{},
							Output: []string{},
						},
					},
				},
			},
		},
		{
			name: "Creating app with valid boundary",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "app2",
				searchQuery: "app2.app2",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app2",
						Reference:   "app2.app2",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node:     meta.Node{},
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
			},
			wantErr: false,
			want: &meta.App{
				Meta: meta.Metadata{
					Name:        "app2",
					Reference:   "app2.app2",
					Annotations: map[string]string{},
					Parent:      "app2",
					UUID:        "",
				},
				Spec: meta.AppSpec{
					Node:     meta.Node{},
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
		},
		{
			name: "Creating app with invalid boundary",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "app2",
				searchQuery: "app2.app2",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app2",
						Reference:   "",
						Annotations: map[string]string{},
						Parent:      "app2",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node:     meta.Node{},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1app2invalid"},
								Output: []string{"ch2app2"},
							},
						},
					},
				},
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "Creating app with node and other apps in it",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "app2",
				searchQuery: "app2.app2",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app2",
						Reference:   "",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name:        "nodeApp2-2",
								Reference:   "",
								Annotations: map[string]string{},
								Parent:      "app2",
								UUID:        "",
							},
							Spec: meta.NodeSpec{
								Image: "imageNodeAppTest",
							},
						},
						Apps: map[string]*meta.App{
							"appTest1": {},
						},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
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
			want:    nil,
		},
		{
			name: "Creating app with Node",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "app2",
				searchQuery: "app2.app2",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app2",
						Reference:   "",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name:        "app2",
								Reference:   "",
								Annotations: nil,
								Parent:      "",
								UUID:        "",
							},
							Spec: meta.NodeSpec{
								Image: "imageNodeAppTest",
							},
						},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{},
								Output: []string{},
							},
						},
					},
				},
			},
			wantErr: false,
			want: &meta.App{
				Meta: meta.Metadata{
					Name:        "app2",
					Reference:   "",
					Annotations: map[string]string{},
					Parent:      "app2",
					UUID:        "",
				},
				Spec: meta.AppSpec{
					Node: meta.Node{
						Meta: meta.Metadata{
							Name:        "app2",
							Reference:   "",
							Annotations: map[string]string{},
							Parent:      "app2",
							UUID:        "",
						},
						Spec: meta.NodeSpec{
							Image: "imageNodeAppTest",
						},
					},
					Apps:     map[string]*meta.App{},
					Channels: map[string]*meta.Channel{},
					Types:    map[string]*meta.Type{},
					Boundary: meta.AppBoundary{
						Channels: meta.Boundary{
							Input:  []string{},
							Output: []string{},
						},
					},
				},
			},
		},
		{
			name: "It should update the channel's connectedApps list",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context: "app2",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app7",
						Reference:   "app2.app7",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Apps: map[string]*meta.App{
							"app8": {
								Meta: meta.Metadata{
									Name:        "app8",
									Reference:   "app2.app7.app8",
									Annotations: map[string]string{},
									Parent:      "app2.app7",
									UUID:        "",
								},
								Spec: meta.AppSpec{
									Node:     meta.Node{},
									Apps:     map[string]*meta.App{},
									Channels: map[string]*meta.Channel{},
									Types:    map[string]*meta.Type{},
									Boundary: meta.AppBoundary{
										Channels: meta.Boundary{
											Input:  []string{"channel1"},
											Output: []string{},
										},
									},
								},
							},
						},
						Channels: map[string]*meta.Channel{
							"channel1": {
								Meta: meta.Metadata{
									Name: "channel1",
								},
								ConnectedApps: []string{},
								Spec: meta.ChannelSpec{
									Type: "ct1",
								},
							},
						},
						Types: map[string]*meta.Type{
							"ct1": {
								Meta: meta.Metadata{
									Name: "ct1",
								},
								ConnectedChannels: []string{},
							},
						},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1app2"},
								Output: []string{"ch2app2"},
							},
						},
					},
				},
			},
			wantErr: false,
			checkFunction: func(t *testing.T, tmm *treeMemoryManager) {
				am := tmm.Channels()
				ch, err := am.Get("app2.app7", "channel1")
				if err != nil {
					t.Errorf("cant get channel channel1")
				}
				if !utils.Includes(ch.ConnectedApps, "app2.app7.app8") {
					fmt.Println(ch.ConnectedApps)
					t.Errorf("connectedApps of channel1 dont have app8")
				}
			},
		},
		{
			name: "Create App with a channel that has a invalid Type",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context: "app2",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app2",
						Reference:   "",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{},
						Apps: map[string]*meta.App{},
						Channels: map[string]*meta.Channel{
							"channel1": {
								Meta: meta.Metadata{
									Name: "channel1",
								},
								ConnectedApps: []string{},
								Spec: meta.ChannelSpec{
									Type: "invalidTypeName",
								},
							},
						},
						Types: map[string]*meta.Type{
							"ct1": {
								Meta: meta.Metadata{
									Name: "ct1",
								},
								ConnectedChannels: []string{},
							},
						},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1app2"},
								Output: []string{"ch2app2"},
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "It should update the Type's connectedChannels list",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context: "app2",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app2",
						Reference:   "app2.app2",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{},
						Apps: map[string]*meta.App{},
						Channels: map[string]*meta.Channel{
							"channel1": {
								Meta: meta.Metadata{
									Name: "channel1",
								},
								ConnectedApps: []string{},
								Spec: meta.ChannelSpec{
									Type: "ct1",
								},
							},
						},
						Types: map[string]*meta.Type{
							"ct1": {
								Meta: meta.Metadata{
									Name: "ct1",
								},
								ConnectedChannels: []string{},
							},
						},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1app2"},
								Output: []string{"ch2app2"},
							},
						},
					},
				},
			},
			wantErr: false,
			checkFunction: func(t *testing.T, tmm *treeMemoryManager) {
				am := tmm.Types()
				ct, err := am.Get("app2.app2", "ct1")
				if err != nil {
					t.Errorf("cant get Type ct1")
				}
				if !utils.Includes(ct.ConnectedChannels, "channel1") {
					t.Errorf("connectedChannels of ct1 dont have channel1")
				}
			},
		},
		{
			name: "Invalid name - doesn't create app",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "",
				searchQuery: "appCr1",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app%Cr1",
						Reference:   "appCr1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node:     meta.Node{},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
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
			want:    nil,
		},
		{
			name: "Creating app with Node without name",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "app2",
				searchQuery: "app2.app2",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app2",
						Reference:   "",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name:        "",
								Reference:   "",
								Annotations: nil,
								Parent:      "",
								UUID:        "",
							},
							Spec: meta.NodeSpec{
								Image: "imageNodeAppTest",
							},
						},
						Apps:     map[string]*meta.App{},
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{},
								Output: []string{},
							},
						},
					},
				},
			},
			wantErr: false,
			want: &meta.App{
				Meta: meta.Metadata{
					Name:        "app2",
					Reference:   "",
					Annotations: map[string]string{},
					Parent:      "app2",
					UUID:        "",
				},
				Spec: meta.AppSpec{
					Node: meta.Node{
						Meta: meta.Metadata{
							Name:        "app2",
							Reference:   "",
							Annotations: map[string]string{},
							Parent:      "app2",
							UUID:        "",
						},
						Spec: meta.NodeSpec{
							Image: "imageNodeAppTest",
						},
					},
					Apps:     map[string]*meta.App{},
					Channels: map[string]*meta.Channel{},
					Types:    map[string]*meta.Type{},
					Boundary: meta.AppBoundary{
						Channels: meta.Boundary{
							Input:  []string{},
							Output: []string{},
						},
					},
				},
			},
		},
		{
			name: "Invalid - App with boundary and channel with same name",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				context:     "app2",
				searchQuery: "app2.app2",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app2",
						Reference:   "",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name:        "",
								Reference:   "",
								Annotations: nil,
								Parent:      "",
								UUID:        "",
							},
							Spec: meta.NodeSpec{
								Image: "imageNodeAppTest",
							},
						},
						Apps: map[string]*meta.App{},
						Channels: map[string]*meta.Channel{
							"output1": {
								Meta: meta.Metadata{
									Name:   "output1",
									Parent: "",
								},
								ConnectedApps: []string{},
								Spec:          meta.ChannelSpec{},
							},
						},
						Types: map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"input1"},
								Output: []string{"output1"},
							},
						},
					},
				},
			},
			wantErr: true,
			want:    nil,
		},
		// {
		// 	name: "Invalid alias",
		// 	fields: fields{
		// 		root: getMockApp(),
		// 	},
		// 	args: args{
		// 		brokers: &apimodels.BrokersDI{
		// 			Available: []string{"kafka"},
		// 			Default:   "kafka",
		// 		},
		// 		context:     "app2",
		// 		searchQuery: "app2.app2",
		// 		app: &meta.App{
		// 			Meta: meta.Metadata{
		// 				Name:        "app2",
		// 				Reference:   "",
		// 				Annotations: map[string]string{},
		// 				Parent:      "app2",
		// 				UUID:        "",
		// 			},
		// 			Spec: meta.AppSpec{
		// 				Aliases: map[string]*meta.Alias{
		// 					"app6.output1": {
		// 						Resource: "fakeChannel",
		// 					},
		// 				},
		// 				Apps: map[string]*meta.App{
		// 					"app6": {
		// 						Meta: meta.Metadata{
		// 							Name:        "app6",
		// 							Reference:   "app2.app6",
		// 							Annotations: map[string]string{},
		// 							Parent:      "app2.app2",
		// 							UUID:        "",
		// 						},
		// 						Spec: meta.AppSpec{
		// 							Node: meta.Node{
		// 								Meta: meta.Metadata{
		// 									Name:        "app6",
		// 									Reference:   "app6.nodeApp4",
		// 									Annotations: map[string]string{},
		// 									Parent:      "app4",
		// 									UUID:        "",
		// 								},
		// 								Spec: meta.NodeSpec{
		// 									Image: "imageNodeApp4",
		// 								},
		// 							},
		// 							Boundary: meta.AppBoundary{
		// 								Channels: meta.Boundary{
		// 									Input:  []string{"output1"},
		// 									Output: []string{"output1"},
		// 								},
		// 							},
		// 						},
		// 					},
		// 				},
		// 				Channels: map[string]*meta.Channel{
		// 					"output1": {
		// 						Meta: meta.Metadata{
		// 							Name:   "output1",
		// 							Parent: "",
		// 						},
		// 						ConnectedApps: []string{},
		// 						Spec:          meta.ChannelSpec{},
		// 					},
		// 				},
		// 				Types: map[string]*meta.Type{},
		// 				Boundary: meta.AppBoundary{
		// 					Channels: meta.Boundary{
		// 						Input:  []string{"ch1app2"},
		// 						Output: []string{"ch1app2"},
		// 					},
		// 				},
		// 			},
		// 		},
		// 	},
		// 	wantErr: true,
		// 	want:    nil,
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := &treeMemoryManager{
				root: tt.fields.root,
				tree: tt.fields.root,
			}
			am := mem.Apps()
			err := am.Create(tt.args.context, tt.args.app, tt.args.brokers)
			if (err != nil) != tt.wantErr {
				t.Errorf("AppMemoryManager.Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				scope, _ := metautils.JoinScopes(tt.args.context, tt.args.app.Meta.Name)
				got, _ := am.Get(scope)
				metautils.RecursiveValidateUUIDS("AppMemoryManager.Create()", got, t)
			}
			if tt.want != nil {
				got, err := am.Get(tt.args.searchQuery)
				if (err != nil) || !metautils.CompareWithoutUUID(got, tt.want) {
					t.Errorf("AppMemoryManager.Get() = %v, want %v", got, tt.want)
				}
			}

			if tt.checkFunction != nil {
				tt.checkFunction(t, mem)
			}
		})
	}
}

func TestAppMemoryManager_Delete(t *testing.T) {
	type fields struct {
		root *meta.App
	}
	type args struct {
		query string
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		wantErr       bool
		want          *meta.App
		checkFunction func(t *testing.T, tmm *treeMemoryManager)
	}{
		{
			name: "Deleting leaf app from root",
			fields: fields{
				root: getMockApp(),
			},
			args: args{

				query: "app1.thenewapp",
			},
			wantErr: false,
			want:    nil,
		},
		{
			name: "Deleting leaf app from another app",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				query: "app2.app3",
			},
			wantErr: false,
			want:    nil,
		},
		{
			name: "Deleting app with child apps and channels",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				query: "app2",
			},
			wantErr: false,
			want:    nil,
		},
		{
			name: "Deleting root - invalid deletion",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				query: "",
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "Deleting with invalid query - invalid deletion",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				query: "invalid.query.to.app",
			},
			wantErr: true,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := &treeMemoryManager{
				root: tt.fields.root,
				tree: tt.fields.root,
			}
			am := mem.Apps()
			err := am.Delete(tt.args.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("AppMemoryManager.Delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				got, err := am.Get(tt.args.query)
				if err == nil {
					t.Errorf("AppMemoryManager.Get() = %v, want %v", got, tt.want)
				}
			}
			if tt.checkFunction != nil {
				tt.checkFunction(t, mem)
			}
		})
	}
}

func TestAppMemoryManager_Update(t *testing.T) {
	kafkaConfig := sidecars.KafkaConfig{}
	bmm := brokers.GetBrokerMemory()
	bmm.Create(&kafkaConfig)

	type fields struct {
		root    *meta.App
		updated bool
	}
	type args struct {
		app     *meta.App
		query   string
		brokers *apimodels.BrokersDI
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		wantErr       bool
		want          *meta.App
		checkFunction func(t *testing.T, tmm *treeMemoryManager)
	}{
		{
			name: "invalid- update changing apps' name",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				query: "app1",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app1Invalid",
						Reference:   "app1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{},
						Apps: map[string]*meta.App{},
						Channels: map[string]*meta.Channel{
							"ch1app1": {
								Meta: meta.Metadata{
									Name:   "ch1app1",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
							"ch2app1": {
								Meta: meta.Metadata{
									Name:   "ch2app1",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
						},
						Types: map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1"},
								Output: []string{"ch2"},
							},
						},
					},
				},
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "invalid- updated app has node and child apps",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				query: "app1",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app1",
						Reference:   "app1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
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
						Apps: map[string]*meta.App{
							"invalidChildApp": {},
						},
						Channels: map[string]*meta.Channel{
							"ch1app1": {
								Meta: meta.Metadata{
									Name:   "ch1app1",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
							"ch2app1": {
								Meta: meta.Metadata{
									Name:   "ch2app1",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
						},
						Types: map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1"},
								Output: []string{"ch2"},
							},
						},
					},
				},
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "invalid- has structural errors",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				query: "app1",
				app: &meta.App{
					Meta: meta.Metadata{
						Name:        "app1Invalid",
						Reference:   "app1",
						Annotations: map[string]string{},
						Parent:      "",
						UUID:        "",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{},
						Apps: map[string]*meta.App{},
						Channels: map[string]*meta.Channel{
							"ch1app1Invalid": {
								Meta: meta.Metadata{
									Name:   "ch1app1",
									Parent: "app1",
								},
								Spec: meta.ChannelSpec{
									Type: "dsntExist",
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
						Types: map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1"},
								Output: []string{"ch2"},
							},
						},
					},
				},
			},
			wantErr: true,
			want:    nil,
		},
		{
			name: "Valid - updated app doesn't have changes",
			fields: fields{
				root: getMockApp(),
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				query: "app1",
				app:   getMockApp().Spec.Apps["app1"],
			},
			wantErr: false,
			want:    getMockApp().Spec.Apps["app1"],
		},
		{
			name: "Valid - updated app has changes",
			fields: fields{
				root: getMockApp(),

				updated: true,
			},
			args: args{
				brokers: &apimodels.BrokersDI{
					Available: []string{"kafka"},
					Default:   "kafka",
				},
				query: "app1",
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
							"appUpdate1": {
								Meta: meta.Metadata{
									Name:        "appUpdate1",
									Reference:   "app1.appUpdate1",
									Annotations: map[string]string{},
									Parent:      "",
									UUID:        "",
								},
								Spec: meta.AppSpec{},
							},
							"appUpdate2": {
								Meta: meta.Metadata{
									Name:        "appUpdate2",
									Reference:   "app1.appUpdate2",
									Annotations: map[string]string{},
									Parent:      "",
									UUID:        "",
								},
								Spec: meta.AppSpec{},
							},
						},
						Channels: map[string]*meta.Channel{
							"ch1app1": {
								Meta: meta.Metadata{
									Name:   "ch1app1",
									Parent: "",
								},
								Spec: meta.ChannelSpec{},
							},
							"ch2app1Update": {
								Meta: meta.Metadata{
									Name:   "ch2app1Update",
									Parent: "app1",
								},
								Spec: meta.ChannelSpec{
									Type: "ctUpdate1",
								},
							},
						},
						Types: map[string]*meta.Type{
							"ctUpdate1": {
								Meta: meta.Metadata{
									Name:        "ctUpdate1",
									Reference:   "app1.ctUpdate1",
									Annotations: map[string]string{},
									Parent:      "app1",
									UUID:        "",
								},
							},
						},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1"},
								Output: []string{"ch2"},
							},
						},
					},
				},
			},
			wantErr: false,
			want: &meta.App{
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
						"appUpdate1": {
							Meta: meta.Metadata{
								Name:        "appUpdate1",
								Reference:   "app1.appUpdate1",
								Annotations: map[string]string{},
								Parent:      "app1",
								UUID:        "",
							},
							Spec: meta.AppSpec{},
						},
						"appUpdate2": {
							Meta: meta.Metadata{
								Name:        "appUpdate2",
								Reference:   "app1.appUpdate2",
								Annotations: map[string]string{},
								Parent:      "app1",
								UUID:        "",
							},
							Spec: meta.AppSpec{},
						},
					},
					Channels: map[string]*meta.Channel{
						"ch1app1": {
							Meta: meta.Metadata{
								Name:   "ch1app1",
								Parent: "",
							},
							Spec: meta.ChannelSpec{},
						},
						"ch2app1Update": {
							Meta: meta.Metadata{
								Name:   "ch2app1Update",
								Parent: "app1",
							},
							Spec: meta.ChannelSpec{
								Type: "ctUpdate1",
							},
						},
					},
					Types: map[string]*meta.Type{
						"ctUpdate1": {
							Meta: meta.Metadata{
								Name:        "ctUpdate1",
								Reference:   "app1.ctUpdate1",
								Annotations: map[string]string{},
								Parent:      "app1",
								UUID:        "",
							},
							ConnectedChannels: []string{"ch2app1Update"},
						},
					},
					Boundary: meta.AppBoundary{
						Channels: meta.Boundary{
							Input:  []string{"ch1"},
							Output: []string{"ch2"},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := &treeMemoryManager{
				root: tt.fields.root,
				tree: tt.fields.root,
			}
			am := mem.Apps()
			err := am.Update(tt.args.query, tt.args.app, tt.args.brokers)
			if (err != nil) != tt.wantErr {
				t.Errorf("AppMemoryManager.Update() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.want != nil {
				got, err := am.Get(tt.args.query)
				_, derr := diff.Diff(got, tt.want)
				if derr != nil {
					fmt.Println(derr.Error())
				}

				uuidComp := metautils.CompareWithoutUUID(got, tt.want)
				if (err != nil) || (!uuidComp && !tt.fields.updated) {
					t.Errorf("AppMemoryManager.Get() = %v, want %v", got, tt.want)
				}
			}
			if tt.checkFunction != nil {
				tt.checkFunction(t, mem)
			}
		})
	}
}

func TestAppMemoryManager_ResolveBoundary(t *testing.T) {
	type fields struct {
		root *meta.App
		tree *meta.App
	}
	type args struct {
		app         *meta.App
		usePermTree bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]string
		want1   map[string]string
		wantErr bool
	}{
		{
			name: "Succesfully resolve boundary",
			fields: fields{
				root: aliasMockedApp(),
				tree: aliasMockedApp(),
			},
			args: args{
				app:         aliasMockedApp().Spec.Apps["A"].Spec.Apps["N"],
				usePermTree: true,
			},
			want: map[string]string{
				"four": "C.D.one",
			},
			want1: map[string]string{
				"ten": "C.D.thirteen",
			},
			wantErr: false,
		},
		{
			name: "cannot find boundaries - should return errors",
			fields: fields{
				root: aliasMockedAppError(),
				tree: aliasMockedAppError(),
			},
			args: args{
				app:         aliasMockedAppError().Spec.Apps["A"].Spec.Apps["N"],
				usePermTree: true,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := &treeMemoryManager{
				root: tt.fields.root,
				tree: tt.fields.tree,
			}
			amm := mem.Apps().(*AppMemoryManager)

			got, got1, err := amm.ResolveBoundary(tt.args.app, tt.args.usePermTree)

			if (err != nil) != tt.wantErr {
				t.Errorf("AppMemoryManager.ResolveBoundaryNew() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AppMemoryManager.ResolveBoundaryNew() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("AppMemoryManager.ResolveBoundaryNew() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func aliasMockedApp() *meta.App {
	return &meta.App{
		Meta: meta.Metadata{
			Name:   "",
			Parent: "",
		},
		Spec: meta.AppSpec{
			Aliases: map[string]*meta.Alias{
				"three": {
					Meta: meta.Metadata{
						Name: "three",
					},
					Resource:    "two",
					Source:      "C",
					Destination: "A",
				},
				"eleven": {
					Meta: meta.Metadata{
						Name: "eleven",
					},
					Resource:    "twelve",
					Source:      "C",
					Destination: "A",
				},
			},
			Channels: map[string]*meta.Channel{},
			Routes:   map[string]*meta.RouteConnection{},
			Apps: map[string]*meta.App{
				"A": {
					Meta: meta.Metadata{
						Name:   "A",
						Parent: "",
					},
					Spec: meta.AppSpec{
						Aliases: map[string]*meta.Alias{
							"four": {
								Meta: meta.Metadata{
									Name: "four",
								},
								Resource:    "three",
								Source:      "",
								Destination: "N",
							},
							"ten": {
								Meta: meta.Metadata{
									Name: "ten",
								},
								Resource:    "eleven",
								Source:      "",
								Destination: "N",
							},
						},
						Channels: map[string]*meta.Channel{},
						Routes:   map[string]*meta.RouteConnection{},
						Apps: map[string]*meta.App{
							"N": {
								Meta: meta.Metadata{
									Name:   "N",
									Parent: "A",
								},
								Spec: meta.AppSpec{
									Boundary: meta.AppBoundary{
										Routes: utils.StringArray{
											"four",
										},
										Channels: meta.Boundary{
											Input: utils.StringArray{
												"ten",
											},
										},
									},
									Channels: map[string]*meta.Channel{},
									Routes:   map[string]*meta.RouteConnection{},
								},
							},
						},
					},
				},

				"C": {
					Meta: meta.Metadata{
						Name:   "C",
						Parent: "",
					},
					Spec: meta.AppSpec{
						Aliases: map[string]*meta.Alias{
							"two": {
								Resource:    "one",
								Source:      "D",
								Destination: "",
							},

							"twelve": {
								Resource:    "thirteen",
								Source:      "D",
								Destination: "",
							},
						},
						Channels: map[string]*meta.Channel{},
						Routes:   map[string]*meta.RouteConnection{},
						Apps: map[string]*meta.App{
							"D": {
								Meta: meta.Metadata{
									Name:   "D",
									Parent: "C",
								},
								Spec: meta.AppSpec{
									Routes: map[string]*meta.RouteConnection{
										"one": {
											Meta: meta.Metadata{
												Name: "one",
											},
										},
									},
									Channels: map[string]*meta.Channel{
										"thirteen": {
											Meta: meta.Metadata{
												Name: "thirteen",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func aliasMockedAppError() *meta.App {
	return &meta.App{
		Meta: meta.Metadata{
			Name:   "",
			Parent: "",
		},
		Spec: meta.AppSpec{
			Aliases: map[string]*meta.Alias{
				"three": {
					Meta: meta.Metadata{
						Name: "three",
					},
					Resource:    "two",
					Source:      "C",
					Destination: "A",
				},
				"twenty-one": {
					Meta: meta.Metadata{
						Name: "twenty-one",
					},
					Resource:    "twenty-two",
					Source:      "C",
					Destination: "A",
				},
			},
			Channels: map[string]*meta.Channel{},
			Routes:   map[string]*meta.RouteConnection{},
			Apps: map[string]*meta.App{
				"A": {
					Meta: meta.Metadata{
						Name:   "A",
						Parent: "",
					},
					Spec: meta.AppSpec{
						Aliases: map[string]*meta.Alias{
							"four": {
								Meta: meta.Metadata{
									Name: "four",
								},
								Resource:    "three",
								Source:      "",
								Destination: "N",
							},
							"ten": {
								Meta: meta.Metadata{
									Name: "ten",
								},
								Resource:    "eleven",
								Source:      "",
								Destination: "N",
							},
							"twenty": {
								Meta: meta.Metadata{
									Name: "twenty",
								},
								Resource:    "twenty-one",
								Source:      "",
								Destination: "N",
							},
						},
						Channels: map[string]*meta.Channel{},
						Routes:   map[string]*meta.RouteConnection{},
						Apps: map[string]*meta.App{
							"N": {
								Meta: meta.Metadata{
									Name:   "N",
									Parent: "A",
								},
								Spec: meta.AppSpec{
									Boundary: meta.AppBoundary{
										Routes: utils.StringArray{
											"four",
											"ten",
											"twenty",
										},
									},
									Channels: map[string]*meta.Channel{},
									Routes:   map[string]*meta.RouteConnection{},
								},
							},
						},
					},
				},

				"C": {
					Meta: meta.Metadata{
						Name:   "C",
						Parent: "",
					},
					Spec: meta.AppSpec{
						Aliases: map[string]*meta.Alias{
							"two": {
								Resource:    "one",
								Source:      "D",
								Destination: "",
							},

							"twenty-two": {
								Resource:    "twenty-three",
								Source:      "D",
								Destination: "B",
							},
						},
						Channels: map[string]*meta.Channel{},
						Routes:   map[string]*meta.RouteConnection{},
						Apps: map[string]*meta.App{
							"D": {
								Meta: meta.Metadata{
									Name:   "D",
									Parent: "C",
								},
								Spec: meta.AppSpec{},
							},
						},
					},
				},
			},
		},
	}
}

func TestAppMemoryManager_isAppUsed(t *testing.T) {
	type args struct {
		app    *meta.App
		parent *meta.App
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "channel declared in app is being used",
			args: args{
				app:    getIsAppUsedChannel().Spec.Apps["B"],
				parent: getIsAppUsedChannel(),
			},
			want: true,
		},
		{
			name: "route declared in app is being used",
			args: args{
				app:    getIsAppUsedRoute().Spec.Apps["B"],
				parent: getIsAppUsedRoute(),
			},
			want: true,
		},
		{
			name: "alias declared in app is being used",
			args: args{
				app:    getIsAppUsedAlias().Spec.Apps["B"],
				parent: getIsAppUsedAlias(),
			},
			want: true,
		},
		{
			name: "app is not being used",
			args: args{
				app:    getIsAppUsedNotUsed().Spec.Apps["B"],
				parent: getIsAppUsedNotUsed(),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			amm := &AppMemoryManager{
				treeMemoryManager: &treeMemoryManager{
					root: getMockApp(),
				},
			}
			if got := amm.isAppUsed(tt.args.app, tt.args.parent); got != tt.want {
				t.Errorf("AppMemoryManager.isAppUsed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func getIsAppUsedChannel() *meta.App {
	return &meta.App{
		Meta: meta.Metadata{
			Name: "",
		},
		Spec: meta.AppSpec{
			Apps: map[string]*meta.App{
				"B": {
					Meta: meta.Metadata{
						Name:   "B",
						Parent: "",
					},
					Spec: meta.AppSpec{
						Channels: map[string]*meta.Channel{
							"channel": {
								Meta: meta.Metadata{
									Name: "channel",
								},
							},
						},
					},
				},
			},
			Aliases: map[string]*meta.Alias{
				"myalias": {
					Meta: meta.Metadata{
						Name: "myalias",
					},
					Resource:    "channel",
					Source:      "B",
					Destination: "",
				},
			},
		},
	}
}

func getIsAppUsedRoute() *meta.App {
	return &meta.App{
		Meta: meta.Metadata{
			Name: "",
		},
		Spec: meta.AppSpec{
			Apps: map[string]*meta.App{
				"B": {
					Meta: meta.Metadata{
						Name:   "B",
						Parent: "",
					},
					Spec: meta.AppSpec{
						Routes: map[string]*meta.RouteConnection{
							"route": {
								Meta: meta.Metadata{
									Name: "route",
								},
							},
						},
					},
				},
			},
			Aliases: map[string]*meta.Alias{
				"myalias": {
					Meta: meta.Metadata{
						Name: "myalias",
					},
					Resource:    "route",
					Source:      "B",
					Destination: "",
				},
			},
		},
	}
}

func getIsAppUsedAlias() *meta.App {
	return &meta.App{
		Meta: meta.Metadata{
			Name: "",
		},
		Spec: meta.AppSpec{
			Apps: map[string]*meta.App{
				"B": {
					Meta: meta.Metadata{
						Name:   "B",
						Parent: "",
					},
					Spec: meta.AppSpec{
						Aliases: map[string]*meta.Alias{
							"myawesomealias": {
								Meta: meta.Metadata{
									Name: "myawesomealias",
								},
								Resource:    "C.Route",
								Source:      "C",
								Destination: "",
							},
						},
					},
				},
			},
			Aliases: map[string]*meta.Alias{
				"myalias": {
					Meta: meta.Metadata{
						Name: "myalias",
					},
					Resource:    "myawesomealias",
					Source:      "B",
					Destination: "",
				},
			},
		},
	}
}

func getIsAppUsedNotUsed() *meta.App {
	return &meta.App{
		Meta: meta.Metadata{
			Name: "",
		},
		Spec: meta.AppSpec{
			Apps: map[string]*meta.App{
				"B": {
					Meta: meta.Metadata{
						Name:   "B",
						Parent: "",
					},
					Spec: meta.AppSpec{},
				},
			},
			Aliases: map[string]*meta.Alias{
				"myalias": {
					Meta: meta.Metadata{
						Name: "myalias",
					},
					Resource:    "myawesomealias",
					Source:      "B",
					Destination: "",
				},
			},
		},
	}
}
