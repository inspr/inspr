package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"inspr.dev/inspr/pkg/api/models"
	"inspr.dev/inspr/pkg/ierrors"
	"inspr.dev/inspr/pkg/meta"
	"inspr.dev/inspr/pkg/meta/utils/diff"
	"inspr.dev/inspr/pkg/rest"
	"inspr.dev/inspr/pkg/rest/request"
)

func TestAppClient_Delete(t *testing.T) {

	type args struct {
		ctx     context.Context
		context string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "delete app test",
			args: args{
				ctx:     context.Background(),
				context: "app1.app2",
			},
			wantErr: false,
		},
		{
			name: "delete app with error test",
			args: args{
				ctx:     context.Background(),
				context: "app1.app2",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(w http.ResponseWriter, r *http.Request) {
				encoder := json.NewEncoder(w)
				if tt.wantErr {
					w.WriteHeader(http.StatusBadRequest)
					encoder.Encode(ierrors.New("").BadRequest())
					return
				}

				if r.URL.Path != "/apps" {
					t.Errorf("path is not apps")
				}

				if r.Method != http.MethodDelete {
					t.Errorf("method is not DELETE")
				}

				var di models.AppQueryDI
				scope := r.Header.Get(rest.HeaderScopeKey)

				decoder := request.JSONDecoderGenerator(r.Body)
				err := decoder.Decode(&di)
				if err != nil {
					t.Error(err)
				}

				if scope != tt.args.context {
					t.Errorf("context set incorrectly. want = %v, got = %v", scope, tt.args.context)
				}

				encoder.Encode(diff.Changelog{})
			}
			s := httptest.NewServer(http.HandlerFunc(handler))
			defer s.Close()
			ac := &AppClient{
				reqClient: request.NewJSONClient(s.URL),
			}
			if _, err := ac.Delete(tt.args.ctx, tt.args.context, false); (err != nil) != tt.wantErr {
				t.Errorf("AppClient.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAppClient_Get(t *testing.T) {

	type args struct {
		ctx     context.Context
		context string
	}
	tests := []struct {
		name    string
		args    args
		want    *meta.App
		wantErr bool
	}{
		{
			name: "get app test",
			args: args{
				ctx:     context.Background(),
				context: "app1.app2",
			},
			wantErr: false,
			want: &meta.App{
				Meta: meta.Metadata{
					Name:      "app2",
					Reference: "app1",
				},
				Spec: meta.AppSpec{
					Node: meta.Node{
						Meta: meta.Metadata{
							Name:   "node1",
							Parent: "app1",
						},
					},
					Boundary: meta.AppBoundary{
						Channels: meta.Boundary{
							Input:  []string{"channel1"},
							Output: []string{"channel2"},
						},
					},
				},
			},
		},
		{
			name: "get app with error test",
			args: args{
				ctx:     context.Background(),
				context: "app1.app2",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(w http.ResponseWriter, r *http.Request) {
				encoder := json.NewEncoder(w)
				if tt.wantErr {
					w.WriteHeader(http.StatusBadRequest)
					encoder.Encode(ierrors.New("").BadRequest())
					return
				}

				if r.URL.Path != "/apps" {
					t.Errorf("path is not apps")
				}

				if r.Method != http.MethodGet {
					t.Errorf("method is not GET")
				}

				var di models.AppQueryDI
				scope := r.Header.Get(rest.HeaderScopeKey)

				decoder := request.JSONDecoderGenerator(r.Body)
				err := decoder.Decode(&di)
				if err != nil {
					t.Error(err)
				}

				if scope != tt.args.context {
					t.Errorf("context set incorrectly. want = %v, got = %v", scope, tt.args.context)
				}

				encoder.Encode(tt.want)
			}

			s := httptest.NewServer(http.HandlerFunc(handler))
			defer s.Close()
			ac := &AppClient{
				reqClient: request.NewJSONClient(s.URL),
			}
			got, err := ac.Get(tt.args.ctx, tt.args.context)
			if (err != nil) != tt.wantErr {
				t.Errorf("AppClient.Get() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AppClient.Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAppClient_Create(t *testing.T) {

	type args struct {
		ctx     context.Context
		context string
		ch      *meta.App
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test create app",
			args: args{
				ctx:     context.Background(),
				context: "app1.app2",
				ch: &meta.App{
					Meta: meta.Metadata{
						Name: "app3",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name: "node",
							},
						},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"channel1"},
								Output: []string{"channel2"},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "create app with error test",
			args: args{
				ctx:     context.Background(),
				context: "app1.app2",
				ch:      &meta.App{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(w http.ResponseWriter, r *http.Request) {
				encoder := json.NewEncoder(w)
				if tt.wantErr {
					w.WriteHeader(http.StatusBadRequest)
					encoder.Encode(ierrors.New("").BadRequest())
					return
				}

				if r.URL.Path != "/apps" {
					t.Errorf("path is not apps")
				}

				if r.Method != http.MethodPost {
					t.Errorf("method is not POST")
				}

				var di models.AppDI
				scope := r.Header.Get(rest.HeaderScopeKey)

				decoder := request.JSONDecoderGenerator(r.Body)
				err := decoder.Decode(&di)
				if err != nil {
					t.Error(err)
				}

				if scope != tt.args.context {
					t.Errorf("context set incorrectly. want = %v, got = %v", scope, tt.args.context)
				}

				if !reflect.DeepEqual(di.App, *tt.args.ch) {
					t.Errorf("request is different. want = \n%+v, \ngot = \n%+v", di.App, tt.args.ch)
				}
				encoder.Encode(diff.Changelog{})
			}
			s := httptest.NewServer(http.HandlerFunc(handler))
			defer s.Close()
			ac := &AppClient{
				reqClient: request.NewJSONClient(s.URL),
			}
			if _, err := ac.Create(tt.args.ctx, tt.args.context, tt.args.ch, false); (err != nil) != tt.wantErr {
				t.Errorf("AppClient.Create() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAppClient_Update(t *testing.T) {

	type args struct {
		ctx     context.Context
		context string
		ch      *meta.App
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test create app",
			args: args{
				ctx:     context.Background(),
				context: "app1.app2",
				ch: &meta.App{
					Meta: meta.Metadata{
						Name: "app3",
					},
					Spec: meta.AppSpec{
						Node: meta.Node{
							Meta: meta.Metadata{
								Name: "node",
							},
						},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"channel1"},
								Output: []string{"channel2"},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "create app with error test",
			args: args{
				ctx:     context.Background(),
				context: "app1.app2",
				ch:      &meta.App{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(w http.ResponseWriter, r *http.Request) {
				encoder := json.NewEncoder(w)
				if tt.wantErr {
					w.WriteHeader(http.StatusBadRequest)
					encoder.Encode(ierrors.New("").BadRequest())
					return
				}

				if r.URL.Path != "/apps" {
					t.Errorf("path is not apps")
				}

				if r.Method != http.MethodPut {
					t.Errorf("method is not PUT")
				}

				var di models.AppDI
				scope := r.Header.Get(rest.HeaderScopeKey)

				decoder := request.JSONDecoderGenerator(r.Body)
				err := decoder.Decode(&di)
				if err != nil {
					t.Error(err)
				}

				if scope != tt.args.context {
					t.Errorf("context set incorrectly. want = %v, got = %v", scope, tt.args.context)
				}

				if !reflect.DeepEqual(di.App, *tt.args.ch) {
					t.Errorf("request is different. want = \n%+v, \ngot = \n%+v", di.App, tt.args.ch)
				}
				encoder.Encode(diff.Changelog{})
			}
			s := httptest.NewServer(http.HandlerFunc(handler))
			defer s.Close()
			ac := &AppClient{
				reqClient: request.NewJSONClient(s.URL),
			}
			if _, err := ac.Update(tt.args.ctx, tt.args.context, tt.args.ch, false); (err != nil) != tt.wantErr {
				t.Errorf("AppClient.Update() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
