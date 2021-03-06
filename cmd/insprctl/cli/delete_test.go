package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/spf13/cobra"
	"inspr.dev/inspr/pkg/api/models"
	cliutils "inspr.dev/inspr/pkg/cmd/utils"
	"inspr.dev/inspr/pkg/ierrors"
	"inspr.dev/inspr/pkg/meta"
	"inspr.dev/inspr/pkg/meta/utils/diff"
	"inspr.dev/inspr/pkg/rest"
)

func getMockAppWithoutApp1() *meta.App {
	root := meta.App{
		Meta: meta.Metadata{
			Name:        "appParent",
			Reference:   "appParent",
			Annotations: map[string]string{},
			Parent:      "",
			UUID:        "",
		},
		Spec: meta.AppSpec{
			Node: meta.Node{},
			Apps: map[string]*meta.App{},
			Channels: map[string]*meta.Channel{
				"ch1": {
					Meta: meta.Metadata{
						Name:   "ch1",
						Parent: "",
					},
					Spec: meta.ChannelSpec{
						Type: "ct1",
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
						UUID:        "",
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

func getMockAppWithoutCh1() *meta.App {
	root := meta.App{
		Meta: meta.Metadata{
			Name:        "appParent",
			Reference:   "appParent",
			Annotations: map[string]string{},
			Parent:      "",
			UUID:        "",
		},
		Spec: meta.AppSpec{
			Node: meta.Node{},
			Apps: map[string]*meta.App{
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
						Channels: map[string]*meta.Channel{},
						Types:    map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1"},
								Output: []string{"ch1"},
							},
						},
					},
				},
			},
			Channels: map[string]*meta.Channel{
				"ch1": {
					Meta: meta.Metadata{
						Name:   "ch1",
						Parent: "",
					},
					Spec: meta.ChannelSpec{
						Type: "ct1",
					},
				},
			},
			Types: map[string]*meta.Type{},
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

func getMockAppWithoutCt1() *meta.App {
	root := meta.App{
		Meta: meta.Metadata{
			Name:        "appParent",
			Reference:   "appParent",
			Annotations: map[string]string{},
			Parent:      "",
			UUID:        "",
		},
		Spec: meta.AppSpec{
			Node: meta.Node{},
			Apps: map[string]*meta.App{
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
						},
						Types: map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1"},
								Output: []string{"ch1"},
							},
						},
					},
				},
			},
			Channels: map[string]*meta.Channel{
				"ch1": {
					Meta: meta.Metadata{
						Name:   "ch1",
						Parent: "",
					},
					Spec: meta.ChannelSpec{
						Type: "ct1",
					},
				},
			},
			Types: map[string]*meta.Type{},
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

func getMockAppWithoutAlias() *meta.App {
	root := meta.App{
		Meta: meta.Metadata{
			Name:        "appParent",
			Reference:   "appParent",
			Annotations: map[string]string{},
			Parent:      "",
			UUID:        "",
		},
		Spec: meta.AppSpec{
			Node: meta.Node{},
			Apps: map[string]*meta.App{
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
									Node: meta.Node{
										Meta: meta.Metadata{
											Name:        "thenewapp",
											Reference:   "app1.thenewapp",
											Annotations: map[string]string{},
											Parent:      "app1",
											UUID:        "",
										},
									},
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
						},
						Types: map[string]*meta.Type{},
						Boundary: meta.AppBoundary{
							Channels: meta.Boundary{
								Input:  []string{"ch1"},
								Output: []string{"ch1"},
							},
						},
					},
				},
			},
			Channels: map[string]*meta.Channel{
				"ch1": {
					Meta: meta.Metadata{
						Name:   "ch1",
						Parent: "",
					},
					Spec: meta.ChannelSpec{
						Type: "ct1",
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
						UUID:        "",
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
			Aliases: map[string]*meta.Alias{},
		},
	}
	return &root
}

func TestNewDeleteCmd(t *testing.T) {
	prepareToken(t)
	defer restartScopeFlag()
	tests := []struct {
		name          string
		checkFunction func(t *testing.T, got *cobra.Command)
	}{
		{
			name: "It should create a new delete command",
			checkFunction: func(t *testing.T, got *cobra.Command) {
				if got == nil {
					t.Errorf("NewDeleteCmd() not created successfully")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewDeleteCmd()
			if tt.checkFunction != nil {
				tt.checkFunction(t, got)
			}
		})
	}
}

func Test_deleteApps(t *testing.T) {
	prepareToken(t)
	defer restartScopeFlag()

	bufResp := bytes.NewBufferString("")
	changelog, _ := diff.Diff(getMockApp(), getMockAppWithoutApp1())
	changelog.Print(bufResp)

	handler := func(w http.ResponseWriter, r *http.Request) {
		data := models.AppQueryDI{}
		decoder := json.NewDecoder(r.Body)
		scope := r.Header.Get(rest.HeaderScopeKey)

		err := decoder.Decode(&data)
		if err != nil {
			fmt.Println(err)
		}

		if scope != "appParent.app1" {
			rest.ERROR(w, ierrors.New("error test"))
			return
		}

		rest.JSON(w, http.StatusOK, changelog)
	}

	tests := []struct {
		name           string
		flagsAndArgs   []string
		host           string
		handlerFunc    func(w http.ResponseWriter, r *http.Request)
		expectedOutput string
	}{
		{
			name:           "Should delete the app and return the diff",
			flagsAndArgs:   []string{"a", "appParent.app1"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid scope flag, should not print",
			flagsAndArgs:   []string{"a", "appParent", "--scope", "invalid..scope"},
			handlerFunc:    handler,
			expectedOutput: "",
		},
		{
			name:           "Valid scope flag",
			flagsAndArgs:   []string{"a", "", "--scope", "appParent.app1"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid arg",
			flagsAndArgs:   []string{"a", "invalid..args", "--scope", "appParent"},
			handlerFunc:    handler,
			expectedOutput: "invalid args\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewDeleteCmd()
			buf := bytes.NewBufferString("")

			cliutils.SetOutput(buf)
			cmd.SetArgs(tt.flagsAndArgs)

			server := httptest.NewServer(http.HandlerFunc(tt.handlerFunc))
			cliutils.SetClient(server.URL, tt.host)

			defer server.Close()

			cmd.Execute()
			got := buf.String()

			if !reflect.DeepEqual(got, tt.expectedOutput) {
				t.Errorf("deleteApps() = %v, want %v", got, tt.expectedOutput)
			}
		})
	}
}

func Test_deleteChannels(t *testing.T) {
	prepareToken(t)
	defer restartScopeFlag()

	bufResp := bytes.NewBufferString("")
	changelog, _ := diff.Diff(getMockApp(), getMockAppWithoutCh1())
	changelog.Print(bufResp)

	handler := func(w http.ResponseWriter, r *http.Request) {
		data := models.ChannelQueryDI{}
		scope := r.Header.Get(rest.HeaderScopeKey)
		decoder := json.NewDecoder(r.Body)

		err := decoder.Decode(&data)
		if err != nil {
			fmt.Println(err)
		}

		if scope != "appParent" || data.ChName != "ch1" {
			rest.ERROR(w, ierrors.New("error test"))
			return
		}

		rest.JSON(w, http.StatusOK, changelog)
	}

	tests := []struct {
		name           string
		flagsAndArgs   []string
		host           string
		handlerFunc    func(w http.ResponseWriter, r *http.Request)
		expectedOutput string
	}{
		{
			name:           "Should delete the channel and return the diff",
			flagsAndArgs:   []string{"ch", "appParent.ch1"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid scope flag, should not print",
			flagsAndArgs:   []string{"ch", "appParent.ch1", "--scope", "invalid..scope"},
			handlerFunc:    handler,
			expectedOutput: "",
		},
		{
			name:           "Valid scope flag",
			flagsAndArgs:   []string{"ch", "ch1", "--scope", "appParent"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid arg",
			flagsAndArgs:   []string{"ch", "invalid..args", "--scope", "appParent.ch1"},
			handlerFunc:    handler,
			expectedOutput: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewDeleteCmd()
			buf := bytes.NewBufferString("")

			cliutils.SetOutput(buf)
			cmd.SetArgs(tt.flagsAndArgs)

			server := httptest.NewServer(http.HandlerFunc(tt.handlerFunc))
			cliutils.SetClient(server.URL, tt.host)

			defer server.Close()

			cmd.Execute()
			got := buf.String()

			if !reflect.DeepEqual(got, tt.expectedOutput) {
				t.Errorf("deleteChannels() = %v, want %v", got, tt.expectedOutput)
			}
		})
	}
}

func Test_deletetypes(t *testing.T) {
	prepareToken(t)
	defer restartScopeFlag()
	bufResp := bytes.NewBufferString("")
	changelog, _ := diff.Diff(getMockApp(), getMockAppWithoutCt1())

	changelog.Print(bufResp)

	handler := func(w http.ResponseWriter, r *http.Request) {
		data := models.TypeQueryDI{}
		decoder := json.NewDecoder(r.Body)
		scope := r.Header.Get(rest.HeaderScopeKey)

		err := decoder.Decode(&data)
		if err != nil {
			fmt.Println(err)
		}

		if scope != "appParent" || data.TypeName != "t1" {
			rest.ERROR(w, ierrors.New("error test"))
			return
		}

		rest.JSON(w, http.StatusOK, changelog)
	}

	tests := []struct {
		name           string
		flagsAndArgs   []string
		host           string
		handlerFunc    func(w http.ResponseWriter, r *http.Request)
		expectedOutput string
	}{
		{
			name:           "Should delete the type and return the diff",
			flagsAndArgs:   []string{"t", "appParent.t1"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid scope flag, should not print",
			flagsAndArgs:   []string{"t", "appParent.ct1", "--scope", "invalid..scope"},
			handlerFunc:    handler,
			expectedOutput: "",
		},
		{
			name:           "Valid scope flag",
			flagsAndArgs:   []string{"t", "t1", "--scope", "appParent"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid arg",
			flagsAndArgs:   []string{"t", "invalid..args", "--scope", "appParent.t1"},
			handlerFunc:    handler,
			expectedOutput: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewDeleteCmd()
			buf := bytes.NewBufferString("")

			cliutils.SetOutput(buf)
			cmd.SetArgs(tt.flagsAndArgs)

			server := httptest.NewServer(http.HandlerFunc(tt.handlerFunc))
			cliutils.SetClient(server.URL, tt.host)

			defer server.Close()

			cmd.Execute()
			got := buf.String()

			if !reflect.DeepEqual(got, tt.expectedOutput) {
				t.Errorf("deletetypes() = %v, want %v", got, tt.expectedOutput)
			}
		})
	}
}

func Test_deleteAlias(t *testing.T) {
	prepareToken(t)
	defer restartScopeFlag()

	bufResp := bytes.NewBufferString("")
	changelog, _ := diff.Diff(getMockApp(), getMockAppWithoutAlias())
	changelog.Print(bufResp)

	handler := func(w http.ResponseWriter, r *http.Request) {
		data := models.AliasQueryDI{}
		decoder := json.NewDecoder(r.Body)
		scope := r.Header.Get(rest.HeaderScopeKey)

		err := decoder.Decode(&data)
		if err != nil {
			fmt.Println(err)
		}

		fmt.Println(scope)
		if (scope != "appParent" && scope != "") ||
			(data.Name != "alias.name" && data.Name != "appParent.alias.name") {

			rest.ERROR(w, ierrors.New("error test"))
			return
		}

		rest.JSON(w, http.StatusOK, changelog)
	}

	tests := []struct {
		name           string
		flagsAndArgs   []string
		host           string
		handlerFunc    func(w http.ResponseWriter, r *http.Request)
		expectedOutput string
	}{
		{
			name:           "Should delete the alias and return the diff",
			flagsAndArgs:   []string{"al", "appParent.alias.name"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid scope flag, should not print",
			flagsAndArgs:   []string{"al", "appParent.alias.name", "--scope", "invalid..scope"},
			handlerFunc:    handler,
			expectedOutput: "",
		},
		{
			name:           "Valid scope flag",
			flagsAndArgs:   []string{"al", "alias.name", "--scope", "appParent"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid arg",
			flagsAndArgs:   []string{"al", "invalid..args", "--scope", "appParent.ct1"},
			handlerFunc:    handler,
			expectedOutput: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewDeleteCmd()
			buf := bytes.NewBufferString("")

			cliutils.SetOutput(buf)
			cmd.SetArgs(tt.flagsAndArgs)

			server := httptest.NewServer(http.HandlerFunc(tt.handlerFunc))
			cliutils.SetClient(server.URL, tt.host)

			defer server.Close()

			cmd.Execute()

			got := buf.String()

			if !reflect.DeepEqual(got, tt.expectedOutput) {
				t.Errorf("deleteAlias() = %v, want %v", got, tt.expectedOutput)
			}
		})
	}
}
