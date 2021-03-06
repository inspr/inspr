package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/spf13/cobra"
	"inspr.dev/inspr/pkg/api/models"
	"inspr.dev/inspr/pkg/cmd"
	cliutils "inspr.dev/inspr/pkg/cmd/utils"
	"inspr.dev/inspr/pkg/meta"
	"inspr.dev/inspr/pkg/meta/utils"
	"inspr.dev/inspr/pkg/rest"
)

func restartScopeFlag() {
	cmd.InsprOptions.Scope = ""
}

func prepareToken(t *testing.T) {
	dir := t.TempDir()
	ioutil.WriteFile(
		filepath.Join(dir, "token"),
		[]byte("Bearer mock_token"),
		os.ModePerm,
	)
	cmd.InsprOptions.Token = filepath.Join(dir, "token")
}

func getMockApp() *meta.App {
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
			Aliases: map[string]*meta.Alias{
				"alias.name": {
					Meta: meta.Metadata{
						Name: "alias.name",
					},
					Resource: "alias_target",
				},
			},
		},
	}
	return &root
}

func TestNewDescribeCmd(t *testing.T) {
	prepareToken(t)
	defer restartScopeFlag()
	tests := []struct {
		name          string
		checkFunction func(t *testing.T, got *cobra.Command)
	}{
		{
			name: "It should create a new describe command",
			checkFunction: func(t *testing.T, got *cobra.Command) {
				if got == nil {
					t.Errorf("NewDescribeCmd() not created successfully")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewDescribeCmd()
			if tt.checkFunction != nil {
				tt.checkFunction(t, got)
			}
		})
	}
}

func Test_displayAppState(t *testing.T) {
	prepareToken(t)
	defer restartScopeFlag()
	bufResp := bytes.NewBufferString("")
	utils.PrintAppTree(getMockApp(), bufResp)

	handler := func(w http.ResponseWriter, r *http.Request) {
		data := models.AppQueryDI{}
		decoder := json.NewDecoder(r.Body)

		err := decoder.Decode(&data)
		if err != nil {
			fmt.Println(err)
		}

		app := getMockApp()

		rest.JSON(w, http.StatusOK, app)
	}

	tests := []struct {
		name           string
		host           string
		flagsAndArgs   []string
		handlerFunc    func(w http.ResponseWriter, r *http.Request)
		expectedOutput string
	}{
		{
			name:           "Should describe the app state",
			flagsAndArgs:   []string{"a", "appParent"},
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
			flagsAndArgs:   []string{"a", "", "--scope", "appParent"},
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
			cmd := NewDescribeCmd()
			buf := bytes.NewBufferString("")

			cliutils.SetOutput(buf)
			cmd.SetArgs(tt.flagsAndArgs)

			server := httptest.NewServer(http.HandlerFunc(tt.handlerFunc))
			cliutils.SetClient(server.URL, tt.host)

			defer server.Close()

			cmd.Execute()
			got := buf.String()

			if !reflect.DeepEqual(got, tt.expectedOutput) {
				t.Errorf("displayAppState() = %v, want %v", got, tt.expectedOutput)
			}
		})
	}
}

func Test_displayChannelState(t *testing.T) {
	prepareToken(t)
	defer restartScopeFlag()
	bufResp := bytes.NewBufferString("")
	utils.PrintChannelTree(getMockApp().Spec.Channels["ch1"], bufResp)

	handler := func(w http.ResponseWriter, r *http.Request) {
		data := models.ChannelQueryDI{}
		decoder := json.NewDecoder(r.Body)

		err := decoder.Decode(&data)
		if err != nil {
			fmt.Println(err)
		}

		ch := getMockApp().Spec.Channels[data.ChName]

		rest.JSON(w, http.StatusOK, ch)
	}

	tests := []struct {
		name           string
		host           string
		flagsAndArgs   []string
		handlerFunc    func(w http.ResponseWriter, r *http.Request)
		expectedOutput string
	}{
		{
			name:           "Should describe the channel state",
			flagsAndArgs:   []string{"ch", "appParent.ch1"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid scope flag, should not print",
			flagsAndArgs:   []string{"ch", "ch1", "--scope", "invalid..scope"},
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
			flagsAndArgs:   []string{"ch", "invalid..args", "--scope", "appParent"},
			handlerFunc:    handler,
			expectedOutput: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewDescribeCmd()
			buf := bytes.NewBufferString("")

			cliutils.SetOutput(buf)
			cmd.SetArgs(tt.flagsAndArgs)

			server := httptest.NewServer(http.HandlerFunc(tt.handlerFunc))
			cliutils.SetClient(server.URL, tt.host)

			defer server.Close()

			cmd.Execute()
			got := buf.String()

			if !reflect.DeepEqual(got, tt.expectedOutput) {
				t.Errorf("displayChannelState() = %v, want %v", got, tt.expectedOutput)
			}
		})
	}
}

func Test_displayTypeState(t *testing.T) {
	prepareToken(t)
	defer restartScopeFlag()
	bufResp := bytes.NewBufferString("")
	utils.PrintTypeTree(getMockApp().Spec.Types["ct1"], bufResp)
	prepareToken(t)

	handler := func(w http.ResponseWriter, r *http.Request) {
		data := models.TypeQueryDI{}
		decoder := json.NewDecoder(r.Body)

		err := decoder.Decode(&data)
		if err != nil {
			fmt.Println(err)
		}

		t := getMockApp().Spec.Types[data.TypeName]

		rest.JSON(w, http.StatusOK, t)
	}

	tests := []struct {
		name           string
		host           string
		flagsAndArgs   []string
		handlerFunc    func(w http.ResponseWriter, r *http.Request)
		expectedOutput string
	}{
		{
			name:           "Should describe the type state",
			flagsAndArgs:   []string{"t", "appParent.ct1"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid scope flag, should not print",
			flagsAndArgs:   []string{"t", "ct1", "--scope", "invalid..scope"},
			handlerFunc:    handler,
			expectedOutput: "",
		},
		{
			name:           "Valid scope flag",
			flagsAndArgs:   []string{"t", "ct1", "--scope", "appParent"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid arg",
			flagsAndArgs:   []string{"t", "invalid..args", "--scope", "appParent"},
			handlerFunc:    handler,
			expectedOutput: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewDescribeCmd()
			buf := bytes.NewBufferString("")

			cliutils.SetOutput(buf)
			cmd.SetArgs(tt.flagsAndArgs)

			server := httptest.NewServer(http.HandlerFunc(tt.handlerFunc))
			cliutils.SetClient(server.URL, tt.host)

			defer server.Close()

			cmd.Execute()
			got := buf.String()

			if !reflect.DeepEqual(got, tt.expectedOutput) {
				t.Errorf("displayTypeState() = %v, want %v", got, tt.expectedOutput)
			}
		})
	}
}

func Test_displayAlias(t *testing.T) {
	prepareToken(t)
	defer restartScopeFlag()
	bufResp := bytes.NewBufferString("")
	utils.PrintAliasTree(getMockApp().Spec.Aliases["alias.name"], bufResp)

	handler := func(w http.ResponseWriter, r *http.Request) {
		data := models.AliasQueryDI{}
		decoder := json.NewDecoder(r.Body)

		err := decoder.Decode(&data)
		if err != nil {
			fmt.Println(err)
		}

		al := getMockApp().Spec.Aliases[data.Name]

		rest.JSON(w, http.StatusOK, al)
	}

	tests := []struct {
		name           string
		host           string
		flagsAndArgs   []string
		handlerFunc    func(w http.ResponseWriter, r *http.Request)
		expectedOutput string
	}{
		{
			name:           "Should describe the alias state",
			flagsAndArgs:   []string{"al", "appParent.alias.name"},
			handlerFunc:    handler,
			expectedOutput: bufResp.String(),
		},
		{
			name:           "Invalid scope flag, should not print",
			flagsAndArgs:   []string{"al", "alias.name", "--scope", "invalid..scope"},
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
			flagsAndArgs:   []string{"al", "invalid..args", "--scope", "appParent"},
			handlerFunc:    handler,
			expectedOutput: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewDescribeCmd()
			buf := bytes.NewBufferString("")

			cliutils.SetOutput(buf)
			cmd.SetArgs(tt.flagsAndArgs)

			server := httptest.NewServer(http.HandlerFunc(tt.handlerFunc))
			cliutils.SetClient(server.URL, tt.host)

			defer server.Close()

			cmd.Execute()
			got := buf.String()

			if !reflect.DeepEqual(got, tt.expectedOutput) {
				t.Errorf("displayAliasState() = %v, want %v", got, tt.expectedOutput)
			}
		})
	}
}
