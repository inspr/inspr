package cli

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	cliutils "inspr.dev/inspr/cmd/inspr/cli/utils"
	"inspr.dev/inspr/pkg/meta"
	"gopkg.in/yaml.v2"
)

const (
	filePath = "filetest.yaml"
)

func createDAppYaml() string {
	comp := meta.Component{
		Kind:       "dapp",
		APIVersion: "v1",
	}
	data, _ := yaml.Marshal(&comp)
	return string(data)
}

func createChannelYaml() string {
	comp := meta.Component{
		Kind:       "channel",
		APIVersion: "v1",
	}
	data, _ := yaml.Marshal(&comp)
	return string(data)
}

func createChannelTypeYaml() string {
	comp := meta.Component{
		Kind:       "channeltype",
		APIVersion: "v1",
	}
	data, _ := yaml.Marshal(&comp)
	return string(data)
}

func createInvalidYaml() string {
	comp := meta.Component{
		Kind:       "none",
		APIVersion: "",
	}
	data, _ := yaml.Marshal(&comp)
	return string(data)
}

func getCurrentFilesInFolder() []string {
	var files []string
	folder, _ := ioutil.ReadDir(".")

	for _, file := range folder {
		files = append(files, file.Name())
	}
	return files
}

// TestNewApplyCmd is mainly for improving test coverage,
// it was really tested by instantiating Inspr's CLI
func TestNewApplyCmd(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "Creates a new Cobra command",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewApplyCmd()
			if got == nil {
				t.Errorf("NewApplyCmd() = %v", got)
			}
		})
	}
}

func Test_isYaml(t *testing.T) {
	type args struct {
		file string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Given file is yaml",
			args: args{
				file: "itsAYaml.yaml",
			},
			want: true,
		},
		{
			name: "Given file is yml",
			args: args{
				file: "itsAYml.yml",
			},
			want: true,
		},
		{
			name: "Given file is another extension",
			args: args{
				file: "itsNotAYaml.txt",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isYaml(tt.args.file); got != tt.want {
				t.Errorf("isYaml() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_printAppliedFiles(t *testing.T) {
	type args struct {
		appliedFiles []applied
	}
	tests := []struct {
		name    string
		args    args
		wantOut string
	}{
		{
			name: "Prints a valid file",
			args: args{
				[]applied{{
					fileName: "aFile.yaml",
					component: meta.Component{
						Kind:       "randKind",
						APIVersion: "v1",
					},
				}},
			},
			wantOut: "\nApplied:\naFile.yaml | randKind | v1\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := &bytes.Buffer{}
			printAppliedFiles(tt.args.appliedFiles, out)
			if gotOut := out.String(); gotOut != tt.wantOut {
				t.Errorf("printAppliedFiles() = %v, want %v", gotOut, tt.wantOut)
			}
		})
	}
}

func Test_doApply(t *testing.T) {
	defer os.Remove(filePath)
	yamlString := createDAppYaml()

	bufResp := bytes.NewBufferString("")
	fmt.Fprintf(bufResp, "filetest.yaml\n\nApplied:\nfiletest.yaml | dapp | v1\n")
	outResp, _ := ioutil.ReadAll(bufResp)

	bufResp2 := bytes.NewBufferString("")
	fmt.Fprintln(bufResp2, "Invalid command call\nFor help, type 'inspr apply --help'")
	outResp2, _ := ioutil.ReadAll(bufResp2)

	bufResp3 := bytes.NewBufferString("")
	fmt.Fprint(bufResp3, "No files were applied\nFiles to be applied must be .yaml or .yml\n")
	outResp3, _ := ioutil.ReadAll(bufResp3)

	// creates a file with the expected syntax
	ioutil.WriteFile(
		filePath,
		[]byte(yamlString),
		os.ModePerm,
	)

	tests := []struct {
		name           string
		flagsAndArgs   []string
		expectedOutput []byte
	}{
		{
			name:           "Should apply the file",
			flagsAndArgs:   []string{"-f", "filetest.yaml"},
			expectedOutput: outResp,
		},
		{
			name:           "Too many flags, should raise an error",
			flagsAndArgs:   []string{"-f", "example", "-k", "example"},
			expectedOutput: outResp2,
		},
		{
			name:           "No files applied",
			flagsAndArgs:   []string{"-f", "example.yaml"},
			expectedOutput: outResp3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GetFactory().Subscribe(meta.Component{
				APIVersion: "v1",
				Kind:       "dapp",
			},
				func(b []byte, out io.Writer) error {
					return nil
				})

			cmd := NewApplyCmd()
			buf := bytes.NewBufferString("")

			cliutils.SetOutput(buf)
			cmd.SetArgs(tt.flagsAndArgs)

			cmd.Execute()
			got, _ := ioutil.ReadAll(buf)

			if !reflect.DeepEqual(got, tt.expectedOutput) {
				t.Errorf("doApply() = %v, want\n%v", string(got), string(tt.expectedOutput))
			}
		})
	}
}

func Test_getFilesFromFolder(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		want    []string
	}{
		{
			name: "Get file from current folder",
			args: args{
				path: ".",
			},
			wantErr: false,
			want:    getCurrentFilesInFolder(),
		},
		{
			name: "Invalid - path doesn't exist",
			args: args{
				path: "invalid/",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getFilesFromFolder(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("getFilesFromFolder() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getFilesFromFolder() = %v, want %v", got, tt.want)
				return
			}
		})
	}
}

func Test_applyValidFiles(t *testing.T) {
	defer os.Remove(filePath)
	tempFiles := []string{filePath}
	yamlString := createDAppYaml()
	// creates a file with the expected syntax
	ioutil.WriteFile(
		filePath,
		[]byte(yamlString),
		os.ModePerm,
	)

	type args struct {
		path  string
		files []string
	}
	tests := []struct {
		name string
		args args
		want []applied
	}{
		{
			name: "Get file from current folder",
			args: args{
				path:  "",
				files: tempFiles,
			},
			want: []applied{{
				fileName: filePath,
				component: meta.Component{
					Kind:       "dapp",
					APIVersion: "v1",
				},
				content: []byte(yamlString),
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := &bytes.Buffer{}
			GetFactory().Subscribe(meta.Component{
				APIVersion: "v1",
				Kind:       "dapp",
			},
				func(b []byte, out io.Writer) error {
					ch := meta.Channel{}

					yaml.Unmarshal(b, &ch)
					fmt.Println(ch)

					return nil
				})
			if got := applyValidFiles(tt.args.path, tt.args.files, out); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("applyValidFiles() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getOrderedFiles(t *testing.T) {
	defer os.Remove("app.yml")
	defer os.Remove("ch.yml")
	defer os.Remove("ct.yml")
	defer os.Remove("invalid.yml")
	tempFiles := []string{"app.yml", "invalid.yml",
		"ch.yml", "ct.yml"}
	// creates a file with the expected syntax
	ioutil.WriteFile(
		"app.yml",
		[]byte(createDAppYaml()),
		os.ModePerm,
	)
	ioutil.WriteFile(
		"ch.yml",
		[]byte(createChannelYaml()),
		os.ModePerm,
	)
	ioutil.WriteFile(
		"ct.yml",
		[]byte(createChannelTypeYaml()),
		os.ModePerm,
	)
	ioutil.WriteFile(
		"invalid.yml",
		[]byte(createInvalidYaml()),
		os.ModePerm,
	)

	type args struct {
		path  string
		files []string
	}
	tests := []struct {
		name string
		args args
		want []applied
	}{
		{
			name: "Return ordered files",
			args: args{
				path:  ".",
				files: tempFiles,
			},
			want: orderedContent(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getOrderedFiles(tt.args.path, tt.args.files); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getOrderedFiles() = %v, want %v", got, tt.want)
			}
		})
	}
}

func orderedContent() []applied {
	ordered := []applied{
		{
			fileName: "app.yml",
			component: meta.Component{
				Kind:       "dapp",
				APIVersion: "v1",
			},
			content: []byte(createDAppYaml()),
		},
		{
			fileName: "ct.yml",
			component: meta.Component{
				Kind:       "channeltype",
				APIVersion: "v1",
			},
			content: []byte(createChannelTypeYaml()),
		},
		{
			fileName: "ch.yml",
			component: meta.Component{
				Kind:       "channel",
				APIVersion: "v1",
			},
			content: []byte(createChannelYaml()),
		},
	}

	return ordered
}
