package environment

import (
	"os"
	"reflect"
	"testing"
)

func mockInsprEnvironment() *InsprEnvVars {
	return &InsprEnvVars{
		InputChannels:    "chan@someBroker;chan1@someBroker;chan2@someBroker;chan3@someBroker",
		OutputChannels:   "chan@someBroker;chan1@someBroker;chan2@someBroker;chan3@someBroker",
		SidecarImage:     "mock_sidecar_image",
		InsprAppContext:  "mock.dapp.context",
		InsprEnvironment: "mock_env",
		InsprAppID:       "testappid1",
	}
}

func TestGetEnvironment(t *testing.T) {
	SetMockEnv()
	defer UnsetMockEnv()
	tests := []struct {
		name string
		want *InsprEnvVars
	}{
		{
			name: "Get all environment variables",
			want: mockInsprEnvironment(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if got := GetEnvironment(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetEnvironment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getEnv(t *testing.T) {
	SetMockEnv()
	defer UnsetMockEnv()
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Get input channel environment variable",
			args: args{
				name: "INSPR_INPUT_CHANNELS",
			},
			want: "chan@someBroker;chan1@someBroker;chan2@someBroker;chan3@someBroker",
		},
		{
			name: "Get output channel environment variable",
			args: args{
				name: "INSPR_OUTPUT_CHANNELS",
			},
			want: "chan@someBroker;chan1@someBroker;chan2@someBroker;chan3@someBroker",
		},
		{
			name: "Get unix socket environment variable",
			args: args{
				name: "INSPR_UNIX_SOCKET",
			},
			want: "socket_addr",
		},
		{
			name: "Invalid - Get invalid environment variable",
			args: args{
				name: "INSPR_INVALID_ENV_VAR",
			},
			want: "[ENV VAR] INSPR_INVALID_ENV_VAR not found",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			defer func() {
				recover()
			}()

			if got := getEnv(tt.args.name); got != tt.want {
				t.Errorf("getEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRefreshEnviromentVariables(t *testing.T) {
	SetMockEnv()
	defer UnsetMockEnv()
	os.Setenv("INSPR_INPUT_CHANNELS", "one")
	os.Setenv("INSPR_OUTPUT_CHANNELS", "two")
	os.Setenv("INSPR_UNIX_SOCKET", "three")
	os.Setenv("INSPR_APP_SCOPE", "four")
	os.Setenv("INSPR_ENV", "five")
	os.Setenv("INSPR_APP_ID", "six")
	os.Setenv("INSPR_LBSIDECAR_IMAGE", "seven")
	tests := []struct {
		name    string
		refresh bool
		want    *InsprEnvVars
	}{
		{
			name:    "Changed and refreshed environment variables",
			refresh: true,
			want: &InsprEnvVars{
				InputChannels:    "one",
				OutputChannels:   "two",
				InsprAppContext:  "four",
				InsprEnvironment: "five",
				SidecarImage:     "seven",
				InsprAppID:       "six",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if got := RefreshEnviromentVariables(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetEnvironment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecoverEnvironmentErrors(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
		before  func()
	}{
		{
			name:    "no environment errors",
			wantErr: false,
			before: func() {
				SetMockEnv()
			},
		},
		{
			name:    "environment errors",
			wantErr: true,
			before: func() {
				UnsetMockEnv()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.before != nil {
				tt.before()
			}
			// creating the error channel to hold recovered errors
			cherr := make(chan error, 10)

			var err error
			func() {
				defer RecoverEnvironmentErrors(cherr)
				RefreshEnviromentVariables()
			}()

			err = <-cherr

			if (err != nil) != tt.wantErr {
				t.Errorf("RecoverEnvironmentErrors() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSidecarEnvVarsMethods(t *testing.T) {
	SetMockEnv()
	defer UnsetMockEnv()

	t.Run("Get Broker-specific sidecars env vars", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Recovered in 'TestSidecarEnvVarsMethods': %v", r)
			}
		}()
		GetBrokerReadPort("TEST")
		GetBrokerWritePort("TEST")
		GetBrokerSpecificSidecarAddr("TEST")
	})
}
