package utils

import (
	"encoding/json"
	"io"
	"os"

	"gitlab.inspr.dev/inspr/core/pkg/controller"
	"gitlab.inspr.dev/inspr/core/pkg/controller/client"
	"gitlab.inspr.dev/inspr/core/pkg/controller/mocks"
	"gitlab.inspr.dev/inspr/core/pkg/rest/request"
)

type cliGlobalStructure struct {
	client controller.Interface
	out    io.Writer
}

var defaults cliGlobalStructure

//GetCliClient returns the default controller client for cli.
func GetCliClient() controller.Interface {
	if defaults.client == nil {
		setGlobalClient()
	}
	return defaults.client
}

//GetCliOutput returns the default output for cli.
func GetCliOutput() io.Writer {
	if defaults.out == nil {
		setGlobalOutput()
	}
	return defaults.out
}

//SetDefaultClient creates cli's controller client from viper's configured serverIp
func setGlobalClient() {
	url := GetConfiguredServerIP()
	rc := request.NewClient().BaseURL(url).Encoder(json.Marshal).Decoder(request.JSONDecoderGenerator).Build()

	defaults.client = client.NewControllerClient(rc)
}

func setGlobalOutput() {
	defaults.out = os.Stdout
}

// SetOutput sets the default output of CLI
func SetOutput(out io.Writer) {
	defaults.out = out
}

// SetClient sets the default server IP of CLI
func SetClient(url string) {
	rc := request.NewClient().BaseURL(url).Encoder(json.Marshal).Decoder(request.JSONDecoderGenerator).Build()
	defaults.client = client.NewControllerClient(rc)
}

//SetMockedClient configures singleton's client as a mocked client given a error
func SetMockedClient(err error) {
	defaults.client = mocks.NewClientMock(err)
}
