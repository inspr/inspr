package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"gitlab.inspr.dev/inspr/core/cmd/insprd/api/models"
	"gitlab.inspr.dev/inspr/core/cmd/insprd/memory"
	"gitlab.inspr.dev/inspr/core/cmd/insprd/memory/fake"
	"gitlab.inspr.dev/inspr/core/cmd/insprd/operators"
	ofake "gitlab.inspr.dev/inspr/core/cmd/insprd/operators/fake"
	"gitlab.inspr.dev/inspr/core/pkg/ierrors"
	"gitlab.inspr.dev/inspr/core/pkg/meta"
)

type AliasAPITest struct {
	name string
	ah   *AliasHandler
	send sendInRequest
	want expectedResponse
}

// AliasDICases - generates the test cases to be used in functions that
// handle the use the AliasDI struct of the models package.
// For example, HandleCreateAlias and HandleUpdateAlias use these test cases
func AliasDICases(funcName string) []AliasAPITest {
	parsedAliasDI, _ := json.Marshal(models.AliasDI{
		Alias: meta.Alias{
			Target: "mock_Alias",
		},
		Ctx: "",
	})
	wrongFormatData := []byte{1}
	return []AliasAPITest{
		{
			name: "successful_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(nil), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusOK},
		},
		{
			name: "unsuccessful_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(errors.New("test_error")), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "bad_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(nil), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: wrongFormatData},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "not_found_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().NotFound().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusNotFound},
		},
		{
			name: "already_exists_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().AlreadyExists().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusConflict},
		},
		{
			name: "internal_server_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().InternalServer().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "invalid_name_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().InvalidName().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_app_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().InvalidApp().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_channel_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().InvalidChannel().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_channel_type_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().InvalidChannelType().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "bad_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().BadRequest().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusBadRequest},
		},
	}
}

// AliasQueryDICases - generates the test cases to be used in functions
// that handle the use the AliasQueryDI struct of the models package.
// For example, HandleGetAliasByRef and HandleDeleteAlias use these test cases
func AliasQueryDICases(funcName string) []AliasAPITest {
	parsedAliasQueryDI, _ := json.Marshal(models.AliasQueryDI{
		Ctx:    "",
		Key:    "mock_Alias",
		DryRun: false,
	})
	wrongFormatData := []byte{1}
	return []AliasAPITest{
		{
			name: "successful_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(nil), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusOK},
		},
		{
			name: "unsuccessful_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(errors.New("test_error")), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "bad_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(nil), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: wrongFormatData},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "not_found_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().NotFound().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusNotFound},
		},
		{
			name: "already_exists_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().AlreadyExists().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusConflict},
		},
		{
			name: "internal_server_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().InternalServer().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "invalid_name_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().InvalidName().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_app_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().InvalidApp().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_channel_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().InvalidChannel().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_channel_type_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().InvalidChannelType().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "bad_request_" + funcName,
			ah:   NewHandler(fake.MockMemoryManager(ierrors.NewError().BadRequest().Build()), ofake.NewFakeOperator()).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusBadRequest},
		},
	}
}

func TestNewAliasHandler(t *testing.T) {
	h := NewHandler(
		fake.MockMemoryManager(nil),
		ofake.NewFakeOperator(),
	)
	type args struct {
		memManager memory.Manager
		op         operators.OperatorInterface
	}
	tests := []struct {
		name string
		args args
		want *AliasHandler
	}{
		{
			name: "success_CreateAliasHandler",
			args: args{
				memManager: fake.MockMemoryManager(nil),
				op:         ofake.NewFakeOperator(),
			},
			want: &AliasHandler{
				h,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := h.NewAliasHandler(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewAliasHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAliasHandler_HandleCreateAlias(t *testing.T) {
	tests := AliasDICases("HandleCreateAlias")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerFunc := tt.ah.HandleCreateAlias().HTTPHandlerFunc()
			ts := httptest.NewServer(handlerFunc)
			defer ts.Close()

			client := ts.Client()
			res, err := client.Post(ts.URL, "application/json", bytes.NewBuffer(tt.send.body))
			if err != nil {
				t.Log("error making a POST in the httptest server")
				return
			}
			defer res.Body.Close()

			if res.StatusCode != tt.want.status {
				t.Errorf("AliasHandler.HandleCreateAlias() = %v, want %v", res.StatusCode, tt.want.status)
			}
		})
	}
}

func TestAliasHandler_HandleGetAlias(t *testing.T) {
	tests := AliasQueryDICases("HandleGetAliasByRef")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerFunc := tt.ah.HandleGet().HTTPHandlerFunc()
			ts := httptest.NewServer(handlerFunc)
			defer ts.Close()

			tt.ah.Memory.Alias().CreateAlias("", "ch", &meta.Alias{Target: "mock_Alias"})

			client := ts.Client()
			res, err := client.Post(ts.URL, "application/json", bytes.NewBuffer(tt.send.body))
			if err != nil {
				t.Log("error making a POST in the httptest server")
				return
			}
			defer res.Body.Close()

			if res.StatusCode != tt.want.status {
				t.Errorf("AliasHandler.HandleGetAliasByRef() = %v, want %v", res.StatusCode, tt.want.status)
			}
		})
	}
}

func TestAliasHandler_HandleUpdateAlias(t *testing.T) {
	tests := AliasDICases("HandleUpdateAlias")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerFunc := tt.ah.HandleUpdateAlias().HTTPHandlerFunc()
			ts := httptest.NewServer(handlerFunc)
			defer ts.Close()

			tt.ah.Memory.Alias().CreateAlias("", "ch", &meta.Alias{Target: "mock_Alias"})

			client := ts.Client()
			res, err := client.Post(ts.URL, "application/json", bytes.NewBuffer(tt.send.body))
			if err != nil {
				t.Log("error making a POST in the httptest server")
				return
			}
			defer res.Body.Close()

			if res.StatusCode != tt.want.status {
				t.Errorf("AliasHandler.HandleUpdateAlias() = %v, want %v", res.StatusCode, tt.want.status)
			}
		})
	}
}

func TestAliasHandler_HandleDeleteAlias(t *testing.T) {
	tests := AliasQueryDICases("HandleDeleteAlias")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerFunc := tt.ah.HandleDeleteAlias()
			ts := httptest.NewServer(handlerFunc)
			defer ts.Close()

			tt.ah.Memory.Alias().CreateAlias("", "ch", &meta.Alias{Target: "mock_Alias"})

			client := ts.Client()
			res, err := client.Post(ts.URL, "application/json", bytes.NewBuffer(tt.send.body))
			if err != nil {
				t.Log("error making a POST in the httptest server")
				return
			}
			defer res.Body.Close()

			if res.StatusCode != tt.want.status {
				t.Errorf("AliasHandler.HandleDeleteAlias() = %v, want %v", res.StatusCode, tt.want.status)
			}
		})
	}
}
