package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"inspr.dev/inspr/cmd/insprd/memory"
	"inspr.dev/inspr/cmd/insprd/memory/fake"
	"inspr.dev/inspr/cmd/insprd/operators"
	ofake "inspr.dev/inspr/cmd/insprd/operators/fake"
	"inspr.dev/inspr/pkg/api/models"
	authmock "inspr.dev/inspr/pkg/auth/mocks"
	"inspr.dev/inspr/pkg/ierrors"
	"inspr.dev/inspr/pkg/meta"
)

type AliasAPITest struct {
	name string
	ah   *AliasHandler
	send sendInRequest
	want expectedResponse
}

// AliasDICases - generates the test cases to be used in functions that
// handle the use the AliasDI struct of the models package.
// For example, HandleCreate and HandleUpdate use these test cases
func AliasDICases(funcName string) []AliasAPITest {
	parsedAliasDI, _ := json.Marshal(models.AliasDI{
		Alias: meta.Alias{
			Resource: "mock_Alias",
		},
	})
	wrongFormatData := []byte{1}
	return []AliasAPITest{
		{
			name: "successful_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(nil, nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusOK},
		},
		{
			name: "unsuccessful_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(errors.New("test_error"), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "bad_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(nil, nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: wrongFormatData},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "not_found_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").NotFound(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusNotFound},
		},
		{
			name: "already_exists_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").AlreadyExists(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusConflict},
		},
		{
			name: "internal_server_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").InternalServer(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "invalid_name_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").InvalidName(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_app_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").InvalidApp(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_channel_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").InvalidChannel(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_type_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").InvalidType(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "bad_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").BadRequest(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasDI},
			want: expectedResponse{status: http.StatusBadRequest},
		},
	}
}

// AliasQueryDICases - generates the test cases to be used in functions
// that handle the use the AliasQueryDI struct of the models package.
// For example, HandleGetAliasByRef and HandleDelete use these test cases
func AliasQueryDICases(funcName string) []AliasAPITest {
	parsedAliasQueryDI, _ := json.Marshal(models.AliasQueryDI{
		Name:   "mock_Alias",
		DryRun: false,
	})
	wrongFormatData := []byte{1}
	return []AliasAPITest{
		{
			name: "successful_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(nil, nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusOK},
		},
		{
			name: "unsuccessful_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(errors.New("test_error"), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "bad_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(nil, nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: wrongFormatData},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "not_found_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").NotFound(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusNotFound},
		},
		{
			name: "already_exists_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").AlreadyExists(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusConflict},
		},
		{
			name: "internal_server_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").InternalServer(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusInternalServerError},
		},
		{
			name: "invalid_name_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").InvalidName(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_app_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").InvalidApp(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_channel_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").InvalidChannel(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "invalid_type_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").InvalidType(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusForbidden},
		},
		{
			name: "bad_request_" + funcName,
			ah:   NewHandler(fake.GetMockMemoryManager(ierrors.New("").BadRequest(), nil), ofake.NewFakeOperator(), authmock.NewMockAuth(nil)).NewAliasHandler(),
			send: sendInRequest{body: parsedAliasQueryDI},
			want: expectedResponse{status: http.StatusBadRequest},
		},
	}
}

func TestNewAliasHandler(t *testing.T) {
	h := NewHandler(
		fake.GetMockMemoryManager(nil, nil),
		ofake.NewFakeOperator(),
		authmock.NewMockAuth(nil),
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
			name: "success_CreateHandler",
			args: args{
				memManager: fake.GetMockMemoryManager(nil, nil),
				op:         ofake.NewFakeOperator(),
			},
			want: &AliasHandler{
				h,
				logger,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := h.NewAliasHandler(); !reflect.DeepEqual(got.Handler, tt.want.Handler) {
				t.Errorf("NewAliasHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAliasHandler_HandleCreate(t *testing.T) {
	tests := AliasDICases("HandleCreate")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerFunc := tt.ah.HandleCreate().HTTPHandlerFunc()
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
				t.Errorf("AliasHandler.HandleCreate() = %v, want %v", res.StatusCode, tt.want.status)
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

			tt.ah.Memory.Tree().Alias().Create("", &meta.Alias{Resource: "mock_Alias"})

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

func TestAliasHandler_HandleUpdate(t *testing.T) {
	tests := AliasDICases("HandleUpdate")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerFunc := tt.ah.HandleUpdate().HTTPHandlerFunc()
			ts := httptest.NewServer(handlerFunc)
			defer ts.Close()

			tt.ah.Memory.Tree().Alias().Create("", &meta.Alias{Resource: "mock_Alias"})

			client := ts.Client()
			res, err := client.Post(ts.URL, "application/json", bytes.NewBuffer(tt.send.body))
			if err != nil {
				t.Log("error making a POST in the httptest server")
				return
			}
			defer res.Body.Close()

			if res.StatusCode != tt.want.status {
				t.Errorf("AliasHandler.HandleUpdate() = %v, want %v", res.StatusCode, tt.want.status)
			}
		})
	}
}

func TestAliasHandler_HandleDelete(t *testing.T) {
	tests := AliasQueryDICases("HandleDelete")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerFunc := tt.ah.HandleDelete()
			ts := httptest.NewServer(handlerFunc)
			defer ts.Close()

			tt.ah.Memory.Tree().Alias().Create("", &meta.Alias{Resource: "mock_Alias"})

			client := ts.Client()
			res, err := client.Post(ts.URL, "application/json", bytes.NewBuffer(tt.send.body))
			if err != nil {
				t.Log("error making a POST in the httptest server")
				return
			}
			defer res.Body.Close()

			if res.StatusCode != tt.want.status {
				t.Errorf("AliasHandler.HandleDelete() = %v, want %v", res.StatusCode, tt.want.status)
			}
		})
	}
}
