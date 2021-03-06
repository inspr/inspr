package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"go.uber.org/zap"
	"inspr.dev/inspr/cmd/insprd/memory"
	"inspr.dev/inspr/cmd/insprd/memory/fake"
	"inspr.dev/inspr/cmd/insprd/operators"
	ofake "inspr.dev/inspr/cmd/insprd/operators/fake"
	"inspr.dev/inspr/pkg/api/models"
	"inspr.dev/inspr/pkg/auth"
	authmock "inspr.dev/inspr/pkg/auth/mocks"
	"inspr.dev/inspr/pkg/meta/utils/diff"
	"inspr.dev/inspr/pkg/utils"
)

func TestHandler_NewBrokerHandler(t *testing.T) {
	type fields struct {
		Memory          memory.Manager
		Operator        operators.OperatorInterface
		Auth            auth.Auth
		diffReactions   []diff.DifferenceReaction
		changeReactions []diff.ChangeReaction
	}
	tests := []struct {
		name   string
		fields fields
		want   *BrokerHandler
	}{
		{
			name: "valid new broker handler",
			fields: fields{
				Memory:   fake.GetMockMemoryManager(nil, nil),
				Operator: ofake.NewFakeOperator(),
				Auth:     authmock.NewMockAuth(nil),
			},
			want: &BrokerHandler{
				Handler: &Handler{
					Memory:   fake.GetMockMemoryManager((nil), nil),
					Operator: ofake.NewFakeOperator(),
					Auth:     authmock.NewMockAuth(nil),
				},
				logger: logger.With(zap.String("subSection", "brokers")),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &Handler{
				Memory:          tt.fields.Memory,
				Operator:        tt.fields.Operator,
				Auth:            tt.fields.Auth,
				diffReactions:   tt.fields.diffReactions,
				changeReactions: tt.fields.changeReactions,
			}
			if got := handler.NewBrokerHandler(); !reflect.DeepEqual(got.Handler, tt.want.Handler) {
				t.Errorf("Handler.NewBrokerHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBrokerHandler_HandleGet(t *testing.T) {
	type fields struct {
		Handler *Handler
	}
	tests := []struct {
		name     string
		fields   fields
		want     int
		wantData *models.BrokersDI
	}{
		{
			name: "valid broker get test",
			fields: fields{
				Handler: &Handler{
					Memory: fake.GetMockMemoryManager(nil, nil),
				},
			},
			want: 200,
			wantData: &models.BrokersDI{
				Available: utils.StringArray{"default_mock"},
				Default:   "default_mock",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bh := &BrokerHandler{
				Handler: tt.fields.Handler,
				logger:  logger,
			}
			handlerFunc := bh.HandleGet().HTTPHandlerFunc()
			ts := httptest.NewServer(handlerFunc)
			defer ts.Close()

			client := ts.Client()
			res, err := client.Get(ts.URL)
			if err != nil {
				t.Log("error making a POST in the httptest server")
				return
			}
			defer res.Body.Close()

			if res.StatusCode != tt.want {
				t.Errorf("AppHandler.HandleCreate() = %v, want %v", res.StatusCode, tt.want)
			}
		})
	}
}

func TestBrokerHandler_KafkaHandler(t *testing.T) {
	type fields struct {
		Handler     *Handler
		bodyContent models.BrokerConfigDI
	}
	tests := []struct {
		name      string
		fields    fields
		brokerErr error
		wantCode  int
	}{
		{
			name: "error_reading_body",
			fields: fields{
				Handler: &Handler{
					Memory: fake.GetMockMemoryManager(nil, nil),
				},
			},
			brokerErr: nil,
			wantCode:  http.StatusInternalServerError,
		},
		{
			name: "error_parsing_to_kafka_config",
			fields: fields{
				Handler: &Handler{
					Memory: fake.GetMockMemoryManager(nil, nil),
				},
				bodyContent: models.BrokerConfigDI{
					FileContents: []byte{1}, // throws error at the yaml parser
				},
			},
			brokerErr: nil,
			wantCode:  http.StatusInternalServerError, // yaml pkg error translates to this code
		},
		{
			name: "broker error",
			fields: fields{
				Handler: &Handler{
					Memory: fake.GetMockMemoryManager(nil, errors.New("brokerManager_error")),
				},
			},
			wantCode: http.StatusInternalServerError,
		},
		{
			name: "working",
			fields: fields{
				Handler: &Handler{
					Memory: fake.GetMockMemoryManager(nil, nil),
				},
			},
			wantCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bh := &BrokerHandler{
				Handler: tt.fields.Handler,
				logger:  logger,
			}

			// creating the test server
			handlerFunc := bh.KafkaCreateHandler().HTTPHandlerFunc()
			ts := httptest.NewServer(handlerFunc)
			defer ts.Close()

			// marshalling the body content of the http post
			bodyBytes, err := json.Marshal(tt.fields.bodyContent)
			if err != nil {
				t.Errorf("when passing a test field arg there was an error")
			}

			if tt.name == "error_reading_body" {
				bodyBytes = []byte{1} // throws error when decoding error
			}

			// request
			req, err := http.NewRequest(http.MethodPost,
				ts.URL,
				bytes.NewBuffer(bodyBytes))
			if err != nil {
				t.Errorf("Failed to created request for the test")
			}

			client := ts.Client()
			res, err := client.Do(req)
			if err != nil {
				t.Log("error making a POST in the httptest server")
				return
			}
			defer res.Body.Close()

			if res.StatusCode != tt.wantCode {
				t.Errorf("BrokerHandler.KafkaHandler() = %v, want %v",
					res.StatusCode,
					tt.wantCode)
			}
		})
	}
}
