package client

import (
	"context"
	"net/http"

	"inspr.dev/inspr/pkg/auth"
	"inspr.dev/inspr/pkg/rest/request"
)

// AuthClient is a client for getting auth information from Insprd
type AuthClient struct {
	reqClient *request.Client
}

// GenerateToken sends a request containing a payload so Insprd
// generates a new auth token based on the payload's info
func (ac *AuthClient) GenerateToken(ctx context.Context, payload auth.Payload) (string, error) {
	authDI := auth.JwtDO{}

	reqClient := ac.reqClient

	err := reqClient.Send(ctx, "/auth", http.MethodPost, payload, &authDI)
	if err != nil {
		return "", err
	}

	return string(authDI.Token), nil
}

// Init function for initializing a cluster
func (ac *AuthClient) Init(ctx context.Context, key string) (string, error) {

	authDO := struct{ Key string }{key}
	authDI := auth.JwtDO{}

	err := ac.reqClient.Send(ctx, "/init", http.MethodPost, authDO, &authDI)
	if err != nil {
		return "", err
	}
	return string(authDI.Token), nil
}
