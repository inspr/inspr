// package jwtauth is responsible for implementing the auth
// methods specified in the auth folder of the inspr pkg.
package jwtauth

import (
	"reflect"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwt"
	"gitlab.inspr.dev/inspr/core/pkg/auth/models"
)

func TestNewJWTauth(t *testing.T) {
	tests := []struct {
		name string
		want *JWTauth
	}{
		{
			name: "returns_JWT_auth",
			want: &JWTauth{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewJWTauth(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewJWTauth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJWTauth_Validade(t *testing.T) {
	invalidToken := func() []byte {
		token := jwt.New()
		token.Set(jwt.ExpirationKey, time.Now().Add(30*time.Minute))
		signed, _ := jwt.Sign(token, jwa.RS256, "privateKey")
		return signed
	}
	expiredToken := func() []byte {
		token := jwt.New()
		token.Set(jwt.ExpirationKey, time.Now())
		token.Set("payload", models.Payload{
			UID:        "mock_UID",
			Role:       0,
			Scope:      []string{"mock"},
			Refresh:    "mock_refresh",
			RefreshURL: "mock_refresh_url",
		})
		signed, _ := jwt.Sign(token, jwa.RS256, "privateKey")
		return signed
	}
	noPayloadToken := func() []byte {
		token := jwt.New()
		token.Set(jwt.ExpirationKey, time.Now().Add(30*time.Minute))
		token.Set("payload", nil)
		signed, _ := jwt.Sign(token, jwa.RS256, "privateKey")
		return signed
	}
	fineToken := func() []byte {
		token := jwt.New()
		token.Set(jwt.ExpirationKey, time.Now().Add(30*time.Minute))
		token.Set("payload", models.Payload{
			UID:        "mock_UID",
			Role:       0,
			Scope:      []string{"mock"},
			Refresh:    "mock_refresh",
			RefreshURL: "mock_refresh_url",
		})
		signed, _ := jwt.Sign(token, jwa.RS256, "privateKey")
		return signed
	}

	type args struct {
		token []byte
	}
	tests := []struct {
		name    string
		JA      *JWTauth
		args    args
		want    models.Payload
		want1   []byte
		wantErr bool
	}{
		{
			name: "Invalid_token",
			JA:   NewJWTauth(),
			args: args{
				token: invalidToken(),
			},
			want:    models.Payload{},
			want1:   invalidToken(),
			wantErr: true,
		},
		{
			name: "Expired_token",
			JA:   NewJWTauth(),
			args: args{
				token: expiredToken(),
			},
			want:    models.Payload{},
			want1:   expiredToken(),
			wantErr: true,
		},
		{
			name: "Payload_notFound",
			JA:   NewJWTauth(),
			args: args{
				token: noPayloadToken(),
			},
			want:    models.Payload{},
			want1:   noPayloadToken(),
			wantErr: true,
		},
		{
			name: "Worked",
			JA:   NewJWTauth(),
			args: args{
				token: fineToken(),
			},
			want:    models.Payload{},
			want1:   fineToken(),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			JA := &JWTauth{}
			got, got1, err := JA.Validade(tt.args.token)
			if (err != nil) != tt.wantErr {
				t.Errorf("JWTauth.Validade() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JWTauth.Validade() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("JWTauth.Validade() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestJWTauth_Tokenize(t *testing.T) {
	type args struct {
		load models.Payload
	}
	tests := []struct {
		name    string
		JA      *JWTauth
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			JA := &JWTauth{}
			got, err := JA.Tokenize(tt.args.load)
			if (err != nil) != tt.wantErr {
				t.Errorf("JWTauth.Tokenize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JWTauth.Tokenize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJWTauth_Refresh(t *testing.T) {
	type args struct {
		token []byte
	}
	tests := []struct {
		name    string
		JA      *JWTauth
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			JA := &JWTauth{}
			got, err := JA.Refresh(tt.args.token)
			if (err != nil) != tt.wantErr {
				t.Errorf("JWTauth.Refresh() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JWTauth.Refresh() = %v, want %v", got, tt.want)
			}
		})
	}
}
