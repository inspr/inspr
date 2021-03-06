package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"golang.org/x/crypto/bcrypt"
	"inspr.dev/inspr/pkg/api/models"
	"inspr.dev/inspr/pkg/auth"
	"inspr.dev/inspr/pkg/meta/utils"
	"inspr.dev/inspr/pkg/rest"
)

var redisServer *miniredis.Miniredis
var redisClient Client
var insprServer *httptest.Server

func TestNewRedisClient(t *testing.T) {
	setup()
	defer teardown()
	tests := []struct {
		name string
		want *Client
	}{
		{
			name: "client_creation",
			want: &Client{
				rdb: &redis.ClusterClient{},
			},
		},
	}
	for _, tt := range tests {
		got := NewRedisClient()

		if reflect.TypeOf(got) != reflect.TypeOf(tt.want) {
			t.Errorf(
				"NewRedisClient() = %v, want %v",
				reflect.TypeOf(got),
				reflect.TypeOf(tt.want),
			)
		}
	}
}

func TestClient_CreateUser(t *testing.T) {
	setup()
	defer teardown()

	auxPass := "none"
	hashedAuxPass, _ := bcrypt.GenerateFromPassword([]byte("none"), bcrypt.DefaultCost)

	auxCtx := context.Background()
	auxUser := User{
		UID:         "user1",
		Permissions: map[string][]string{auth.CreateToken: nil},
		Password:    string(hashedAuxPass),
	}
	auxUser2 := User{
		UID:         "user2",
		Permissions: map[string][]string{auth.UpdateAlias: {"ascope"}},
		Password:    string(hashedAuxPass),
	}

	strData, _ := json.Marshal(auxUser)
	redisClient.rdb.Set(auxCtx, auxUser.UID, strData, 0)
	strData2, _ := json.Marshal(auxUser2)
	redisClient.rdb.Set(auxCtx, auxUser2.UID, strData2, 0)

	type args struct {
		uid     string
		pwd     string
		newUser User
	}
	tests := []struct {
		name    string
		c       *Client
		args    args
		wantErr bool
	}{
		{
			name: "Creates a new user",
			c:    &redisClient,
			args: args{
				uid: auxUser.UID,
				pwd: auxPass,
				newUser: User{
					UID:      "user3",
					Password: "u3pwd",
				},
			},
			wantErr: false,
		},
		{
			name: "Invalid - requestor can't create new users",
			c:    &redisClient,
			args: args{
				uid: auxUser2.UID,
				pwd: auxPass,
				newUser: User{
					UID:      "user3",
					Password: "u3pwd",
					Permissions: map[string][]string{
						auth.CreateAlias: {"ascope.bscope"},
						auth.UpdateAlias: {"ascope.bscope"},
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.c.CreateUser(auxCtx, tt.args.uid, tt.args.pwd, tt.args.newUser)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.CreateUser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			createdUser, err := get(auxCtx, redisClient.rdb, tt.args.newUser.UID)
			if err != nil || reflect.DeepEqual(createdUser, User{}) {
				t.Errorf("Client.CreateUser() error = %v", err)
			}
		})
	}
}

func TestClient_DeleteUser(t *testing.T) {
	setup()
	defer teardown()

	auxPass := "none"
	hashedAuxPass, _ := bcrypt.GenerateFromPassword([]byte("none"), bcrypt.DefaultCost)

	auxCtx := context.Background()
	auxUser := User{
		UID:         "user1",
		Permissions: map[string][]string{auth.CreateToken: nil},
		Password:    string(hashedAuxPass),
	}
	auxUser2 := User{
		UID:         "user2",
		Permissions: map[string][]string{auth.UpdateAlias: {"ascope"}},
		Password:    string(hashedAuxPass),
	}
	auxUser3 := User{
		UID:         "user3",
		Permissions: map[string][]string{auth.UpdateAlias: {"ascope"}},
		Password:    string(hashedAuxPass),
	}

	strData, _ := json.Marshal(auxUser)
	redisClient.rdb.Set(auxCtx, auxUser.UID, strData, 0)
	strData2, _ := json.Marshal(auxUser2)
	redisClient.rdb.Set(auxCtx, auxUser2.UID, strData2, 0)
	strData3, _ := json.Marshal(auxUser3)
	redisClient.rdb.Set(auxCtx, auxUser3.UID, strData3, 0)

	type args struct {
		uid            string
		pwd            string
		usrToBeDeleted string
	}
	tests := []struct {
		name    string
		c       *Client
		args    args
		wantErr bool
	}{
		{
			name: "Deletes an existent user",
			c:    &redisClient,
			args: args{
				uid:            auxUser.UID,
				pwd:            auxPass,
				usrToBeDeleted: "user3",
			},
			wantErr: false,
		},
		{
			name: "Invalid - requestor can't delete users",
			c:    &redisClient,
			args: args{
				uid:            auxUser2.UID,
				pwd:            auxPass,
				usrToBeDeleted: "user3",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.c.DeleteUser(auxCtx, tt.args.uid, tt.args.pwd, tt.args.usrToBeDeleted)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.DeleteUser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			createdUser, err := get(auxCtx, redisClient.rdb, tt.args.usrToBeDeleted)
			if !tt.wantErr && (err == nil || createdUser != nil) {
				t.Errorf("Client.DeleteUser() error = %v", err)
			}
		})
	}
}

func TestClient_UpdatePassword(t *testing.T) {
	setup()
	defer teardown()

	auxPass := "none"
	hashedAuxPass, _ := bcrypt.GenerateFromPassword([]byte(auxPass), bcrypt.DefaultCost)

	auxPass2 := "banana"

	auxCtx := context.Background()
	auxUser := User{
		UID:         "user1",
		Permissions: map[string][]string{auth.CreateToken: nil},
		Password:    string(hashedAuxPass),
	}
	auxUser2 := User{
		UID:         "user2",
		Permissions: map[string][]string{auth.UpdateAlias: {"ascope"}},
		Password:    string(hashedAuxPass),
	}

	strData, _ := json.Marshal(auxUser)
	redisClient.rdb.Set(auxCtx, auxUser.UID, strData, 0)
	strData2, _ := json.Marshal(auxUser2)
	redisClient.rdb.Set(auxCtx, auxUser2.UID, strData2, 0)

	type args struct {
		uid            string
		pwd            string
		usrToBeUpdated string
		newPwd         string
	}
	tests := []struct {
		name    string
		c       *Client
		args    args
		wantErr bool
	}{
		{
			name: "Updated an user password",
			c:    &redisClient,
			args: args{
				uid:            auxUser.UID,
				pwd:            auxPass,
				usrToBeUpdated: auxUser2.UID,
				newPwd:         auxPass2,
			},
			wantErr: false,
		},
		{
			name: "Invalid - requestor can't update users",
			c:    &redisClient,
			args: args{
				uid:            auxUser2.UID,
				pwd:            auxPass,
				usrToBeUpdated: auxUser2.UID,
				newPwd:         auxPass2,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.c.UpdatePassword(auxCtx, tt.args.uid, tt.args.pwd, tt.args.usrToBeUpdated, tt.args.newPwd)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.UpdatePassword() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			updatedUser, err := get(auxCtx, redisClient.rdb, tt.args.usrToBeUpdated)

			passErr := bcrypt.CompareHashAndPassword([]byte(updatedUser.Password), []byte(tt.args.newPwd))

			if !tt.wantErr && (err != nil || passErr != nil) {
				t.Errorf("Client.UpdatePassword() error = %v", err)
			}
		})
	}
}

func TestClient_Login(t *testing.T) {
	setup()
	defer teardown()

	auxPass := "none"
	hashedAuxPass, _ := bcrypt.GenerateFromPassword([]byte(auxPass), bcrypt.DefaultCost)

	auxCtx := context.Background()
	auxUser := User{
		UID:         "user1",
		Permissions: map[string][]string{"ascope": {auth.CreateToken}},
		Password:    string(hashedAuxPass),
	}

	strData, _ := json.Marshal(auxUser)
	redisClient.rdb.Set(auxCtx, auxUser.UID, strData, 0)

	type args struct {
		uid string
		pwd string
	}
	tests := []struct {
		name    string
		c       *Client
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Login with a valid user",
			c:    &redisClient,
			args: args{
				uid: auxUser.UID,
				pwd: auxPass,
			},
			want:    "user1-ascope",
			wantErr: false,
		},
		{
			name: "Invalid - non existent user",
			c:    &redisClient,
			args: args{
				uid: "RANDOMUSER",
				pwd: "RANDOMPWD",
			},
			wantErr: true,
		},
		{
			name: "Invalid - invalid password",
			c:    &redisClient,
			args: args{
				uid: auxUser.UID,
				pwd: "RANDOMPWD",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.Login(auxCtx, tt.args.uid, tt.args.pwd)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Login() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Client.Login() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_RefreshToken(t *testing.T) {
	setup()
	defer teardown()

	auxCtx := context.Background()
	auxUser := User{
		UID:         "user1",
		Permissions: map[string][]string{"": {auth.CreateToken}},
		Password:    "none",
	}
	auxUser2 := User{
		UID:         "user2",
		Permissions: map[string][]string{"": {auth.CreateToken}},
		Password:    "none",
	}

	strData, _ := json.Marshal(auxUser)
	redisClient.rdb.Set(auxCtx, auxUser.UID, strData, 0)

	payload, _ := redisClient.encrypt(auxUser)
	payload2, _ := redisClient.encrypt(auxUser2)

	type args struct {
		refreshToken []byte
	}
	tests := []struct {
		name    string
		c       *Client
		args    args
		wantErr bool
	}{
		{
			name: "Refreshing a valid token",
			c:    &redisClient,
			args: args{
				payload.Refresh,
			},
			wantErr: false,
		},
		{
			name: "Invalid - invalid token",
			c:    &redisClient,
			args: args{
				[]byte("invalid"),
			},
			wantErr: true,
		},
		{
			name: "Invalid - user was deleted",
			c:    &redisClient,
			args: args{
				payload2.Refresh,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.RefreshToken(auxCtx, tt.args.refreshToken)
			if (err != nil) != tt.wantErr {
				t.Errorf("Client.RefreshToken() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && got == nil {
				t.Errorf("Client.RefreshToken() return is empty")
			}
		})
	}
}

func Test_set(t *testing.T) {
	setup()
	defer teardown()

	auxCtx := context.Background()
	auxUser := User{
		UID:         "user1",
		Permissions: map[string][]string{"": {auth.CreateToken}},
		Password:    "none",
	}

	type args struct {
		data User
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Create new user",
			args: args{
				data: auxUser,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := set(auxCtx, redisClient.rdb, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("set() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			createdUser, err := get(auxCtx, redisClient.rdb, auxUser.UID)
			if err != nil || reflect.DeepEqual(createdUser, User{}) {
				t.Errorf("user wasn't set. Error %v", err)
			}
		})
	}
}

func Test_get(t *testing.T) {
	setup()
	defer teardown()

	auxCtx := context.Background()
	auxUser := User{
		UID:         "user1",
		Permissions: map[string][]string{"": {auth.CreateToken}},
		Password:    "none",
	}

	strData, _ := json.Marshal(auxUser)
	redisClient.rdb.Set(auxCtx, auxUser.UID, strData, 0)

	type args struct {
		key string
	}
	tests := []struct {
		name    string
		args    args
		want    *User
		wantErr bool
	}{
		{
			name: "Get user given UID",
			args: args{
				key: "user1",
			},
			want: &User{
				UID:         "user1",
				Permissions: map[string][]string{"": {auth.CreateToken}},
				Password:    "none",
			},
			wantErr: false,
		},
		{
			name: "Invalid - get user given non-existent UID",
			args: args{
				key: "RANDOMKEY",
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := get(auxCtx, redisClient.rdb, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("get() error = %v, wantErr %v", err, tt.wantErr)
				fmt.Println(got)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_delete(t *testing.T) {
	setup()
	defer teardown()

	auxCtx := context.Background()
	auxUser := User{
		UID:         "user1",
		Permissions: map[string][]string{"": {auth.CreateToken}},
		Password:    "none",
	}

	strData, _ := json.Marshal(auxUser)
	redisClient.rdb.Set(auxCtx, auxUser.UID, strData, 0)

	type args struct {
		key string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Delete user given UID",
			args: args{
				key: "user1",
			},
			wantErr: false,
		},
		{
			name: "Invalid - Delete non-existent user",
			args: args{
				key: "RANDOMUSER",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			err := delete(auxCtx, redisClient.rdb, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			deletedUser, err := get(auxCtx, redisClient.rdb, tt.args.key)
			if err == nil {
				t.Errorf("user %v wasn't deleted", deletedUser)
			}
		})
	}
}

func Test_hasPermission(t *testing.T) {
	setup()
	defer teardown()

	auxPass := "none"
	hashedAuxPass, _ := bcrypt.GenerateFromPassword([]byte(auxPass), bcrypt.DefaultCost)

	auxCtx := context.Background()
	auxUser := User{
		UID: "user1",
		Permissions: map[string][]string{
			auth.CreateToken:   nil,
			auth.CreateChannel: {""},
		},
		Password: string(hashedAuxPass),
	}
	auxUser2 := User{
		UID:         "user2",
		Permissions: nil,
		Password:    string(hashedAuxPass),
	}
	auxUser3 := User{
		UID: "user3",
		Permissions: map[string][]string{
			auth.CreateToken:   nil,
			auth.CreateChannel: {"a"},
		},
		Password: string(hashedAuxPass),
	}

	strData, _ := json.Marshal(auxUser)
	redisClient.rdb.Set(auxCtx, auxUser.UID, strData, 0)
	strData2, _ := json.Marshal(auxUser2)
	redisClient.rdb.Set(auxCtx, auxUser2.UID, strData2, 0)
	jsonUser3, _ := json.Marshal(auxUser3)
	redisClient.rdb.Set(auxCtx, auxUser3.UID, jsonUser3, 0)

	type args struct {
		uid string
		pwd string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "User has admin permission",
			args: args{
				uid: "user1",
				pwd: auxPass,
			},
			wantErr: false,
			want:    true,
		},
		{
			name: "Non existent user",
			args: args{
				uid: "RANDOMUSER",
				pwd: "",
			},
			wantErr: true,
		},
		{
			name: "Wrong user credentials",
			args: args{
				uid: "user1",
				pwd: "invalid",
			},
			wantErr: true,
		},
		{
			name: "User is not admin",
			args: args{
				uid: "user2",
				pwd: auxPass,
			},
			wantErr: true,
		},
		{
			name: "User doent have enought permission",
			args: args{
				uid: "user3",
				pwd: auxPass,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := hasPermission(auxCtx, redisClient.rdb, tt.args.uid, tt.args.pwd, auxUser, true)

			if (err != nil) != tt.wantErr {
				t.Errorf("delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_encrypt(t *testing.T) {
	setup()
	defer teardown()

	type args struct {
		user User
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Returns payload with encrypted refresh token",
			args: args{
				user: User{
					UID:         "user1",
					Permissions: map[string][]string{"": {auth.CreateToken}},
					Password:    "none",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := redisClient.encrypt(tt.args.user)
			if err != nil {
				t.Errorf("encrypt() return an error: %v", err)
				return
			}

			if string(got.Refresh) == "" {
				t.Errorf("error while creating the refresh token")
				return
			}
		})
	}
}

func Test_decrypt(t *testing.T) {
	setup()
	defer teardown()

	auxUser := User{
		UID:      "user1",
		Password: "strongpwd",
	}

	payload, _ := redisClient.encrypt(auxUser)

	tests := []struct {
		name string
		want *User
	}{
		{
			name: "Decrypts valid user refresh token",
			want: &User{
				UID:      "user1",
				Password: "strongpwd",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := redisClient.decrypt(payload.Refresh)
			if err != nil {
				t.Errorf("unable to decrypt, error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decrypt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_requestNewToken(t *testing.T) {
	setup()
	defer teardown()

	// checks whether the two strings are the same or not, handles different
	// orders
	check := func(got, want string) bool {
		gotsplit := strings.Split(got, "-")
		gotset, _ := utils.MakeStrSet(gotsplit)

		wantsplit := strings.Split(want, "-")
		wantset, _ := utils.MakeStrSet(wantsplit)

		disjunct := utils.DisjunctSet(gotset, wantset)

		// if their length stays the same nothing new was added
		return len(disjunct) == 0
	}

	type args struct {
		ctx     context.Context
		payload auth.Payload
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Sends request for new token",
			args: args{
				ctx: context.Background(),
				payload: auth.Payload{
					UID:         "user1",
					Permissions: map[string][]string{"app1": {}, "app2": {}},
				},
			},
			want:    "user1-app1-app2",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := redisClient.requestNewToken(tt.args.ctx, tt.args.payload)
			if (err != nil) != tt.wantErr {
				t.Errorf("requestNewToken() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !check(got, tt.want) {
				t.Errorf("requestNewToken() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Auxiliar methods

func setup() {
	redisServer = mockRedis()
	insprServer = httptest.NewServer(http.HandlerFunc(insprServerHandler))

	os.Setenv("INSPR_CLUSTER_ADDR", insprServer.URL)
	os.Setenv("REFRESH_URL", "randomurl")
	os.Setenv("REFRESH_KEY", "61626364616263646162636461626364")
	os.Setenv("REDIS_HOST", redisServer.Host())
	os.Setenv("REDIS_PORT", redisServer.Port())
	os.Setenv("REDIS_PASSWORD", "")

	redisClient = *NewRedisClient()
}

func teardown() {
	os.Unsetenv("REFRESH_KEY")
	os.Unsetenv("REFRESH_URL")
	os.Unsetenv("REDIS_HOST")
	os.Unsetenv("REDIS_PORT")
	os.Unsetenv("REDIS_PASSWORD")
	os.Unsetenv("INSPR_CLUSTER_ADDR")
	redisServer.Close()
	insprServer.Close()
}

func mockRedis() *miniredis.Miniredis {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}

	return s
}

func insprServerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		rest.ERROR(w, fmt.Errorf("method should be POST"))
	}
	data := auth.Payload{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		rest.ERROR(w, err)
		return
	}

	var scopes []string
	for k := range data.Permissions {
		scopes = append(scopes, k)
	}
	strScope := strings.Join(scopes, "-")
	token := fmt.Sprintf("%s-%s", data.UID, strScope)
	val := models.AuthDI{
		Token: []byte(token),
	}
	rest.JSON(w, 200, val)
}

func Test_isScopeAllowed(t *testing.T) {
	type args struct {
		newUserPermScope    string
		requestorPermScopes []string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "root scope allowed",
			args: args{
				newUserPermScope:    "",
				requestorPermScopes: []string{"a.b", ""},
			},
			want: true,
		},
		{
			name: "root scope allower",
			args: args{
				newUserPermScope:    "a.b.c.d.etc",
				requestorPermScopes: []string{""},
			},
			want: true,
		},
		{
			name: "scope is allowed",
			args: args{
				newUserPermScope:    "a.b.c",
				requestorPermScopes: []string{"a.b"},
			},
			want: true,
		},
		{
			name: "scope is not allowed",
			args: args{
				newUserPermScope:    "a.b.c",
				requestorPermScopes: []string{"a.b.c.d"},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isScopeAllowed(tt.args.newUserPermScope, tt.args.requestorPermScopes); got != tt.want {
				t.Errorf("isScopeAllowed() = %v, want %v", got, tt.want)
			}
		})
	}
}
