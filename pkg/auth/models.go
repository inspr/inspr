package auth

// ResfreshDO is the body type expected by UID provider to refresh a payload
type ResfreshDO struct {
	RefreshToken []byte `json:"refreshtoken"`
}

// JwtDO is a data output type for master's token handler
type JwtDO struct {
	Token []byte `json:"token"`
}

// InitDO  structure for initialization requests
type InitDO struct {
	Payload
	Key string
}

//Payload is information caried by a Inspr acceess token
type Payload struct {
	UID string `json:"uid"`
	// Permissions is a map where key is the Scope and values are permissions
	Permissions map[string][]string `json:"permissions"`
	Refresh     []byte              `json:"refresh"`
	RefreshURL  string              `json:"refreshurl"`
}

// ImportPermissionList converts Dapps auth permission scheme to paylod permissions
func (pld *Payload) ImportPermissionList(permissions []string, scope string) {
	pld.Permissions = make(map[string][]string)
	for _, perm := range permissions {
		if perm == CreateToken {
			pld.Permissions[perm] = nil
		} else {
			pld.Permissions[perm] = []string{scope}
		}
	}
}

// All Permissions possible values
const (
	CreateDapp    string = "create:dapp"
	CreateChannel string = "create:channel"
	CreateType    string = "create:type"
	CreateAlias   string = "create:alias"
	CreateBroker  string = "create:broker"

	GetDapp    string = "get:dapp"
	GetChannel string = "get:channel"
	GetType    string = "get:type"
	GetAlias   string = "get:alias"
	GetBroker  string = "get:broker"

	UpdateDapp    string = "update:dapp"
	UpdateChannel string = "update:channel"
	UpdateType    string = "update:type"
	UpdateAlias   string = "update:alias"

	DeleteDapp    string = "delete:dapp"
	DeleteChannel string = "delete:channel"
	DeleteType    string = "delete:type"
	DeleteAlias   string = "delete:alias"

	CreateToken string = "create:token"
)

// AdminPermissions defines all the permissions that the admin user have
// when the cluster is initialized
var AdminPermissions = map[string][]string{

	CreateAlias: {""},
	DeleteAlias: {""},
	GetAlias:    {""},
	UpdateAlias: {""},

	CreateChannel: {""},
	DeleteChannel: {""},
	GetChannel:    {""},
	UpdateChannel: {""},

	CreateDapp: {""},
	DeleteDapp: {""},
	GetDapp:    {""},
	UpdateDapp: {""},

	CreateType: {""},
	DeleteType: {""},
	GetType:    {""},
	UpdateType: {""},

	GetBroker:    nil,
	CreateBroker: nil,

	CreateToken: nil,
}
