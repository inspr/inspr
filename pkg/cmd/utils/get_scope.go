package utils

import (
	"github.com/inspr/inspr/pkg/cmd"
	"github.com/inspr/inspr/pkg/ierrors"
	"github.com/inspr/inspr/pkg/meta/utils"
)

//GetScope retreives a path for use as base on insprd request.
//Takes into consideration viper config and scope flag.
func GetScope() (string, error) {
	defaultScope := GetConfiguredScope()
	scope := defaultScope

	if cmd.InsprOptions.Scope != "" {
		if utils.IsValidScope(cmd.InsprOptions.Scope) {
			scope = cmd.InsprOptions.Scope
		} else {
			return "", ierrors.
				NewError().
				BadRequest().
				Message("'%v' is an invalid scope", cmd.InsprOptions.Scope).
				Build()
		}
	}

	return scope, nil
}