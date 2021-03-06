package main

import (
	"os"

	"inspr.dev/inspr/cmd/insprd/memory"
	"inspr.dev/inspr/cmd/insprd/operators"
	"inspr.dev/inspr/pkg/api"
	"inspr.dev/inspr/pkg/auth"
	jwtauth "inspr.dev/inspr/pkg/auth/jwt"
	authmock "inspr.dev/inspr/pkg/auth/mocks"
)

func main() {
	var memoryManager memory.Manager
	var operator operators.OperatorInterface
	var authenticator auth.Auth
	var err error

	if _, ok := os.LookupEnv("DEBUG"); ok {
		authenticator = authmock.NewMockAuth(nil)
		memoryManager = memory.GetMemoryManager()
		operator, err = operators.NewOperator(memoryManager, authenticator)
		if err != nil {
			panic(err)
		}
	} else {
		pubKey, err := auth.GetPublicKey()
		if err != nil {
			panic(err)
		}
		authenticator = jwtauth.NewJWTauth(pubKey)
		memoryManager = memory.GetMemoryManager()
		operator, err = operators.NewOperator(memoryManager, authenticator)
		if err != nil {
			panic(err)
		}
	}

	api.Run(memoryManager, operator, authenticator)
}
