package api

import (
	"github.com/inspr/inspr/cmd/insprd/memory"
	"github.com/inspr/inspr/cmd/insprd/operators"
	ctrl "github.com/inspr/inspr/pkg/api/controllers"
)

var server ctrl.Server

// Run is the server start up function
func Run(mm memory.Manager, op operators.OperatorInterface) {
	// server.Init(mocks.MockMemoryManager(nil))
	server.Init(mm, op)
	server.Run(":8080")
}