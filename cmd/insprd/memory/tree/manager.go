package tree

import (
	"gitlab.inspr.dev/inspr/core/cmd/insprd/memory"
	"gitlab.inspr.dev/inspr/core/pkg/meta"
)

type TreeMemoryManager struct {
	root *meta.App
}

var tree memory.Manager

func GetTreeMemory() memory.Manager {
	if tree == nil {
		tree = newTreeMemory()
	}
	return tree
}

func newTreeMemory() *TreeMemoryManager {
	return &TreeMemoryManager{
		root: &meta.App{},
	}
}

func setTree(tmm memory.Manager) {
	tree = tmm
}
