package tree

import (
	"gitlab.inspr.dev/inspr/core/cmd/insprd/memory"
	"gitlab.inspr.dev/inspr/core/pkg/meta"
	"gitlab.inspr.dev/inspr/core/pkg/utils"
)

// MemoryManager defines a memory manager interface
type MemoryManager struct {
	root *meta.App
	curr *meta.App
}

var dapptree memory.Manager

// GetTreeMemory returns a memory manager interface
func GetTreeMemory() memory.Manager {
	if dapptree == nil {
		setTree(newTreeMemory())
	}
	return dapptree
}

func newTreeMemory() *MemoryManager {
	return &MemoryManager{
		root: &meta.App{
			Meta: meta.Metadata{
				Annotations: map[string]string{},
			},
			Spec: meta.AppSpec{
				Apps:         map[string]*meta.App{},
				Channels:     map[string]*meta.Channel{},
				ChannelTypes: map[string]*meta.ChannelType{},
			},
		},
	}
}

func setTree(tmm memory.Manager) {
	dapptree = tmm
}

func (mm *MemoryManager) InitTransaction() error {
	var err error
	mm.curr, err = utils.DCopy(mm.root)
	return err
}

func (mm *MemoryManager) Commit() {
	mm.root = mm.curr
	mm.curr = nil
}
