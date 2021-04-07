package handler

import (
	"gitlab.inspr.dev/inspr/core/cmd/insprd/auth"
	"gitlab.inspr.dev/inspr/core/cmd/insprd/memory"
	"gitlab.inspr.dev/inspr/core/cmd/insprd/operators"
	"gitlab.inspr.dev/inspr/core/pkg/ierrors"
	"gitlab.inspr.dev/inspr/core/pkg/meta/utils/diff"
)

// Handler is a general handler for inspr routes. It contains the necessary components
// for managing components on each route.
type Handler struct {
	Memory          memory.Manager
	Operator        operators.OperatorInterface
	auth            auth.Auth
	diffReactions   []diff.DifferenceReaction
	changeReactions []diff.ChangeReaction
}

// NewHandler creates a handler from a memory manager and an operator. It also initializes the reactors for
// changes on the cluster.
func NewHandler(memory memory.Manager, operator operators.OperatorInterface) *Handler {
	h := Handler{
		Memory:          memory,
		Operator:        operator,
		diffReactions:   []diff.DifferenceReaction{},
		changeReactions: []diff.ChangeReaction{},
	}
	h.initReactions()
	return &h
}

func (handler *Handler) addDiffReactor(op ...diff.DifferenceReaction) {
	if handler.diffReactions == nil {
		handler.diffReactions = []diff.DifferenceReaction{}
	}
	handler.diffReactions = append(handler.diffReactions, op...)
}

func (handler *Handler) addChangeReactor(op ...diff.ChangeReaction) {
	if handler.changeReactions == nil {
		handler.changeReactions = []diff.ChangeReaction{}
	}
	handler.changeReactions = append(handler.changeReactions, op...)
}

func (handler *Handler) applyChangesInDiff(changes diff.Changelog) error {
	errs := ierrors.MultiError{
		Errors: []error{},
	}
	errs.Add(changes.ForEachDiffFiltered(handler.diffReactions...))
	errs.Add(changes.ForEachFiltered(handler.changeReactions...))
	if errs.Empty() {
		return nil
	}

	return ierrors.NewError().Message(errs.Error()).Build()
}
