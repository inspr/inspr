package tree

import (
	"strings"

	"gitlab.inspr.dev/inspr/core/cmd/insprd/memory"
	"gitlab.inspr.dev/inspr/core/pkg/ierrors"
	"gitlab.inspr.dev/inspr/core/pkg/meta"
)

type AppMemoryManager struct {
	root *meta.App
}

func (tmm *TreeMemoryManager) Apps() memory.AppMemory {
	return &AppMemoryManager{
		root: tmm.root,
	}
}

// GetApp recieves a query string (format = 'x.y.z') and iterates through the
// memory tree until it finds the dApp which name is equal to the last query element.
// The root app is returned if the query string is an empty string.
// If the specified dApp is found, it is returned. Otherwise, returns an error.
func (amm *AppMemoryManager) GetApp(query string) (*meta.App, error) {
	reference := strings.Split(query, ".")
	err := ierrors.NewError().NotFound().Message("dApp not found for given query").Build()

	if len(reference) == 1 {
		if reference[0] == "" {
			return amm.root, nil
		} else if amm.root.Spec.Apps[reference[0]] != nil {
			return amm.root.Spec.Apps[reference[0]], nil
		}
	} else {
		nxtApp := amm.root.Spec.Apps[reference[0]]
		if nxtApp != nil {
			for _, element := range reference[1:] {
				nxtApp = nxtApp.Spec.Apps[element]
				if nxtApp == nil {
					return nil, err
				}
			}
			return nxtApp, nil
		}
	}
	return nil, err
}

// CreateApp instantiates a new dApp in the given context.
// If the dApp's information is invalid, returns an error. The same goes for an invalid context.
// In case of context being an empty string, the dApp is created inside the root dApp.
func (amm *AppMemoryManager) CreateApp(app *meta.App, context string) error {
	parentApp, err := amm.GetApp(context)
	if err != nil {
		return err
	}

	if validAppStructure(*app, *parentApp) {
		app.Meta.Parent = parentApp.Meta.Name
		parentApp.Spec.Apps[app.Meta.Name] = app

		newContext := context + app.Meta.Name
		// If new dApp has dApps inside of it, creates them recursively
		if len(app.Spec.Apps) > 0 {
			for _, newApp := range app.Spec.Apps {
				amm.CreateApp(newApp, newContext)
			}
		}
		// If new dApp has Channels inside of it, creates them
		if len(app.Spec.Channels) > 0 {
			for _, newChannel := range app.Spec.Channels {
				errCh := GetTreeMemory().Channels().CreateChannel(newChannel, newContext)
				if errCh != nil {
					return ierrors.NewError().InvalidChannel().Message("Invalid Channel inside dApp structure").Build()
				}
			}
		}
		// If new dApp has ChannelTypes inside of it, creates them
		if len(app.Spec.ChannelTypes) > 0 {
			for _, newChannelType := range app.Spec.ChannelTypes {
				errChTy := GetTreeMemory().ChannelTypes().CreateChannelType(newChannelType, newContext)
				if errChTy != nil {
					return ierrors.NewError().InvalidChannelType().Message("Invalid ChannelType inside dApp structure").Build()
				}
			}
		}

		return nil
	}

	return ierrors.NewError().InvalidApp().Message("Invalid dApp structure").Build()
}

// DeleteApp receives a query and searches for the specified dApp through the tree.
// If the dApp is found and it doesn't have any dApps insite of it, it's deleted.
// If it has other dApps inside of itself, those dApps are deleted recursively.
// Channels and Channel Types inside the dApps to be deleted are also deleted
// dApp's reference inside of it's parent is also deleted.
// In case of dApp not found an error is returned.
func (amm *AppMemoryManager) DeleteApp(query string) error {
	app, err := amm.GetApp(query)
	if err != nil {
		return err
	}

	// Delete dApp's Channels (channel dependencies are validated inside 'DeleteChannel" function)
	if len(app.Spec.Channels) > 0 {
		for _, channel := range app.Spec.Channels {
			GetTreeMemory().Channels().DeleteChannel(query, channel.Meta.Name)
		}
	}
	// Delete dApp's Channel Types
	if len(app.Spec.ChannelTypes) > 0 {
		for _, channeltype := range app.Spec.ChannelTypes {
			GetTreeMemory().Channels().DeleteChannel(query, channeltype.Meta.Name)
		}
	}
	// If this dApps contain another dApps inside of it, deletes them recursively
	if len(app.Spec.Apps) > 0 {
		for _, nxtApp := range app.Spec.Apps {
			newQuery := query + nxtApp.Meta.Name
			GetTreeMemory().Apps().DeleteApp(newQuery)
		}
	}

	parent, errParent := getParentApp(query)
	if errParent != nil {
		return errParent
	}
	deleteApp(app)
	delete(parent.Spec.Apps, app.Meta.Name)

	return nil
}

// UpdateApp receives a pointer to a dApp and the path to where this dApp is inside the memory
// tree. If the current dApp is found and the updated one has valid information, the current is updated.
// Otherwise, returns an error.
func (amm *AppMemoryManager) UpdateApp(app *meta.App, query string) error {

	return nil
}

// Auxiliar unexported functions
func validAppStructure(app, parentApp meta.App) bool {
	var validName, validSubstructure, validBoundary bool

	validName = (app.Meta.Name != "") && (parentApp.Spec.Apps[app.Meta.Name] == nil)
	validSubstructure = nodeIsEmpty(app.Spec.Node) || (len(app.Spec.Apps) == 0)
	boundariesExist := len(app.Spec.Boundary.Input) > 0 || len(app.Spec.Boundary.Output) > 0
	if boundariesExist {
		validBoundary = checkBoundaries(app.Spec.Boundary, parentApp.Spec.Channels)
	}

	return validName && validSubstructure && validBoundary
}

func nodeIsEmpty(node meta.Node) bool {
	noAnnotations := node.Meta.Annotations == nil
	noName := node.Meta.Name == ""
	noParent := node.Meta.Parent == ""
	noImage := node.Spec.Image == ""

	return noAnnotations && noName && noParent && noImage
}

func checkBoundaries(bound meta.AppBoundary, parentChannels map[string]*meta.Channel) bool {
	var parentHasChannels, validInputs, validOutputs bool
	parentHasChannels = len(parentChannels) > 0

	if len(bound.Input) > 0 {
		for _, input := range bound.Input {
			if parentChannels[input] == nil {
				validInputs = false
			}
		}
	} else {
		validInputs = true
	}

	if len(bound.Output) > 0 {
		for _, output := range bound.Output {
			if parentChannels[output] == nil {
				validInputs = false
			}
		}
	} else {
		validOutputs = true
	}

	return parentHasChannels && validInputs && validOutputs
}

func deleteApp(app *meta.App) error {
	app.Meta = meta.Metadata{}
	app.Spec.Node = meta.Node{}
	app.Spec.Apps = nil
	app.Spec.Channels = nil
	app.Spec.Boundary = meta.AppBoundary{}
	return nil
}

func getParentApp(sonQuery string) (*meta.App, error) {
	sonRef := strings.Split(sonQuery, ".")
	parentQuery := strings.Join(sonRef[:len(sonRef)-1], ".")

	parentApp, err := GetTreeMemory().Apps().GetApp(parentQuery)

	return parentApp, err
}
