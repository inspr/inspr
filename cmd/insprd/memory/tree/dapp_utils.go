package tree

import (
	"fmt"

	"go.uber.org/zap"
	apimodels "inspr.dev/inspr/pkg/api/models"
	"inspr.dev/inspr/pkg/ierrors"
	"inspr.dev/inspr/pkg/meta"
	metautils "inspr.dev/inspr/pkg/meta/utils"
	"inspr.dev/inspr/pkg/utils"
)

// SelectBrokerFromPriorityList takes a broker priority list and returns the first
// broker that is available
func SelectBrokerFromPriorityList(brokerList []string, brokers *apimodels.BrokersDI) (string, error) {
	logger.Info("selecting broker from priority list")

	logger.Debug("available brokers", zap.Any("brokers", brokers.Available))
	if len(brokers.Available) == 0 {
		return "", ierrors.New("there are no brokers installed in insprd")
	}

	for _, broker := range brokerList {
		if utils.Includes(brokers.Available, broker) {
			logger.Debug("selected broker: ", zap.String("broker", broker))
			return broker, nil
		}
	}

	logger.Debug("selected the default broker: ", zap.String("broker", brokers.Default))

	return brokers.Default, nil
}

// Auxiliar dApp  functions

// checkApp is used when creating or updating dApps. It verifies if the dApp structure
// is valid, not consideing boundary resolution.
func (amm *AppMemoryManager) checkApp(app, parentApp *meta.App, brokers *apimodels.BrokersDI) error {
	structureErrors := amm.recursiveCheckAndRefineApp(app, parentApp, brokers)
	if structureErrors != nil {
		return structureErrors
	}
	return nil
}

func (amm *AppMemoryManager) recursiveCheckAndRefineApp(app, parentApp *meta.App, brokers *apimodels.BrokersDI) error {
	merr := ierrors.MultiError{
		Errors: []error{},
	}

	parentScope, _ := metautils.JoinScopes(parentApp.Meta.Parent, parentApp.Meta.Name)
	app.Meta.Parent = parentScope
	if !nodeIsEmpty(app.Spec.Node) {
		app.Spec.Node.Meta.Parent = parentScope
	}

	merr.Add(validAppStructure(app, parentApp, brokers))
	for _, childApp := range app.Spec.Apps {
		merr.Add(amm.recursiveCheckAndRefineApp(childApp, app, brokers))
	}

	if !merr.Empty() {
		return &merr
	}

	return nil
}

func validAppStructure(app, parentApp *meta.App, brokers *apimodels.BrokersDI) error {
	merr := ierrors.MultiError{
		Errors: []error{},
	}

	merr.Add(metautils.StructureNameIsValid(app.Meta.Name))

	if !nodeIsEmpty(app.Spec.Node) && !(len(app.Spec.Apps) == 0) {
		merr.Add(ierrors.New("a node can't contain child dApps inside of it"))
	}

	if !nodeIsEmpty(parentApp.Spec.Node) {
		merr.Add(ierrors.New("unable to create dApp for its parent is a Node"))
	}

	merr.Add(checkAndUpdates(app, brokers))
	merr.Add(validAliases(app))

	if !merr.Empty() {
		return &merr
	}

	return nil
}

// addAppInTree is used when creating or updating dApps. Once the structure is verified by
// 'checkApp' method, the new/updated dApp is added to the current tree
func (amm *AppMemoryManager) addAppInTree(app, parentApp *meta.App) {
	if parentApp.Spec.Apps == nil {
		parentApp.Spec.Apps = make(map[string]*meta.App)
	}
	parentStr, _ := metautils.JoinScopes(parentApp.Meta.Parent, parentApp.Meta.Name)
	amm.updateUUID(app, parentStr)
	if app.Spec.Auth.Permissions == nil {
		app.Spec.Auth = parentApp.Spec.Auth
	}
	for _, child := range app.Spec.Apps {
		amm.addAppInTree(child, app)
	}
	parentApp.Spec.Apps[app.Meta.Name] = app
	if !nodeIsEmpty(app.Spec.Node) {
		app.Spec.Node.Meta.Parent = parentStr
		app.Spec.Node.Meta.Name = app.Meta.Name
		app.Spec.Node.Meta.UUID = app.Meta.UUID
		if app.Spec.Node.Meta.Annotations == nil {
			app.Spec.Node.Meta.Annotations = make(map[string]string)
		}
	} else {
		attachRoutes(app)
	}
}

// recursiveBoundaryValidation is used when creating dApps. Once the structure is added to the tree
// by 'addAppInTree', this function verifies if the new dApps boundaries are valid
func (amm *AppMemoryManager) recursiveBoundaryValidation(app *meta.App) error {
	merr := ierrors.MultiError{
		Errors: []error{},
	}
	_, err := amm.ResolveBoundary(app, false)
	if err != nil {
		merr.Add(ierrors.New(err))
		return &merr
	}
	for _, childApp := range app.Spec.Apps {
		err = amm.recursiveBoundaryValidation(childApp)
		if err != nil {
			merr.Add(err)
		}
	}

	if !merr.Empty() {
		return &merr
	}

	return nil
}

// connectAppsBoundaries  is used when creating dApps. Once the boundaries are validated by
// 'recursiveBoundaryValidation', the Channels are updated so that they receive their new
// connected aliases and connected dApps
func (amm *AppMemoryManager) connectAppsBoundaries(app *meta.App) error {
	for _, childApp := range app.Spec.Apps {
		amm.connectAppsBoundaries(childApp)
	}
	return amm.connectAppBoundary(app)
}

func (amm *AppMemoryManager) connectAppBoundary(app *meta.App) error {
	merr := ierrors.MultiError{
		Errors: []error{},
	}
	parentApp, err := amm.Get(app.Meta.Parent)
	if err != nil {
		return err
	}
	for key, val := range parentApp.Spec.Aliases {
		if ch, ok := parentApp.Spec.Channels[val.Target]; ok {
			ch.ConnectedAliases = append(ch.ConnectedAliases, key)
			continue
		}
		if parentApp.Spec.Boundary.Input.Union(parentApp.Spec.Boundary.Output).Contains(val.Target) {
			continue
		}
		merr.Add(ierrors.New(
			"%s's alias %s points to an non-existent channel",
			parentApp.Meta.Name, key,
		))
	}
	if !merr.Empty() {
		return &merr
	}

	appBoundary := utils.StringSliceUnion(app.Spec.Boundary.Input, app.Spec.Boundary.Output)
	for _, boundary := range appBoundary {
		aliasKey, _ := metautils.JoinScopes(app.Meta.Name, boundary)
		if _, ok := parentApp.Spec.Aliases[aliasKey]; ok {
			continue
		}
		if ch, ok := parentApp.Spec.Channels[boundary]; ok {
			ch.ConnectedApps = append(ch.ConnectedApps, app.Meta.Name)
			continue
		}
		if parentApp.Spec.Boundary.Input.Union(parentApp.Spec.Boundary.Output).Contains(boundary) {
			continue
		}
		merr.Add(ierrors.New(
			"%s boundary '%s' is invalid",
			parentApp.Meta.Name, boundary,
		))
	}
	if !merr.Empty() {
		return &merr
	}
	return nil
}

// updateUUID is used by 'addAppInTree' so that new dApps are injected with an UUID, or
// dApps that are being updated remain with their older version UUID
func (amm *AppMemoryManager) updateUUID(app *meta.App, parentStr string) {
	app.Meta.Parent = parentStr
	query, _ := metautils.JoinScopes(parentStr, app.Meta.Name)
	oldApp, err := amm.Perm().Apps().Get(query)
	if err == nil {
		app.Meta.UUID = oldApp.Meta.UUID
		for chName, ch := range app.Spec.Channels {
			if oldApp.Spec.Channels != nil {
				if oldCh, ok := oldApp.Spec.Channels[chName]; ok {
					ch.Meta.UUID = oldCh.Meta.UUID
				} else {
					ch.Meta = metautils.InjectUUID(ch.Meta)
				}
			}
		}
		for ctName, ct := range app.Spec.Types {
			if oldApp.Spec.Types != nil {
				if oldCt, ok := oldApp.Spec.Types[ctName]; ok {
					ct.Meta.UUID = oldCt.Meta.UUID
				} else {
					ct.Meta = metautils.InjectUUID(ct.Meta)
				}
			}
		}
		for alName, al := range app.Spec.Aliases {
			if oldApp.Spec.Aliases != nil {
				if oldAl, ok := oldApp.Spec.Aliases[alName]; ok {
					al.Meta.UUID = oldAl.Meta.UUID
				} else {
					al.Meta = metautils.InjectUUID(al.Meta)
				}
			}
		}
	} else {
		app.Meta = metautils.InjectUUID(app.Meta)
		for _, ch := range app.Spec.Channels {
			ch.Meta = metautils.InjectUUID(ch.Meta)
		}
		for _, ct := range app.Spec.Types {
			ct.Meta = metautils.InjectUUID(ct.Meta)
		}
		for _, al := range app.Spec.Aliases {
			al.Meta = metautils.InjectUUID(al.Meta)
		}
	}
}

func nodeIsEmpty(node meta.Node) bool {
	noAnnotations := node.Meta.Annotations == nil
	noName := node.Meta.Name == ""
	noParent := node.Meta.Parent == ""
	noImage := node.Spec.Image == ""

	return noAnnotations && noName && noParent && noImage
}

func attachRoutes(app *meta.App) {
	nodes := 0
	routes := make(map[string]*meta.RouteConnection)
	for name, child := range app.Spec.Apps {
		if child.Spec.Node.Meta.UUID != "" {
			nodes++
			if child.Spec.Node.Spec.Endpoints.Len() > 0 {
				routes[name] = &meta.RouteConnection{
					Address: child.Spec.Node.Meta.UUID,
				}
				copy(routes[name].Endpoints, child.Spec.Node.Spec.Endpoints)
			}
		}
	}
	if nodes > 1 && len(routes) > 0 {
		app.Spec.Routes = routes
		resolveRoutes(app)
	}
}

func resolveRoutes(app *meta.App) {
	for name, child := range app.Spec.Apps {
		if child.Spec.Node.Meta.UUID != "" {
			for route, data := range app.Spec.Routes {
				if route != name {
					child.Spec.Routes[route] = &meta.RouteConnection{
						Address: data.Address,
					}
					copy(child.Spec.Routes[route].Endpoints, data.Endpoints)
				}
			}
		}
	}
}

func getParentApp(childQuery string, tmm *treeMemoryManager) (*meta.App, error) {
	parentQuery, childName, err := metautils.RemoveLastPartInScope(childQuery)
	if err != nil {
		return nil, err
	}

	parentApp, err := tmm.Apps().Get(parentQuery)
	if err != nil {
		return nil, err
	}
	if _, ok := parentApp.Spec.Apps[childName]; !ok {
		return nil, ierrors.New(
			"dApp %s doesn't exist in dApp %v",
			childName, parentApp.Meta.Name,
		).NotFound()
	}

	return parentApp, err
}

func checkAndUpdates(app *meta.App, brokers *apimodels.BrokersDI) error {
	boundaries := app.Spec.Boundary.Input.Union(app.Spec.Boundary.Output)
	channels := app.Spec.Channels
	types := app.Spec.Types

	for typeName := range types {
		nameErr := metautils.StructureNameIsValid(typeName)
		if nameErr != nil {
			return ierrors.New("invalid type name '%v'", typeName)
		}
	}

	for channelName, channel := range channels {
		nameErr := metautils.StructureNameIsValid(channelName)
		if nameErr != nil {
			return ierrors.New("invalid channel name '%v'", channelName)
		}

		if channel.Spec.Type != "" {
			if _, ok := types[channel.Spec.Type]; !ok {
				return ierrors.New(
					"channel '%v' using unexistent type '%v'",
					channelName, channel.Spec.Type,
				)
			}

			for _, appName := range channel.ConnectedApps {
				if _, ok := app.Spec.Apps[appName]; !ok {
					app.Spec.Channels[channelName].ConnectedApps = utils.Remove(channel.ConnectedApps, appName)
				}

				appInputs := app.Spec.Apps[appName].Spec.Boundary.Input
				appOutputs := app.Spec.Apps[appName].Spec.Boundary.Output
				appBoundary := utils.StringSliceUnion(appInputs, appOutputs)

				if !utils.Includes(appBoundary, channelName) {
					app.Spec.Channels[channelName].ConnectedApps = utils.Remove(channel.ConnectedApps, appName)
				}
			}

			connectedChannels := types[channel.Spec.Type].ConnectedChannels
			if !utils.Includes(connectedChannels, channelName) {
				types[channel.Spec.Type].ConnectedChannels = append(connectedChannels, channelName)
			}

			broker, err := SelectBrokerFromPriorityList(channel.Spec.BrokerPriorityList, brokers)
			if err != nil {
				return err
			}

			channel.Spec.SelectedBroker = broker
		}

		if len(boundaries) > 0 && boundaries.Contains(channelName) {
			return ierrors.New(
				"channel and boundary with same name '%v'",
				channelName,
			)
		}
	}
	return nil
}

func validAliases(app *meta.App) error {
	var msg utils.StringArray

	for key, val := range app.Spec.Aliases {
		if ch, ok := app.Spec.Channels[val.Target]; ok {
			ch.ConnectedAliases = append(ch.ConnectedAliases, key)
			continue
		}
		if app.Spec.Boundary.Input.Union(app.Spec.Boundary.Output).Contains(val.Target) {
			continue
		}
		msg = append(msg, fmt.Sprintf("alias '%s' points to an unexistent channel '%s'", key, val.Target))
	}

	if len(msg) > 0 {
		return ierrors.New(msg.Join(";"))
	}

	return nil
}
