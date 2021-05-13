package cli

import (
	"context"
	"io"

	"github.com/inspr/inspr/pkg/cmd"
	cliutils "github.com/inspr/inspr/pkg/cmd/utils"
	"github.com/inspr/inspr/pkg/meta"
	metautils "github.com/inspr/inspr/pkg/meta/utils"
	"github.com/inspr/inspr/pkg/meta/utils/diff"
	utils "github.com/inspr/inspr/pkg/meta/utils/parser"
)

// NewApplyApp receives a controller AppInterface and calls it's methods
// depending on the flags values
func NewApplyApp() RunMethod {
	return func(data []byte, out io.Writer) error {
		c := cliutils.GetCliClient().Apps()
		// unmarshal into an app
		app, err := utils.YamlToApp(data)
		if err != nil {
			return err
		}
		if len(app.Spec.Types) > 0 {
			err = schemaInjection(app.Spec.Types)
			if err != nil {
				return err
			}
		}

		for chName, channel := range app.Spec.Channels {
			channel.Meta.Name = chName
		}

		for aliasName, alias := range app.Spec.Aliases {
			alias.Meta.Name = aliasName
		}

		if len(app.Spec.Apps) > 0 {
			err = recursiveSchemaInjection(app.Spec.Apps)
			if err != nil {
				return err
			}
		}

		flagDryRun := cmd.InsprOptions.DryRun
		flagIsUpdate := cmd.InsprOptions.Update

		var log diff.Changelog
		query, err := metautils.JoinScopes(cmd.InsprOptions.Scope, app.Meta.Parent)
		if err != nil {
			return err
		}
		// creates or updates it
		if flagIsUpdate {
			updateQuery, errQuery := metautils.JoinScopes(query, app.Meta.Name)
			if errQuery != nil {
				return errQuery
			}
			log, err = c.Update(context.Background(), updateQuery, &app, flagDryRun)
		} else {
			log, err = c.Create(context.Background(), query, &app, flagDryRun)
		}

		if err != nil {
			return err
		}

		// prints differences
		log.Print(out)

		return nil
	}
}

func schemaInjection(types map[string]*meta.Type) error {
	var err error
	for typeName, insprType := range types {
		insprType.Meta.Name = typeName
		if schemaNeedsInjection(insprType.Schema) {
			insprType.Schema, err = injectedSchema(insprType.Schema)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func recursiveSchemaInjection(apps map[string]*meta.App) error {
	var err error
	for appName, app := range apps {
		if len(app.Spec.Types) > 0 {
			err = schemaInjection(app.Spec.Types)
			if err != nil {
				return err
			}
		}

		for chName, channel := range app.Spec.Channels {
			channel.Meta.Name = chName
		}

		for aliasName, alias := range app.Spec.Aliases {
			alias.Meta.Name = aliasName
		}

		if len(app.Spec.Apps) > 0 {
			err = recursiveSchemaInjection(app.Spec.Apps)
			if err != nil {
				return err
			}
		}

		app.Meta.Name = appName

	}
	return nil
}
