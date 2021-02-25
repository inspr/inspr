package cli

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"gitlab.inspr.dev/inspr/core/pkg/cmd"
	"gitlab.inspr.dev/inspr/core/pkg/controller"
	"gitlab.inspr.dev/inspr/core/pkg/meta/utils/diff"
	utils "gitlab.inspr.dev/inspr/core/pkg/meta/utils/parser"
	"gopkg.in/yaml.v2"
)

// NewApplyChannelType receives a controller ChannelTypeInterface and calls it's methods
// depending on the flags values
func NewApplyChannelType(c controller.ChannelTypeInterface) RunMethod {
	return func(data []byte, out io.Writer) error {
		// unmarshal into a channelType
		channelType, err := utils.YamlToChannelType(data)
		if err != nil {
			return err
		}

		if schemaNeedsInjection(channelType.Schema) {
			channelType.Schema, err = injectSchema(channelType.Schema)
		}

		flagDryRun := cmd.InsprOptions.DryRun
		flagIsUpdate := cmd.InsprOptions.Update

		var log diff.Changelog
		// creates or updates it
		if flagIsUpdate {
			log, err = c.Update(context.Background(), channelType.Meta.Parent, &channelType, flagDryRun)
		} else {
			log, err = c.Create(context.Background(), channelType.Meta.Parent, &channelType, flagDryRun)
		}

		if err != nil {
			return err
		}

		// prints differences
		log.Print(out)

		return nil
	}
}

func schemaNeedsInjection(schema string) bool {
	_, err := os.Stat(schema)
	if !os.IsNotExist(err) && filepath.Ext(schema) == ".schema" {
		// file exists and has the right extention
		return true
	}
	return false
}

func injectSchema(path string) (string, error) {
	var schema interface{}
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}

	err = yaml.Unmarshal(file, &schema)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%v", schema), nil
}
