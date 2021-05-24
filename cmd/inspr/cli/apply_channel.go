package cli

import (
	"context"
	"io"

	"github.com/inspr/inspr/pkg/cmd"
	cliutils "github.com/inspr/inspr/pkg/cmd/utils"
	metautils "github.com/inspr/inspr/pkg/meta/utils"
	"github.com/inspr/inspr/pkg/meta/utils/diff"
)

// NewApplyChannel receives a controller ChannelInterface and calls it's methods
// depending on the flags values
func NewApplyChannel() RunMethod {
	return func(data []byte, out io.Writer) error {
		c := cliutils.GetCliClient().Channels()
		// unmarshal into a channel
		channel, err := metautils.YamlToChannel(data)
		if err != nil {
			return err
		}

		flagDryRun := cmd.InsprOptions.DryRun
		flagIsUpdate := cmd.InsprOptions.Update

		var log diff.Changelog

		scope, err := cliutils.GetScope()
		if err != nil {
			return err
		}

		parentPath, err := metautils.JoinScopes(scope, channel.Meta.Parent)
		if err != nil {
			return err
		}

		// creates or updates it
		if flagIsUpdate {
			log, err = c.Update(context.Background(), parentPath, &channel, flagDryRun)
		} else {
			log, err = c.Create(context.Background(), parentPath, &channel, flagDryRun)
		}

		if err != nil {
			return err
		}

		// prints differences
		log.Print(out)

		return nil
	}
}
