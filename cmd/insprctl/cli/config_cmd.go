package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	cliutils "inspr.dev/inspr/pkg/cmd/utils"

	"inspr.dev/inspr/pkg/cmd"
	"inspr.dev/inspr/pkg/ierrors"
)

// NewConfigChangeCmd - responsible for changing the values of the insprctl's viper config
func NewConfigChangeCmd() *cobra.Command {
	return cmd.NewCmd("config <key> <value>").
		WithDescription("Change the values stored in the insprctl config").
		WithExample("Changing IP config", "config serverip http://127.0.0.1:8080").
		WithExample("Changing scope config", "config scope app1.app2").
		AddSubCommand(NewListConfig()).
		ExactArgs(2, doConfigChange)
}

// NewListConfig - config subcommand that shows all existent variables in the config
func NewListConfig() *cobra.Command {
	return cmd.NewCmd("list").
		WithDescription("See the list of configuration variables and their current values").
		WithCommonFlags().
		WithExample("type", "config list").
		NoArgs(doListConfig)
}

func doConfigChange(_ context.Context, args []string) error {
	out := cliutils.GetCliOutput()

	key := args[0]
	value := args[1]

	// key doesn't exist
	if !cliutils.ExistsKey(key) {
		errMsg := "error: key inserted does not exist in the insprctl config"
		fmt.Fprintln(out, errMsg)
		printExistingKeys()
		return ierrors.New(errMsg)
	}

	// updates
	if key == cliutils.ServerIpKey() {
		if !strings.HasPrefix(value, "http") {
			value = fmt.Sprintf("http://%s", value)
		}
	}
	if err := cliutils.ChangeViperValues(key, value); err != nil {
		return err
	}

	fmt.Fprintf(out, "Success: insprctl config [%v] changed to '%v'\n", key, value)
	return nil
}

func doListConfig(_ context.Context) error {
	printExistingKeys()
	return nil
}

func printExistingKeys() {
	out := cliutils.GetCliOutput()
	fmt.Fprintln(out, "Available configurations: ")
	for _, key := range cliutils.ExistingKeys() {
		value := viper.GetString(key)
		value = "\"" + value + "\""
		fmt.Fprintf(out, "- %v: %v\n", key, value)
	}
}
