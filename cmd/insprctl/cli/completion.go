package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	cliutils "inspr.dev/inspr/pkg/cmd/utils"
	"inspr.dev/inspr/pkg/meta"
	metautils "inspr.dev/inspr/pkg/meta/utils"
	"inspr.dev/inspr/pkg/utils"
)

var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate completion script",
	Long: `To load completions:

Bash:

  $ source <(insprctl completion bash)

  # To load completions for each session, execute once:
  # Linux:
  $ insprctl completion bash > /etc/bash_completion.d/insprctl
  # macOS:
  $ insprctl completion bash > /usr/local/etc/bash_completion.d/insprctl

Zsh:

  # If shell completion is not already enabled in your environment,
  # you will need to enable it.  You can execute the following once:

  $ echo "autoload -U compinit; compinit" >> ~/.zshrc

  # To load completions for each session, execute once:
  $ insprctl completion zsh > "${fpath[1]}/_insprctl"

  # You will need to start a new shell for this setup to take effect.

fish:

  $ insprctl completion fish | source

  # To load completions for each session, execute once:
  $ insprctl completion fish > ~/.config/fish/completions/insprctl.fish

PowerShell:

  PS> insprctl completion powershell | Out-String | Invoke-Expression

  # To load completions for every new session, run:
  PS> insprctl completion powershell > insprctl.ps1
  # and source this file from your PowerShell profile.
`,
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.ExactValidArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			cmd.Root().GenBashCompletion(os.Stdout)
		case "zsh":
			cmd.Root().GenZshCompletion(os.Stdout)
			fmt.Print("compdef _insprctl insprctl")
		case "fish":
			cmd.Root().GenFishCompletion(os.Stdout, true)
		case "powershell":
			cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout)
		}
	},
}

func appsFromApp(app *meta.App) utils.StringArray {
	scopes := utils.StringArray{}
	for name := range app.Spec.Apps {
		scopes = append(scopes, name)
	}
	return scopes
}

func channelsFromApp(app *meta.App) utils.StringArray {
	scopes := utils.StringArray{}
	for name := range app.Spec.Apps {
		scopes = append(scopes, name+".")
	}
	for name := range app.Spec.Channels {
		scopes = append(scopes, name)
	}
	return scopes
}

func typesFromApp(app *meta.App) utils.StringArray {
	scopes := utils.StringArray{}
	for name := range app.Spec.Apps {
		scopes = append(scopes, name+".")
	}
	for name := range app.Spec.Types {
		scopes = append(scopes, name)
	}
	return scopes
}

func aliasesFromApp(app *meta.App) utils.StringArray {
	scopes := utils.StringArray{}
	for name := range app.Spec.Apps {
		scopes = append(scopes, name+".")
	}
	for name := range app.Spec.Aliases {
		scopes = append(scopes, name)
	}
	return scopes
}

func completeDapps(cm *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return generateCompletion(appsFromApp)(cm, args, toComplete)
}

func completeChannels(cm *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return generateCompletion(channelsFromApp)(cm, args, toComplete)
}

func completeTypes(cm *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return generateCompletion(typesFromApp)(cm, args, toComplete)
}

func completeAliases(cm *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return generateCompletion(aliasesFromApp)(cm, args, toComplete)
}

func generateCompletion(tg func(*meta.App) utils.StringArray) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(cm *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		newScope, app, err := getCurrentValidApp(toComplete)
		if err != nil {
			return nil, cobra.ShellCompDirectiveError
		}
		newOnes := tg(app).Map(func(s string) string {
			if newScope != "" {
				return newScope + "." + s
			}
			return s
		}).Filter(
			func(s string) bool {
				return strings.HasPrefix(s, toComplete)
			}).Filter(func(s string) bool {
			return !utils.Includes(args, s)
		})

		return newOnes, cobra.ShellCompDirectiveNoSpace | cobra.ShellCompDirectiveNoFileComp
	}
}

func getCurrentValidApp(toComplete string) (string, *meta.App, error) {
	toComplete = strings.TrimSuffix(toComplete, ".")
	client := cliutils.GetCliClient()
	scope, err := cliutils.GetScope()

	if err != nil {
		return "", nil, errors.New("")
	}

	newScope, err := metautils.JoinScopes(scope, toComplete)
	if err != nil {
		return "", nil, errors.New("")
	}
	if _, err := client.Apps().Get(context.Background(), newScope); err != nil {
		newScope, _, _ = metautils.RemoveLastPartInScope(newScope)
	}

	app, err := client.Apps().Get(context.Background(), newScope)
	if err != nil {
		return "", nil, errors.New("")
	}
	return newScope, app, nil
}
