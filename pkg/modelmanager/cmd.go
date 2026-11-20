package modelmanager

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	return &cobra.Command{Use: "model-manager", Short: "serve node-local model files"}
}
