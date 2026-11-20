package workergateway

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	return &cobra.Command{Use: "worker-gateway", Short: "query connected workers"}
}
