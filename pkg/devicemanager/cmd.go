package devicemanager

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	return &cobra.Command{Use: "device-manager", Short: "inspect and allocate local devices"}
}
