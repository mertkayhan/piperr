package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

func piperrTestSource(cmd *cobra.Command, args []string) {
	log.Println("test source")
}

func piperrTestDestination(cmd *cobra.Command, args []string) {
	log.Println("test destination")
}
