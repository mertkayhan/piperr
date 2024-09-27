/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "",
	Long:  "",
	Run:   piperrRun,
}

var truncateCmd = &cobra.Command{
	Use:   "truncate",
	Short: "",
	Long:  "",
	Run:   piperrTruncate,
}

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "",
	Long:  "",
}

var testSourceCmd = &cobra.Command{
	Use:   "source",
	Short: "",
	Long:  "",
	Run:   piperrTestSource,
}

var testDestinationCmd = &cobra.Command{
	Use:   "destination",
	Short: "",
	Long:  "",
	Run:   piperrTestDestination,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	viper.SetEnvPrefix("PIPERR")
	viper.AutomaticEnv()

	testCmd.AddCommand(testSourceCmd, testDestinationCmd)
	rootCmd.AddCommand(runCmd, truncateCmd, testCmd)
	// config
	runCmd.Flags().String("config", "", "")
	viper.BindPFlag("config", runCmd.Flags().Lookup("config"))
	// state
	runCmd.Flags().String("state", "", "")
	viper.BindPFlag("state", runCmd.Flags().Lookup("state"))
	// catalog
	runCmd.Flags().String("catalog", "", "")
	viper.BindPFlag("catalog", runCmd.Flags().Lookup("catalog"))

	runCmd.MarkFlagsRequiredTogether("config", "state", "catalog")
}
