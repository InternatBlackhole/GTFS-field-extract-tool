/*
Copyright © 2025 InternatBlackhole
*/
package cmd

import (
	"os"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract"
	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/merge"
	"github.com/InternatManhole/dujpp-gtfs-tool/internal/logging"
	"github.com/spf13/cobra"
)

var logger logging.Logger

// rootCmd is the base command for the GTFS tool, providing a CLI interface for various operations.
// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "gtfs-tool",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
	PreRunE: func(cmd *cobra.Command, args []string) error {
		var logLevel logging.StatusLevel
		if _verboseverbose {
			logLevel = logging.EvenMoreVerbose
		} else if _verbose {
			logLevel = logging.Verbose
		} else {
			logLevel = logging.NoStatus
		}

		logger = logging.NewDefaultLogger(logLevel)
		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

var (
	_verbose        bool
	_verboseverbose bool
)

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.dujpp-gtfs-tool.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	fl := rootCmd.PersistentFlags()
	fl.BoolVarP(&_verbose, "verbose", "v", false, "Enable verbose output")
	fl.BoolVar(&_verboseverbose, "verboseverbose", false, "Enable very verbose output")

	rootCmd.AddCommand(extract.ExtractCmd)
	rootCmd.AddCommand(merge.MergeCmd)

}

func GetLogger() logging.Logger {
	return logger
}
