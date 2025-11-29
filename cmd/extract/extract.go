package extract

import (
	"archive/zip"
	"os"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/extract"
	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/params"
	"github.com/spf13/cobra"
)

// extractCmd represents the extract command
var ExtractCmd = &cobra.Command{
	Use:   "extract [flags]... input-gtfs output-gtfs",
	Short: "Extract a subset of GTFS data, with various filtering options",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		in := args[0]
		out := args[1]

		zipReader, err := zip.OpenReader(in)
		if err != nil {
			return err
		}
		defer zipReader.Close()

		writeFile, err := os.Create(out)
		if err != nil {
			return err
		}
		zipWriter := zip.NewWriter(writeFile)
		defer writeFile.Close()
		defer zipWriter.Close()

		return extract.Extract(&zipReader.Reader, zipWriter, _params)
	},
	PreRunE: func(cmd *cobra.Command, args []string) error {
		err := _params.ParseFieldLists()
		if err != nil {
			return err
		}

		err = _params.Validate()
		if err != nil {
			return err
		}
		return nil
	},

	Args: cobra.ExactArgs(2),
}

var (
	_params *params.ExtractParams
)

func init() {
	_params = params.NewExtractParamsWithCobraBindings(ExtractCmd)
}
