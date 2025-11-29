package extract

import (
	"archive/zip"
	"fmt"
	"os"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/extract"
	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/params"
	"github.com/spf13/cobra"
)

var extractor *extract.Extractor

// extractCmd represents the extract command
var ExtractCmd = &cobra.Command{
	Use:   "extract [flags]... input-gtfs output-gtfs",
	Short: "Extract a subset of GTFS data, with various filtering options",
	Long:  `TODO: long description`,
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

		return extractor.Extract(&zipReader.Reader, zipWriter)
	},
	PreRunE: func(cmd *cobra.Command, args []string) error {
		err := _params.Parse()
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

var reporter extract.StatusConsumer = func(status string, level extract.StatusLevel) {
	fmt.Println(status)
}

func init() {
	_params = params.NewExtractParamsWithCobraBindings(ExtractCmd)
}
