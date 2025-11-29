package extract

import (
	"archive/zip"
	"fmt"
	"os"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/extract"
	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/params"
	"github.com/spf13/cobra"
)

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

		return _extractor.Extract(&zipReader.Reader, zipWriter)
	},
	PreRunE: func(cmd *cobra.Command, args []string) error {
		err := _params.Parse()
		if err != nil {
			return err
		}

		verbosity := extract.NoStatus

		if _verboseverbose {
			verbosity = extract.EvenMoreVerbose
		} else if _verbose {
			verbosity = extract.Verbose
		}

		_extractor = extract.NewExtractor(_params, reporter, verbosity)
		if err != nil {
			return err
		}
		return nil
	},

	Args: cobra.ExactArgs(2),
}

var (
	_params         *params.ExtractParams
	_verbose        bool
	_verboseverbose bool
	_extractor      *extract.Extractor
)

var reporter extract.StatusConsumer = func(status string, level extract.StatusLevel) {
	fmt.Println(status)
}

func init() {
	// TODO: make params.ExtractParams only be the parser and holder of parsed state
	_params = params.NewExtractParamsWithCobraBindings(ExtractCmd)
	ExtractCmd.Flags().BoolVarP(&_verbose, "verbose", "v", false, "Enable verbose output")
	ExtractCmd.Flags().BoolVar(&_verboseverbose, "verboseverbose", false, "Enable very verbose output")
}
