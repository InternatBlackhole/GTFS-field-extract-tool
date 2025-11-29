package extract

import (
	"archive/zip"
	"fmt"
	"os"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/extract"
	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/params"
	"github.com/spf13/cobra"
)

var _params *params.ExtractParams
var _extractor *extract.Extractor

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
		_params = params.NewExtractParams(
			_exclude_files,
			_include_files,
			_exclude_emptyfiles,
			_exclude_emptyfields,
			_exclude_shapes,
			_exclude_fields,
			_include_fields,
		)
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

var reporter extract.StatusConsumer = func(status string, level extract.StatusLevel) {
	fmt.Println(status)
}

var (
	_exclude_files       []string
	_include_files       []string
	_exclude_fields      []string
	_include_fields      []string
	_exclude_emptyfiles  bool
	_exclude_emptyfields bool
	_exclude_shapes      bool
	_verbose             bool
	_verboseverbose      bool
)

func init() {
	fl := ExtractCmd.Flags()

	fl.StringArrayVar(&_exclude_files, "exclude-files", []string{}, "Files to exclude")
	fl.StringArrayVar(&_include_files, "include-files", []string{}, "Files to include")
	fl.StringArrayVar(&_exclude_fields, "exclude-fields", []string{}, "Fields to exclude (format: filename,fieldnames,...)")
	fl.StringArrayVar(&_include_fields, "include-fields", []string{}, "Fields to include (format: filename,fieldnames,...)")
	fl.BoolVar(&_exclude_emptyfiles, "exclude-empty-files", false, "Exclude empty files")
	fl.BoolVar(&_exclude_emptyfields, "exclude-empty-fields", false, "Exclude empty fields")
	fl.BoolVar(&_exclude_shapes, "exclude-shapes", false, "Exclude shapes")
	fl.BoolVarP(&_verbose, "verbose", "v", false, "Enable verbose output")
	fl.BoolVar(&_verboseverbose, "verboseverbose", false, "Enable very verbose output")

	ExtractCmd.MarkFlagsMutuallyExclusive("exclude-files", "include-files")

}
