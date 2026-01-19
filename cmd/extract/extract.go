package extract

import (
	"archive/zip"
	"fmt"
	"maps"
	"os"
	"slices"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/extract"
	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/params"
	"github.com/InternatManhole/dujpp-gtfs-tool/internal/logging"
	"github.com/spf13/cobra"
)

var _params *params.ExtractParams
var _extractor *extract.Extractor

// ExtractCmd represents the extract command, which allows users to extract subsets of GTFS data
// based on various filtering options such as included/excluded files and fields.
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
		uniquly_combine := func(a []string, b []string) []string {
			m := make(map[string]any, len(a)+len(b))
			for _, v := range a {
				m[v] = nil
			}
			for _, v := range b {
				m[v] = nil
			}
			return slices.Collect(maps.Keys(m))
		}
		exclude_files := uniquly_combine(_exclude_files_individual, _exclude_files_sliced)
		include_files := uniquly_combine(_include_files_individual, _include_files_sliced)

		_params = params.NewExtractParams(
			exclude_files,
			include_files,
			_exclude_emptyfiles,
			_exclude_emptyfields,
			_exclude_shapes,
			_exclude_fields,
			_include_fields,
		)
		err := _params.ParseAndValidate()
		if err != nil {
			return err
		}

		verbosity := logging.NoStatus

		if _verboseverbose {
			verbosity = logging.EvenMoreVerbose
		} else if _verbose {
			verbosity = logging.Verbose
		}

		_extractor = extract.NewExtractor(_params, reporter, verbosity)
		if err != nil {
			return err
		}
		return nil
	},

	Args: cobra.ExactArgs(2),
}

var reporter logging.LogConsumer = func(status string, level logging.StatusLevel) {
	fmt.Println(status)
}

var (
	_exclude_files_individual []string
	_exclude_files_sliced     []string
	_include_files_individual []string
	_include_files_sliced     []string
	_exclude_fields           []string
	_include_fields           []string
	_exclude_emptyfiles       bool
	_exclude_emptyfields      bool
	_exclude_shapes           bool
	_verbose                  bool
	_verboseverbose           bool
)

func init() {
	fl := ExtractCmd.Flags()

	fl.StringArrayVar(&_exclude_files_individual, "exclude-file", []string{}, "Individual file to exclude (can be specified multiple times)")
	fl.StringArrayVar(&_include_files_individual, "include-file", []string{}, "Individual file to include (can be specified multiple times)")
	fl.StringSliceVar(&_exclude_files_sliced, "exclude-files", []string{}, "Files to exclude, separated by commas")
	fl.StringSliceVar(&_include_files_sliced, "include-files", []string{}, "Files to include, separated by commas")
	fl.StringArrayVar(&_exclude_fields, "exclude-fields", []string{}, "Fields to exclude (format: filename,fieldnames,...)")
	fl.StringArrayVar(&_include_fields, "include-fields", []string{}, "Fields to include (format: filename,fieldnames,...)")
	fl.BoolVar(&_exclude_emptyfiles, "exclude-empty-files", false, "Exclude empty files")
	fl.BoolVar(&_exclude_emptyfields, "exclude-empty-fields", false, "Exclude empty fields")
	fl.BoolVar(&_exclude_shapes, "exclude-shapes", false, "Exclude shapes")
	// fl.BoolVarP(&_verbose, "verbose", "v", false, "Enable verbose output")
	// fl.BoolVar(&_verboseverbose, "verboseverbose", false, "Enable very verbose output")

	ExtractCmd.MarkFlagsMutuallyExclusive("exclude-file", "include-file")
	ExtractCmd.MarkFlagsMutuallyExclusive("exclude-files", "include-files")
	ExtractCmd.MarkFlagsMutuallyExclusive("exclude-file", "include-files")
	ExtractCmd.MarkFlagsMutuallyExclusive("include-file", "exclude-files")
}
