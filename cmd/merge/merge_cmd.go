package merge

import (
	"archive/zip"
	"os"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/merge/internal/mergeparams"
	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/merge/internal/merger"
	"github.com/InternatManhole/dujpp-gtfs-tool/internal/logging"
	"github.com/samber/lo"
	"github.com/spf13/cobra"
)

var (
	_prefixes []string
	_force    bool

	_inputs []string
	_output string
)

// MergeCmd represents the merge command, which merges multiple source GTFS files
// into a single destination file.
var MergeCmd = &cobra.Command{
	Use:   "merge [flags]... input-gtfs... output-gtfs",
	Short: "Merge multiple GTFS files into one",
	Long:  `TODO: long description`,
	// at least 2 input files and 1 output file
	Args: cobra.MinimumNArgs(3),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		logger := logging.GetLogger()
		_inputs = args[:len(args)-1]
		logger.Info("Input GTFS files: %v", _inputs)
		_output = args[len(args)-1]
		logger.Info("Output GTFS file: %s", _output)
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		logger := logging.GetLogger()

		mergeParams := mergeparams.NewMergeParams(_prefixes, _force)
		merger := merger.NewMerger(mergeParams)
		logger.Verbose("Using prefixes: %v", _prefixes)

		inputZips := lo.Map(_inputs, func(inPath string, index int) *zip.ReadCloser {
			zr, err := zip.OpenReader(inPath)
			if err != nil {
				logger.Error("Failed to open input GTFS zip file %s: %v", inPath, err)
				return nil
			}
			return zr
		})
		defer lo.ForEach(inputZips, func(z *zip.ReadCloser, i int) {
			if z != nil {
				z.Close()
			}
		})

		outputFile, err := os.Create(_output)
		if err != nil {
			logger.Error("Failed to create output GTFS zip file %s: %v", _output, err)
			return nil
		}
		defer outputFile.Close()

		outputZip := zip.NewWriter(outputFile)
		defer outputZip.Close()

		err = merger.Merge(lo.Map(inputZips, func(z *zip.ReadCloser, i int) *zip.Reader {
			if z == nil {
				return nil
			}
			return &z.Reader
		}), outputZip)
		if err != nil {
			logger.Error("Merge failed: %v", err)
			return err
		}

		logger.Info("Merge completed successfully, output written to %s", _output)

		return nil
	},
}

func init() {
	// Initialization code for MergeCmd can be added here in the future.
	fl := MergeCmd.Flags()

	fl.StringSliceVarP(&_prefixes, "prefixes", "p", []string{},
		"List of prefixes to add to each source GTFS file's entries. If provided, the number of prefixes must match the number of input GTFS files or one prefix will be used for all input files. Only the first prefix may be blank (no prefix).")
	fl.BoolVarP(&_force, "force", "f", false,
		"Force merge feeds even if there are conflicting IDs")
}
