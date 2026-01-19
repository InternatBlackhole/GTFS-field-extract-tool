package merge

import "github.com/spf13/cobra"

// MergeCmd represents the merge command, which merges multiple source GTFS files
// into a single destination file.
var MergeCmd = &cobra.Command{
	Use:   "merge [flags]... input-gtfs... output-gtfs",
	Short: "Merge multiple GTFS files into one",
	Long:  `TODO: long description`,
	// at least 2 input files and 1 output file
	Args: cobra.MinimumNArgs(3),
	PreRunE: func(cmd *cobra.Command, args []string) error {

		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {

		return nil
	},
}

var (
	_prefixes []string
	_force    bool
)

func init() {
	// Initialization code for MergeCmd can be added here in the future.
	fl := MergeCmd.Flags()

	fl.StringSliceVarP(&_prefixes, "prefixes", "p", []string{},
		"List of prefixes to add to each source GTFS file's entries. If provided, the number of prefixes must match the number of input GTFS files or one prefix will be used for all input files.")
	fl.BoolVarP(&_force, "force", "f", false,
		"Force merge feeds even if there are conflicting IDs")
}
