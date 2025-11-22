package cmd

import (
	"archive/zip"
	"encoding/csv"
	"errors"
	"io"
	"iter"
	"os"
	"slices"
	"strings"

	"github.com/interline-io/transitland-lib/copier"
	"github.com/spf13/cobra"
)

// extractCmd represents the extract command
var extractCmd = &cobra.Command{
	Use:   "extract [flags]... input-gtfs output-gtfs",
	Short: "Extract a subset of GTFS data, with various filtering options",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE:    runExtract,
	PreRunE: preRunExtract,

	Args: cobra.ExactArgs(2),
}

type extractFilter struct{}

var (
	// Define flags here
	excludedFiles []string
	includedFiles []string

	// format filename,fieldnames
	_excludedFields []string
	// format filename,fieldnames
	_includedFields []string

	excludedFields map[string][]string
	includedFields map[string][]string

	excludeEmptyFiles  bool
	excludeEmptyFields bool
	excludeShapes      bool
	//createDb           bool

	// Copier options that do not modify data (hopefully)
	copyOptions copier.Options = copier.Options{
		CopyExtraFiles: true,
		NoValidators:   true,
		//NoShapeCache: true,
		AllowEntityErrors: true,
		//AllowReferenceErrors: true,
		InterpolateStopTimes: false,
		CreateMissingShapes:  false,
		NormalizeServiceIDs:  false,
		NormalizeTimezones:   false,
		SimplifyCalendars:    false,
		UseBasicRouteTypes:   false,
		//SimplifyShapes: 0.0,
		NormalizeNetworks:          false,
		DeduplicateJourneyPatterns: false,
	}
)

var (
	errInvalidWriter           = errors.New("the specified output writer does not support extra columns")
	errInvalidExclude          = errors.New("invalid exclude-fields format, expected filename,fieldnames,...")
	errMutuallyExclusiveFiles  = errors.New("--exclude-files and --include-files cannot be used together")
	errMutuallyExclusiveShapes = errors.New("--exclude-shapes and --exclude-files shapes.txt cannot be used together, use --exclude-shapes instead")
	errShapesExcluded          = errors.New("shapes.txt cannot be excluded. Use --exclude-shapes flag instead")
)

func init() {
	rootCmd.AddCommand(extractCmd)

	fl := extractCmd.Flags()
	fl.StringArrayVar(&excludedFiles, "exclude-files", []string{}, "Files to exclude")
	fl.StringArrayVar(&includedFiles, "include-files", []string{}, "Files to include")
	fl.StringArrayVar(&_excludedFields, "exclude-fields", []string{}, "Fields to exclude (format: filename,fieldnames,...)")
	fl.StringArrayVar(&_includedFields, "include-fields", []string{}, "Fields to include (format: filename,fieldnames,...)")
	fl.BoolVar(&excludeEmptyFiles, "exclude-empty-files", false, "Exclude empty files")
	fl.BoolVar(&excludeEmptyFields, "exclude-empty-fields", false, "Exclude empty fields")
	fl.BoolVar(&excludeShapes, "exclude-shapes", false, "Exclude shapes")
	//fl.BoolVar(&createDb, "create-db", false, "Create a database schema if output is a database")
}

func preRunExtract(cmd *cobra.Command, args []string) error {
	// Process field inclusion/exclusion
	excludedFieldsMap, err := parseFieldsFieldList(_excludedFields)
	if err != nil {
		return err
	}
	excludedFields = excludedFieldsMap

	includedFieldsMap, err := parseFieldsFieldList(_includedFields)
	if err != nil {
		return err
	}
	includedFields = includedFieldsMap

	for fileName, _ := range includedFieldsMap {
		// If there are overlapping fields, return an error
		if _, ok := excludedFieldsMap[fileName]; ok {
			// field overlap
			return errors.New("field " + fileName + " cannot be both included and excluded")
		}
	}

	if len(excludedFiles) > 0 && len(includedFiles) > 0 {
		return errMutuallyExclusiveFiles
	}

	if slices.Contains(excludedFiles, "shapes.txt") {
		if excludeShapes {
			return errMutuallyExclusiveShapes
		}
		return errShapesExcluded
	}

	return nil
}

func parseFieldsFieldList(fieldList []string) (map[string][]string, error) {
	result := make(map[string][]string)
	for _, ef := range fieldList {
		next, stop := iter.Pull(strings.SplitSeq(ef, ","))

		filename, ok := next()
		if !ok || filename == "" {
			stop()
			return nil, errInvalidExclude
		}

		result[filename] = []string{}
		i := 0
		for ; ; i++ {
			fieldname, ok := next()
			if !ok {
				break
			}
			result[filename] = append(result[filename], fieldname)
		}
		stop()
		if i == 0 {
			return nil, errInvalidExclude
		}
	}
	return result, nil
}

/*func runExtractTL(cmd *cobra.Command, args []string) error {
	in := args[0]
	out := args[1]

	reader, err := ext.OpenReader(in)
	if err != nil {
		return err
	}
	defer reader.Close()

	writer, err := ext.OpenWriter(out , false) //createDb
	if err != nil {
		return err
	}
	defer writer.Close()

	if v, ok := writer.(adapters.WriterWithExtraColumns); ok {
		v.WriteExtraColumns(true)
	} else {
		return errInvalidWriter
	}

	//marker := extract.NewMarker()

	copyOptions.AddExtension(&extractFilter{})

	//marker.SetBbox()

	res, err := copier.CopyWithOptions(context.TODO(), reader, writer, copyOptions)
	if err != nil {
		return err
	}
	res.DisplaySummary()
	return nil
}*/

/*func (f *extractFilter) Filter(ent tt.Entity, entitiyMap tt.EntityMap) error {
	if excludeShapes && ent.Filename() == "shapes.txt" {
		return errors.ErrUnsupported
	}

	if

	return nil
}*/

func runExtract(cmd *cobra.Command, args []string) error {
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

	include := len(includedFiles) > 0 // enforced in PreRunE

	var filter []string
	if include {
		filter = includedFiles
	} else {
		filter = excludedFiles
	}
	filteredFiles := filterFiles(zipReader.File, filter, include)

	var fieldDecider deciderFunc = decider
	/*if include {
		fieldDecider = deciderInclude
	} else {
		fieldDecider = deciderExclude
	}*/

	for _, f := range filteredFiles {
		srcFile, err := f.Open()
		if err != nil {
			return err
		}
		writ, err := zipWriter.CreateHeader(&f.FileHeader)
		//writ, err := zipWriter.Create(f.Name)
		if err != nil {
			srcFile.Close()
			return err
		}

		err = handleFileFields(srcFile, writ, f.Name, fieldDecider)
		if err != nil {
			srcFile.Close()
			return err
		}
		srcFile.Close()
	}
	return nil
}

// Returns true if the field should be included
type deciderFunc func(fileName string, fieldName string) bool

func filterFiles(srcFiles []*zip.File, filterFiles []string, include bool) []*zip.File {
	return slices.DeleteFunc(srcFiles, func(file *zip.File) bool {
		//TODO: optimize with map or set (something hash based)
		return include != slices.Contains(filterFiles, file.Name)
	})
}

/*func deciderInclude(fileName string, fieldName string) bool {
	if fields, ok := includedFields[fileName]; ok {
		return slices.Contains(fields, fieldName)
	}
	return false
}

func deciderExclude(fileName string, fieldName string) bool {
	if fields, ok := excludedFields[fileName]; ok {
		return !slices.Contains(fields, fieldName)
	}
	return true
}*/

func decider(fileName string, fieldName string) bool {
	//var inclusion, exclusion bool
	// Check inclusion first
	if fields, ok := includedFields[fileName]; ok {
		return slices.Contains(fields, fieldName)
	}
	// Then check exclusion
	if fields, ok := excludedFields[fileName]; ok {
		return !slices.Contains(fields, fieldName)
	}
	// if not specified, include by default
	return true
}

func handleFileFields(fileSrc io.Reader, fileDst io.Writer, fileName string, fieldDecider deciderFunc) error {
	csvReader := csv.NewReader(fileSrc)
	csvWriter := csv.NewWriter(fileDst)
	defer csvWriter.Flush()

	headers, err := csvReader.Read()
	if err != nil {
		return err
	}

	includeIndices := []int{}
	for i, header := range headers {
		if fieldDecider(fileName, header) {
			includeIndices = append(includeIndices, i)
		}
	}

	// Write new headers
	newHeaders := []string{}
	for _, idx := range includeIndices {
		newHeaders = append(newHeaders, headers[idx])
	}
	if err := csvWriter.Write(newHeaders); err != nil {
		return err
	}

	// Process rows
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		newRecord := []string{}
		for _, idx := range includeIndices {
			newRecord = append(newRecord, record[idx])
		}
		if err := csvWriter.Write(newRecord); err != nil {
			return err
		}
	}
	return nil
}
