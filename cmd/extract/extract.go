package extract

import (
	"archive/zip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

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

		return extract(&zipReader.Reader, zipWriter, _params)
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
	_params *ExtractParams
)

var (
	errInvalidExclude          = errors.New("invalid exclude-fields format, expected filename,fieldnames,...")
	errMutuallyExclusiveFiles  = errors.New("--exclude-files and --include-files cannot be used together")
	errMutuallyExclusiveShapes = errors.New("--exclude-shapes and --exclude-files shapes.txt cannot be used together, use --exclude-shapes instead")
	errShapesExcluded          = errors.New("shapes.txt cannot be excluded. Use --exclude-shapes flag instead")
)

func init() {
	_params = NewExtractParams(ExtractCmd)
}

func extract(zipReader *zip.Reader, zipWriter *zip.Writer, params *ExtractParams) error {
	err := params.Validate()
	if err != nil {
		return err
	}

	include := len(params.IncludedFiles()) > 0 // enforced in PreRunE

	var filter []string
	if include {
		filter = params.IncludedFiles()
	} else {
		filter = params.ExcludedFiles()
	}
	filteredFiles := filterFiles(zipReader.File, filter, include)

	var fieldDecider deciderFunc = func(fileName, fieldName string) bool {
		return decider(fileName, fieldName, params)
	}

	for _, f := range filteredFiles {
		srcFile, err := f.Open()
		if err != nil {
			return err
		}
		writ, err := zipWriter.CreateHeader(&f.FileHeader)
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

func decider(fileName string, fieldName string, params *ExtractParams) bool {
	//var inclusion, exclusion bool
	// Check inclusion first
	if fields, ok := params.IncludedFields()[fileName]; ok {
		return slices.Contains(fields, fieldName)
	}
	// Then check exclusion
	if fields, ok := params.ExcludedFields()[fileName]; ok {
		return !slices.Contains(fields, fieldName)
	}
	// if not specified, include by default
	return true
}

func handleFileFields(fileSrc io.Reader, fileDst io.Writer, fileName string, fieldDecider deciderFunc) error {
	csvReader := csv.NewReader(fileSrc)
	csvWriter := csv.NewWriter(fileDst)
	defer csvWriter.Flush()

	// Fix for malformed CSVs
	csvReader.LazyQuotes = true

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
			return fmt.Errorf("error reading row from file %s: %w", fileName, err)
		}

		newRecord := []string{}
		for _, idx := range includeIndices {
			newRecord = append(newRecord, record[idx])
		}
		if err := csvWriter.Write(newRecord); err != nil {
			return fmt.Errorf("error writing row \"%v\" to file %s: %w", newRecord, fileName, err)
		}
	}
	return nil
}
