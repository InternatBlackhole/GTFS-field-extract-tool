package extract

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"io"
	"slices"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/params"
)

type StatusLevel int

const (
	NoStatus StatusLevel = iota
	Verbose
	EvenMoreVerbose
)

type StatusConsumer func(status string, statusLevel StatusLevel)

// TODO: decouple Extractor from params in general
type Extractor struct {
	params      *params.ExtractParams
	status      StatusConsumer
	reportLevel StatusLevel
}

func NewExtractor(params *params.ExtractParams, status StatusConsumer, reportLevel StatusLevel) *Extractor {
	return &Extractor{
		params:      params,
		status:      status,
		reportLevel: reportLevel,
	}
}

func (e *Extractor) report(level StatusLevel, format string, a ...any) {
	if e.status != nil && e.reportLevel >= level {
		p := make([]any, len(a))
		for i, v := range a {
			switch v := v.(type) {
			case func() any:
			case func() []any:
				p[i] = v()
			default:
				p[i] = v
			}
		}
		e.status(fmt.Sprintf(format, p...), level)
	}
}

func (e *Extractor) Extract(zipReader *zip.Reader, zipWriter *zip.Writer) error {
	if !e.params.IsParsedAndValid() {
		return fmt.Errorf("extract parameters are not parsed or valid")
	}

	params := e.params
	statusReporter := e.report

	var filter []string
	var filteredFiles []*zip.File
	if len(params.IncludedFiles()) == 0 && len(params.ExcludedFiles()) == 0 {
		// Edge case for reporting
		filteredFiles = zipReader.File
		statusReporter(Verbose, "No file inclusion or exclusion specified, including all files")
	} else {
		include := len(params.IncludedFiles()) > 0
		if include {
			filter = params.IncludedFiles()
			statusReporter(Verbose, "Including files: %v\n", filter)
		} else {
			filter = params.ExcludedFiles()
			statusReporter(Verbose, "Excluding files: %v\n", filter)
		}
		filteredFiles = filterFiles(zipReader.File, filter, include)
	}

	for _, f := range filteredFiles {
		statusReporter(Verbose, "Processing file: %s", f.Name)
		srcFile, err := f.Open()
		if err != nil {
			return err
		}
		writ, err := zipWriter.CreateHeader(&f.FileHeader)
		if err != nil {
			srcFile.Close()
			return err
		}

		/*err = handleFileFields(srcFile, writ, f.Name, fieldDecider)
		if err != nil {
			srcFile.Close()
			return err
		}*/
		csvReader := csv.NewReader(srcFile)
		csvWriter := csv.NewWriter(writ)
		defer csvWriter.Flush()

		// Fix for malformed CSVs
		csvReader.LazyQuotes = true

		headers, err := csvReader.Read()
		if err != nil {
			return err
		}
		statusReporter(EvenMoreVerbose, "\tOriginal header: %v", headers)

		includeIndices := make([]int, 0, len(headers))
		for i, header := range headers {
			if e.decider(f.Name, header) {
				includeIndices = append(includeIndices, i)
			}
		}

		statusReporter(EvenMoreVerbose, "\tNew header: %v", func() []any {
			included := make([]any, len(includeIndices))
			for i, idx := range includeIndices {
				included[i] = headers[idx]
			}
			return included
		})

		// Write new headers
		newHeaders := make([]string, len(includeIndices))
		for i, idx := range includeIndices {
			newHeaders[i] = headers[idx]
		}
		if err := csvWriter.Write(newHeaders); err != nil {
			return err
		}

		statusReporter(EvenMoreVerbose, "\tProcessing rows for file: %s", f.Name)

		rows := 0
		// Preallocate newRecord slice
		newRecord := make([]string, len(includeIndices))

		// Process rows
		for ; ; rows++ {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("error reading row from file %s: %w", f.Name, err)
			}

			for i, idx := range includeIndices {
				newRecord[i] = record[idx]
			}
			if err := csvWriter.Write(newRecord); err != nil {
				return fmt.Errorf("error writing row \"%v\" to file %s: %w", newRecord, f.Name, err)
			}
		}

		if rows == 0 {
			// TODO: decide if we want to keep empty files
			statusReporter(Verbose, "\tNo rows written for file: %s", f.Name)
		}

		srcFile.Close()
		statusReporter(Verbose, "Finished processing file: %s, rows written: %d", f.Name, rows)
	}
	return nil
}

// This function returns a function that returns the included headers based on the provided indices
// It exists only because if defined inline in a receiver function, the function also is a receiver, which i don't want
// Also you can't define a function type that is a method receiver
// The quirks of Go...
func getIncludedHeadersFunc(headers []string, includeIndices []int) func() []string {
	return func() []string {
		included := make([]string, len(includeIndices))
		for i, idx := range includeIndices {
			included[i] = headers[idx]
		}
		return included
	}
}

// Returns true if the field should be included
//type deciderFunc func(fileName string, fieldName string) bool

func filterFiles(srcFiles []*zip.File, filterFiles []string, include bool) []*zip.File {
	return slices.DeleteFunc(srcFiles, func(file *zip.File) bool {
		//TODO: optimize with map (just key presence) or set (something hash based)
		return include != slices.Contains(filterFiles, file.Name)
	})
}

func (e *Extractor) decider(fileName string, fieldName string) bool {
	// Check inclusion first
	if fields, ok := e.params.IncludedFields()[fileName]; ok {
		return slices.Contains(fields, fieldName)
	}
	// Then check exclusion
	if fields, ok := e.params.ExcludedFields()[fileName]; ok {
		return !slices.Contains(fields, fieldName)
	}
	// if not specified, include by default
	return true
}

/*func handleFileFields(fileSrc io.Reader, fileDst io.Writer, fileName string, fieldDecider deciderFunc) error {
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
}*/
