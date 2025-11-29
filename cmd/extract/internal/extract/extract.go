package extract

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"io"
	"iter"
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
				p[i] = v()
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
		err := func() error {
			statusReporter(Verbose, "Processing file: %s", f.Name)
			srcFile, err := f.Open()
			if err != nil {
				return err
			}
			defer srcFile.Close()
			csvReader := csv.NewReader(srcFile)

			// Fix for malformed CSVs
			csvReader.LazyQuotes = true

			nextRow, stop := iter.Pull(iterateCSVRows(csvReader))
			defer stop()

			entry, ok := nextRow()
			rowDecider := func(entry *iteratorEntry, ok bool) ([]string, error) {
				switch {
				case entry == nil && !ok:
					// File is completely empty
					return nil, nil
				case entry == nil && ok:
					return nil, fmt.Errorf("unexpected nil entry when reading headers from file %s", f.Name)
				case entry.err != nil && (entry.err == io.EOF || len(entry.record) == 0):
					return nil, nil
				case entry.err != nil:
					return nil, fmt.Errorf("error reading headers from file %s: %w", f.Name, entry.err)
				case len(entry.record) > 0:
					// First read, entry.record has headers
					return entry.record, nil
				default:
					return nil, fmt.Errorf("unexpected state when reading headers from file %s", f.Name)
				}
			}

			headers, err := rowDecider(entry, ok)
			if err != nil {
				return err
			}

			if headers == nil {
				if e.params.ExcludeEmptyFiles() {
					statusReporter(Verbose, "\tEmpty file: %s, excluding!", f.Name)
					return nil
				}
				statusReporter(Verbose, "\tEmpty file: %s, not excluding", f.Name)
			}

			// Read the first row after headers to determine if the file is empty
			entry, ok = nextRow()
			dataRow, err := rowDecider(entry, ok)
			if err != nil {
				return err
			}
			if len(dataRow) == 0 {
				if e.params.ExcludeEmptyFiles() {
					statusReporter(Verbose, "\tNo data rows in file: %s, excluding!", f.Name)
					return nil
				}
				statusReporter(Verbose, "\tNo data rows in file: %s, but not excluding", f.Name)
			}

			// Create the file in the output zip
			// Done here to ensure empty files are not created if such is desired
			writ, err := zipWriter.CreateHeader(&f.FileHeader)
			if err != nil {
				return err
			}
			csvWriter := csv.NewWriter(writ)
			defer csvWriter.Flush()

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

			if dataRow == nil {
				// No data rows to process
				statusReporter(Verbose, "Finished processing file: %s, rows written: 0", f.Name)
				return nil
			}

			// Preallocate newRecord slice
			newRecord := make([]string, len(includeIndices))
			rowsRead := 1 // Since we have already read one data row

			// First process the already read dataRow
			record := dataRow

			// Process rows
			for ; ; rowsRead++ {
				for i, idx := range includeIndices {
					newRecord[i] = record[idx]
				}
				if err := csvWriter.Write(newRecord); err != nil {
					return fmt.Errorf("error writing row \"%v\" to file %s: %w", newRecord, f.Name, err)
				}

				entry, ok = nextRow()
				record, err = rowDecider(entry, ok)
				if err != nil {
					return err
				}
				if record == nil {
					break
				}
			}

			statusReporter(Verbose, "Finished processing file: %s, rows written: %d", f.Name, rowsRead)
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

type iteratorEntry struct {
	record []string
	err    error
}

func iterateCSVRows(src *csv.Reader) iter.Seq[*iteratorEntry] {
	return func(yield func(*iteratorEntry) bool) {
		for {
			record, err := src.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				yield(&iteratorEntry{err: err})
				break
			}
			cont := yield(&iteratorEntry{record: record})
			if !cont {
				break
			}
		}
	}
}

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
