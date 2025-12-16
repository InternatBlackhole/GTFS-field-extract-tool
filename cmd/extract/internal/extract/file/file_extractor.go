package file

import (
	"encoding/csv"
	"fmt"
	"io"
	"iter"
	"slices"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal"
	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/params"
	"github.com/InternatManhole/dujpp-gtfs-tool/internal/logging"
)

// Internal type used for file processing
// Intended to have the valid header fields (after filtering) for readability
// Will do the extraction on per-file basis
//
// Stuff like the decider function, that helper function in the main file loop basically should be handled here
type FileExtractor struct {
	//globalExtractorParams *params.ExtractParams // The global extractor params, for options like ExcludeEmptyFiles etc.
	fileName           string
	statusReporter     logging.LogReporter
	excludeEmtpyFiles  bool
	excludeEmptyFields bool

	includedFields []string
	excludedFields []string
}

func NewFileExtractor(
	fileName string,
	statusReporter logging.LogReporter,
	globalExtractorParams *params.ExtractParams) *FileExtractor {
	includedF, ok := globalExtractorParams.IncludedFields()[fileName]
	if !ok {
		includedF = []string{}
	}
	excludedF, ok := globalExtractorParams.ExcludedFields()[fileName]
	if !ok {
		excludedF = []string{}
	}
	return NewFileExtractorAll(
		fileName,
		statusReporter,
		globalExtractorParams.ExcludeEmptyFiles(),
		globalExtractorParams.ExcludeEmptyFields(),
		includedF,
		excludedF,
	)
}

func NewFileExtractorAll(
	fileName string,
	statusReporter logging.LogReporter,
	excludeEmptyFiles bool,
	excludeEmptyFields bool,
	includedFields []string,
	excludedFields []string,
) *FileExtractor {
	return &FileExtractor{
		fileName:           fileName,
		statusReporter:     statusReporter,
		excludeEmtpyFiles:  excludeEmptyFiles,
		excludeEmptyFields: excludeEmptyFields,
		includedFields:     includedFields,
		excludedFields:     excludedFields,
	}
}

func (fe *FileExtractor) Run(fileReader io.Reader, writerCreate func() (io.Writer, func())) error {
	log := fe.statusReporter

	log(logging.Verbose, "Processing file: %s", fe.fileName)

	// --------------------------------------
	// File opening and iterator setup
	// --------------------------------------

	// CSV reader setup
	csvReader := csv.NewReader(fileReader)
	// Fix for malformed CSVs
	csvReader.LazyQuotes = true

	rowIterator, stop := iter.Pull(readerRowsIterator(csvReader))
	defer stop()

	// --------------------------------------
	// Header extraction and determinaton of empty file
	// --------------------------------------

	// It is assumed that the first row is always the header row

	possibleHeader, err := fe.iteratorEntryParser(rowIterator())
	if err != nil {
		return err
	}

	// File has headers, now check if it has data rows

	// Read the first row after headers to determine if the file is also data empty
	possibleDataRow, err := fe.iteratorEntryParser(rowIterator())
	if err != nil {
		return err
	}
	// TODO: possibly combine this if and if above into a single function to reduce code duplication
	if possibleHeader == nil || len(possibleDataRow) == 0 {
		// File has no data rows and/or no header, check if we exclude empty files
		if fe.excludeEmtpyFiles {
			log(logging.Verbose, "\tNo data rows in file: %s, excluding!", fe.fileName)
			return nil
		}
		// Still has no data rows, but not excluding
		log(logging.Verbose, "\tNo data in file: %s, but not excluding", fe.fileName)
		// Create the file in the output zip
		writ, closeFunc := writerCreate()
		defer closeFunc()
		csvWriter := csv.NewWriter(writ)
		defer csvWriter.Flush()
		return nil
	}

	// File has header and at least one data row, proceed to processing

	// --------------------------------------
	// New header mapping from old header
	// --------------------------------------

	log(logging.EvenMoreVerbose, "\tOriginal header: %v", possibleHeader)

	// Determine which columns to include
	includeMask, _ := GenerateFieldMapping(possibleHeader, fe.includedFields, fe.excludedFields)

	// Sanity check
	if len(includeMask) != len(possibleHeader) {
		return fmt.Errorf("generated include mask length %d does not match header length %d for file %s",
			len(includeMask), len(possibleHeader), fe.fileName)
	}

	newHeader := internal.ApplyBoolMaskToSlice(possibleHeader, includeMask)

	log(logging.EvenMoreVerbose, "\tNew header: %v", func() []any {
		return internal.ToAny(newHeader)
	})

	// --------------------------------------
	// Full file processing
	// --------------------------------------

	// Create the file in the output zip
	writ, closeFunc := writerCreate()
	defer closeFunc()
	csvWriter := csv.NewWriter(writ)
	defer csvWriter.Flush()

	// Write new headers
	if !fe.excludeEmptyFields {
		if err := csvWriter.Write(newHeader); err != nil {
			return err
		}
	} else {
		// Defer writing headers if excluding empty fields
		log(logging.EvenMoreVerbose, "\tDeferring header write due to ExcludeEmptyFields option. Embrace for memory usage!")
	}

	log(logging.EvenMoreVerbose, "\tProcessing rows for file: %s", fe.fileName)

	// Prepare for row processing
	/* TODO: maybe look into a io.Writer with CSVWriter and a backing Buffer (with compression?),
	then write to target writer with the use of io.WriterTo= */
	recordsBuffer := make([][]string, 0, 1000) // buffer to hold data rows if excluding empty fields
	fieldHasDataMask := make([]bool, len(newHeader))

	// Preallocate newRecord slice
	newRecord := make([]string, len(newHeader))
	rowsRead := 1 // Since we have already read one data row
	allFieldsHaveData := false

	writeRowFunc := func(record []string) error {
		if err := csvWriter.Write(record); err != nil {
			return fmt.Errorf("error writing row \"%v\" to file %s: %w", record, fe.fileName, err)
		}
		return nil
	}

	// Process rows
	// First process the already read dataRow
	for record := possibleDataRow; record != nil; rowsRead++ {
		// Apply field mapping
		internal.AssignByBoolMask(newRecord, record, includeMask)

		// Handle writing or buffering based on excludeEmptyFields option
		// as soon as all fields have data, we can switch to direct writing
		if fe.excludeEmptyFields && !allFieldsHaveData {
			recordsBuffer = append(recordsBuffer, slices.Clone(newRecord))
			// Online check if all fields have data
			onFlyAllFieldsHaveData := true
			for i, val := range newRecord {
				// needs to be set to true and never reset to false
				fieldHasDataMask[i] = fieldHasDataMask[i] || len(val) != 0
				// is truen only if all fields have data
				onFlyAllFieldsHaveData = onFlyAllFieldsHaveData && len(val) != 0
			}
			if onFlyAllFieldsHaveData {
				allFieldsHaveData = true

				log(logging.EvenMoreVerbose, "\tAll fields have data in file: %s, switching to direct writing", fe.fileName)

				// Write header now
				if err := writeRowFunc(newHeader); err != nil {
					return fmt.Errorf("error writing header to file %s: %w", fe.fileName, err)
				}
				// Write buffered records now
				for _, bufferedRecord := range recordsBuffer {
					if err := writeRowFunc(bufferedRecord); err != nil {
						return err
					}
				}
				// Clear buffer
				recordsBuffer = nil

				log(logging.EvenMoreVerbose, "\tFinished writing buffered records for file: %s", fe.fileName)
			}
		} else {
			// Directly write
			if err := writeRowFunc(newRecord); err != nil {
				return err
			}
		}

		record, err = fe.iteratorEntryParser(rowIterator())
		if err != nil {
			return err
		}
		// if record == nil {
		// 	break
		// }
	}

	if fe.excludeEmptyFields && !allFieldsHaveData {
		// if we are here, it means some fields are empty in the whole file
		// need to filter out the empty fields from header and all buffered records

		log(logging.EvenMoreVerbose, "\tSome fields are empty in file: %s, filtering them out now", fe.fileName)

		// Generate final field mask
		finalFieldMask := fieldHasDataMask

		// Filter header
		finalHeader := internal.ApplyBoolMaskToSlice(newHeader, finalFieldMask)

		// Write final header
		if err := writeRowFunc(finalHeader); err != nil {
			return fmt.Errorf("error writing final header to file %s: %w", fe.fileName, err)
		}

		// Filter and write buffered records
		for _, bufferedRecord := range recordsBuffer {
			finalRecord := internal.ApplyBoolMaskToSlice(bufferedRecord, finalFieldMask)
			if err := writeRowFunc(finalRecord); err != nil {
				return err
			}
		}

		log(logging.EvenMoreVerbose, "\tFinished writing filtered records for file: %s", fe.fileName)
	}

	log(logging.Verbose, "Finished processing file: %s, rows written: %d", fe.fileName, rowsRead)
	return nil
}

type iteratorEntry struct {
	record []string
	err    error
}

// readerRowsIterator returns an iterator over the rows the input CSV file.
func readerRowsIterator(src *csv.Reader) iter.Seq[*iteratorEntry] {
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

// this function checks way too many conditions that shouldn't even be possible when calling this function
// but better safe than sorry I guess
func (fe *FileExtractor) iteratorEntryParser(entry *iteratorEntry, ok bool) ([]string, error) {
	switch {
	case entry == nil && !ok:
		// iterator exhausted
		return nil, nil
	case entry == nil && ok:
		// should not happen
		return nil, fmt.Errorf("unexpected nil entry when reading data from file %s", fe.fileName)
	case entry.err != nil && (entry.err == io.EOF || len(entry.record) == 0):
		// No more data, iterator exhausted
		return nil, nil
	case entry.err != nil:
		// Some other error
		return nil, fmt.Errorf("error reading data from file %s: %w", fe.fileName, entry.err)
	case len(entry.record) > 0:
		// entry.record has data
		return entry.record, nil
	default:
		// should not happen
		return nil, fmt.Errorf("unexpected state when reading data from file %s", fe.fileName)
	}
}

// returns the mask of indices to include from the original header
func GenerateFieldMapping(extractedHeader []string, includedFields, excludedFields []string) ([]bool, int) {
	// decider determines whether a given field in a file should be included based on the Extractor's parameters.
	shouldFieldBeIncluded := func(fieldName string) bool {
		// Check inclusion first, since inclusion takes precedence over exclusion
		if slices.Contains(includedFields, fieldName) {
			return true
		}

		// Then check exclusion
		if slices.Contains(excludedFields, fieldName) {
			return false
		}

		// If not specified,
		if len(includedFields) > 0 {
			// Inclusion list is non-empty, so exclude by default
			return false
		}
		// Otherwise include by default
		return true
	}
	included := 0
	includeMask := make([]bool, len(extractedHeader))
	for i, fieldName := range extractedHeader {
		includeMask[i] = shouldFieldBeIncluded(fieldName)
		if includeMask[i] {
			included++
		}
	}
	return includeMask, included
}
