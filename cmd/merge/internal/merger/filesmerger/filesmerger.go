package filesmerger

import (
	"encoding/csv"
	"errors"
	"io"
	"strings"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd"
	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/merge/internal/mergeparams"
	"github.com/samber/lo"
)

var (
	ErrorNoPrefixes = errors.New("no prefixes provided for merging")
)

type FilesMerger struct {
	// Add fields as necessary for merging files
	prefixes []string
	force    bool
}

func NewFilesMerger(prefixes []string, force bool) *FilesMerger {
	return &FilesMerger{
		prefixes: prefixes,
		force:    force,
	}
}

func NewFilesMergerWithParams(params mergeparams.MergeParams) *FilesMerger {
	return &FilesMerger{
		prefixes: params.GetPrefixes(),
		force:    params.IsForce(),
	}
}

func (fm *FilesMerger) MergeFiles(inputFiles []io.Reader, writerCreate func() (io.Writer, func())) error {
	logger := cmd.GetLogger()
	// All the input files refer to the same GTFS file from different archives.
	// Implement merging logic here, considering fm.prefixes and fm.force.
	// Use writerCreate to get the output writer.

	inputCsv := make([]*csv.Reader, len(inputFiles))
	headers := make([][]string, len(inputFiles))
	for i, r := range inputFiles {
		inputCsv[i] = csv.NewReader(r)
		inputCsv[i].LazyQuotes = true // Have to deal with bad GTFS files

		// Read headers
		h, err := inputCsv[i].Read()
		if err != nil {
			return err
		}
		headers[i] = h
		logger.EvenMoreVerbose("File %d has header \"%s\"", i, h)
	}

	// The common headers after merging
	unionHeader := lo.Union(headers...)

	// Mask of unionHeader indicating which columns are ID fields
	idFieldsMask := lo.Map(unionHeader, func(columnName string, index int) bool {
		return strings.HasSuffix(columnName, "_id")
	})

	logger.Verbose("Final file will have unified header: \"%s\"", unionHeader)

	outputWriter, closeFunc := writerCreate()
	defer closeFunc()
	outputCsv := csv.NewWriter(outputWriter)
	defer outputCsv.Flush()

	// Write the union header
	if err := outputCsv.Write(unionHeader); err != nil {
		return err
	}

	// map[columnName][]columnIdValue again map (secondary) is used as a set
	readIds := make(map[string]map[string]any)

	// Read records from each input file, merge (aka fill missing columns), and write to output
	for fileIndex, csvReader := range inputCsv {
		prefix := ""
		if len(fm.prefixes) == len(inputCsv) {
			prefix = fm.prefixes[fileIndex]
		} else if len(fm.prefixes) == 1 {
			prefix = fm.prefixes[0]
		}

		logger.EvenMoreVerbose("Processing file %d with prefix \"%s\"", fileIndex, prefix)

		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			// Create a full record with unionHeader
			fullRecord := make([]string, len(unionHeader))
			for i, colName := range unionHeader {
				// Find the index of colName in the current file's header
				colIndex := lo.IndexOf(headers[fileIndex], colName)
				if colIndex != -1 {
					// Column exists in this file, copy the value
					fullRecord[i] = record[colIndex]
				} else {
					// Column missing in this file, set empty
					fullRecord[i] = ""
				}

				if idFieldsMask[i] {
					// Remember the ID field for conflict checking
					if _, exists := readIds[colName]; !exists {
						readIds[colName] = make(map[string]any)
					}
					// If this value already exists, we have a conflict
					if _, exists := readIds[colName][fullRecord[i]]; exists {
						if fm.force {
							// Ignore conflict, just continue
							continue
						}

						// Conflict detected, need to modify the ID
						if prefix != "" {
							fullRecord[i] = prefix + fullRecord[i]
						} else {
							// fullRecord[i] = "merged_" + fullRecord[i]
							// If no prefix is provided, we fail
							return ErrorNoPrefixes
						}
					} else {
						// No conflict, record the ID
						readIds[colName][fullRecord[i]] = nil
					}
				}

				// logger.EvenMoreVerbose("Writing merged record: \"%s\"", fullRecord)
				if err := outputCsv.Write(fullRecord); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
