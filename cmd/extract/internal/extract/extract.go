package extract

import (
	"archive/zip"
	"fmt"
	"io"
	"slices"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/extract/file"
	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/params"
	"github.com/InternatManhole/dujpp-gtfs-tool/internal/logging"
)

// Extractor is responsible for processing GTFS data based on the provided parameters.
// It filters files and fields, and writes the resulting data to the output.
type Extractor struct {
	params      *params.ExtractParams
	status      logging.LogConsumer
	reportLevel logging.StatusLevel
}

// NewExtractor creates a new Extractor instance with the given parameters, status consumer, and report level.
func NewExtractor(params *params.ExtractParams, status logging.LogConsumer, reportLevel logging.StatusLevel) *Extractor {
	return &Extractor{
		params: params,
		// TODO: migrate status reporting to log.Logger or similar
		status:      status,
		reportLevel: reportLevel,
	}
}

func (e *Extractor) report(level logging.StatusLevel, format string, a ...any) {
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

// Extract processes the input zip archive, filtering and transforming the data according to the parameters,
// and writes the result to the output zip archive.
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
		statusReporter(logging.Verbose, "No file inclusion or exclusion specified, including all files")
	} else {
		include := len(params.IncludedFiles()) > 0
		if include {
			filter = params.IncludedFiles()
			statusReporter(logging.Verbose, "Including files: %v\n", filter)
		} else {
			filter = params.ExcludedFiles()
			statusReporter(logging.Verbose, "Excluding files: %v\n", filter)
		}
		filteredFiles = filterFiles(zipReader.File, filter, include)
	}

	for _, f := range filteredFiles {
		// In a closure to ensure file is closed after processing (defer)
		err := func() error {
			fileExtractor := file.NewFileExtractor(f.Name, statusReporter, params)
			fileReader, err := f.Open()
			if err != nil {
				return fmt.Errorf("error opening file %s: %w", f.Name, err)
			}
			defer fileReader.Close()
			err = fileExtractor.Run(fileReader, func() (io.Writer, func()) {
				writeFile, err := zipWriter.CreateHeader(&f.FileHeader)
				if err != nil {
					return nil, func() {}
				}
				return writeFile, func() {}
			})
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func filterFiles(srcFiles []*zip.File, filterFiles []string, include bool) []*zip.File {
	return slices.DeleteFunc(srcFiles, func(file *zip.File) bool {
		//TODO: optimize with map (just key presence) or set (something hash based)
		return include != slices.Contains(filterFiles, file.Name)
	})
}
