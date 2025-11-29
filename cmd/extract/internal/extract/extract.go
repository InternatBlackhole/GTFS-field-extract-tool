package extract

import (
	"archive/zip"
	"encoding/csv"
	"fmt"
	"io"
	"slices"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/params"
)

func Extract(zipReader *zip.Reader, zipWriter *zip.Writer, params *params.ExtractParams) error {
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

	// var fieldDecider deciderFunc = func(fileName, fieldName string) bool {
	// 	return decider(fileName, fieldName, params)
	// }

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

		includeIndices := []int{}
		for i, header := range headers {
			if decider(f.Name, header, params) {
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

		rows := 0

		// Process rows
		for ; ; rows++ {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("error reading row from file %s: %w", f.Name, err)
			}

			newRecord := []string{}
			for _, idx := range includeIndices {
				newRecord = append(newRecord, record[idx])
			}
			if err := csvWriter.Write(newRecord); err != nil {
				return fmt.Errorf("error writing row \"%v\" to file %s: %w", newRecord, f.Name, err)
			}
		}

		if rows == 0 {

		}

		srcFile.Close()
	}
	return nil
}

// Returns true if the field should be included
//type deciderFunc func(fileName string, fieldName string) bool

func filterFiles(srcFiles []*zip.File, filterFiles []string, include bool) []*zip.File {
	return slices.DeleteFunc(srcFiles, func(file *zip.File) bool {
		//TODO: optimize with map (just key presence) or set (something hash based)
		return include != slices.Contains(filterFiles, file.Name)
	})
}

func decider(fileName string, fieldName string, params *params.ExtractParams) bool {
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
