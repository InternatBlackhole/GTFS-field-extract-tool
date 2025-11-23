package extract

import (
	"errors"
	"iter"
	"slices"
	"strings"

	"github.com/spf13/cobra"
)

type ExtractParams struct {
	excludedFiles []string
	includedFiles []string

	excludedFields map[string][]string
	includedFields map[string][]string

	excludeEmptyFiles  bool
	excludeEmptyFields bool
	excludeShapes      bool

	// format filename,fieldnames
	_excludedFields []string
	// format filename,fieldnames
	_includedFields []string
}

type ExtractFlags int

func NewExtractParams(cmd *cobra.Command) *ExtractParams {
	e := &ExtractParams{}
	fl := cmd.Flags()

	fl.StringArrayVar(&e.excludedFiles, "exclude-files", []string{}, "Files to exclude")
	fl.StringArrayVar(&e.includedFiles, "include-files", []string{}, "Files to include")
	fl.StringArrayVar(&e._excludedFields, "exclude-fields", []string{}, "Fields to exclude (format: filename,fieldnames,...)")
	fl.StringArrayVar(&e._includedFields, "include-fields", []string{}, "Fields to include (format: filename,fieldnames,...)")
	fl.BoolVar(&e.excludeEmptyFiles, "exclude-empty-files", false, "Exclude empty files")
	fl.BoolVar(&e.excludeEmptyFields, "exclude-empty-fields", false, "Exclude empty fields")
	fl.BoolVar(&e.excludeShapes, "exclude-shapes", false, "Exclude shapes")

	cmd.MarkFlagsMutuallyExclusive("exclude-files", "include-files")
	return e
}

func (e *ExtractParams) ExcludedFiles() []string {
	return e.excludedFiles
}

func (e *ExtractParams) IncludedFiles() []string {
	return e.includedFiles
}

func (e *ExtractParams) ExcludedFields() map[string][]string {
	return e.excludedFields
}

func (e *ExtractParams) IncludedFields() map[string][]string {
	return e.includedFields
}

func (e *ExtractParams) ExcludeEmptyFiles() bool {
	return e.excludeEmptyFiles
}

func (e *ExtractParams) ExcludeEmptyFields() bool {
	return e.excludeEmptyFields
}

func (e *ExtractParams) ExcludeShapes() bool {
	return e.excludeShapes
}

func (e *ExtractParams) ParseFieldLists() error {
	var err error
	e.excludedFields, err = parseFieldsFieldList(e._excludedFields)
	if err != nil {
		return err
	}
	e.includedFields, err = parseFieldsFieldList(e._includedFields)
	if err != nil {
		return err
	}

	return nil
}

func (e *ExtractParams) Validate() error {
	if e.excludedFields == nil && len(e._excludedFields) > 0 {
		return errors.New("excluded fields not parsed")
	}
	if e.includedFields == nil && len(e._includedFields) > 0 {
		return errors.New("included fields not parsed")
	}

	included := e.includedFields
	excluded := e.excludedFields

	for fileName := range included {
		// If there are overlapping fields, return an error
		if _, ok := excluded[fileName]; ok {
			// field overlap
			return errors.New("field " + fileName + " cannot be both included and excluded")
		}
	}

	if len(e.includedFiles) > 0 && len(e.excludedFiles) > 0 {
		return errMutuallyExclusiveFiles
	}

	if slices.Contains(e.ExcludedFiles(), "shapes.txt") {
		if e.ExcludeShapes() {
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
