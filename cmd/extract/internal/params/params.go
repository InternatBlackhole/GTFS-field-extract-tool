package params

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/spf13/cobra"
)

var (
	ErrInvalidExclude          = errors.New("invalid exclude/include fields format; must be filename,field1,field2,...")
	ErrMutuallyExclusiveFiles  = errors.New("include-files and exclude-files flags are mutually exclusive")
	ErrMutuallyExclusiveShapes = errors.New("exclude-shapes flag cannot be used with exclude-files including shapes.txt")
	ErrShapesExcluded          = errors.New("shapes.txt is excluded")
	ErrExcludedFieldsNotParsed = errors.New("excluded fields not parsed")
	ErrIncludedFieldsNotParsed = errors.New("included fields not parsed")
	ErrFieldOverlap            = errors.New("a field cannot be both included and excluded")
	ErrNotValidated            = errors.New("parameters not validated")
)

type ExtractParams struct {
	excludedFiles []string
	includedFiles []string

	// set after parsing
	excludedFields map[string][]string
	// set after parsing
	includedFields map[string][]string

	excludeEmptyFiles  bool
	excludeEmptyFields bool
	excludeShapes      bool

	// format filename,fieldnames
	_excludedFields []string
	// format filename,fieldnames
	_includedFields []string

	validated bool
}

// TODO: remove?
func NewExtractParamsWithCobraBindings(cmd *cobra.Command) *ExtractParams {
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

func NewExtractParams(
	excludedFiles, includedFiles []string,
	excludeEmptyFiles, excludeEmptyFields, excludeShapes bool,
	excludedFields, includedFields []string,
) *ExtractParams {
	return &ExtractParams{
		excludedFiles:      excludedFiles,
		includedFiles:      includedFiles,
		excludeEmptyFiles:  excludeEmptyFiles,
		excludeEmptyFields: excludeEmptyFields,
		excludeShapes:      excludeShapes,
		_excludedFields:    excludedFields,
		_includedFields:    includedFields,
	}
}

func NewExtractParamsWithMaps(
	excludedFiles, includedFiles []string,
	excludeEmptyFiles, excludeEmptyFields, excludeShapes bool,
	excludedFields, includedFields map[string][]string,
) *ExtractParams {
	return &ExtractParams{
		excludedFiles:      excludedFiles,
		includedFiles:      includedFiles,
		excludeEmptyFiles:  excludeEmptyFiles,
		excludeEmptyFields: excludeEmptyFields,
		excludeShapes:      excludeShapes,
		excludedFields:     excludedFields,
		includedFields:     includedFields,
		validated:          true,
	}
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
	if !e.validated {
		return ErrNotValidated
	}
	if e.excludedFields == nil && len(e._excludedFields) > 0 {
		return ErrExcludedFieldsNotParsed
	}
	if e.includedFields == nil && len(e._includedFields) > 0 {
		return ErrIncludedFieldsNotParsed
	}

	included := e.includedFields
	excluded := e.excludedFields

	for fileName := range included {
		// If there are overlapping fields, return an error
		if _, ok := excluded[fileName]; ok {
			// field overlap
			return fmt.Errorf("%w field overlap on file %s", ErrFieldOverlap, fileName)
		}
	}

	if len(e.includedFiles) > 0 && len(e.excludedFiles) > 0 {
		return ErrMutuallyExclusiveFiles
	}

	if slices.Contains(e.ExcludedFiles(), "shapes.txt") {
		if e.ExcludeShapes() {
			return ErrMutuallyExclusiveShapes
		}
		return ErrShapesExcluded
	}

	e.validated = true

	return nil
}

func parseFieldsFieldList(fieldList []string) (map[string][]string, error) {
	result := make(map[string][]string)
	for _, ef := range fieldList {
		next, stop := iter.Pull(strings.SplitSeq(ef, ","))

		filename, ok := next()
		if !ok || filename == "" {
			stop()
			return nil, ErrInvalidExclude
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
			return nil, ErrInvalidExclude
		}
	}
	return result, nil
}
