package params

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
)

var (
	ErrInvalidExclude          = errors.New("invalid exclude/include fields format; must be filename,field1,field2,...")
	ErrMutuallyExclusiveFiles  = errors.New("include-files and exclude-files flags are mutually exclusive")
	ErrMutuallyExclusiveShapes = errors.New("exclude-shapes flag cannot be used with exclude-files including shapes.txt")
	ErrShapesExcluded          = errors.New("shapes.txt is excluded")
	ErrFieldOverlap            = errors.New("a field cannot be both included and excluded")
	ErrNotParsed               = errors.New("parameters not parsed")
	ErrParsingFailed           = errors.New("parsing parameters failed")
)

// ExtractParams holds the parameters for the extract command, including file and field filters.
// It also provides methods for parsing and validating these parameters.
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

	parsed bool
}

func NewExtractParams(
	excludedFiles, includedFiles []string,
	excludeEmptyFiles, excludeEmptyFields, excludeShapes bool,
	excludedFields, includedFields []string,
) *ExtractParams {
	ret := internalNew(
		excludedFiles,
		includedFiles,
		excludeEmptyFiles,
		excludeEmptyFields,
		excludeShapes,
		nil,
		nil,
	)

	ret._excludedFields = excludedFields
	ret._includedFields = includedFields

	return ret
}

// Creates an ExtractParams instance with all fields already set. Parsing still required to check validity.
func NewExtractParamsParsed(
	excludedFiles, includedFiles []string,
	excludeEmptyFiles, excludeEmptyFields, excludeShapes bool,
	excludedFields, includedFields map[string][]string,
) *ExtractParams {
	ret := internalNew(
		excludedFiles,
		includedFiles,
		excludeEmptyFiles,
		excludeEmptyFields,
		excludeShapes,
		excludedFields,
		includedFields,
	)

	if excludedFields == nil {
		ret.excludedFields = make(map[string][]string)
	}
	if includedFields == nil {
		ret.includedFields = make(map[string][]string)
	}

	return ret
}

func internalNew(
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
		parsed:             false,
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

func (e *ExtractParams) ParseAndValidate() error {
	if e.parsed {
		return nil
	}
	var err error

	if e.excludedFields == nil && len(e._excludedFields) > 0 {
		e.excludedFields, err = parseFieldsFieldList(e._excludedFields)
		if err != nil {
			return errors.Join(ErrParsingFailed, fmt.Errorf("error parsing excluded fields: %w", err))
		}
	}
	if e.includedFields == nil && len(e._includedFields) > 0 {
		e.includedFields, err = parseFieldsFieldList(e._includedFields)
		if err != nil {
			return errors.Join(ErrParsingFailed, fmt.Errorf("error parsing included fields: %w", err))
		}
	}

	included := e.includedFields
	excluded := e.excludedFields

	for fileName := range included {
		// If there are overlapping fields, return an error
		if _, ok := excluded[fileName]; ok {
			// field overlap
			return errors.Join(ErrParsingFailed, ErrFieldOverlap, fmt.Errorf("overlap on file %s", fileName))
		}
	}

	if len(e.includedFiles) > 0 && len(e.excludedFiles) > 0 {
		return errors.Join(ErrParsingFailed, ErrMutuallyExclusiveFiles)
	}

	if slices.Contains(e.ExcludedFiles(), "shapes.txt") {
		if e.ExcludeShapes() {
			return errors.Join(ErrParsingFailed, ErrMutuallyExclusiveShapes)
		}
		return errors.Join(ErrParsingFailed, ErrShapesExcluded)
	}

	// if we want to exclude shapes, then we need to exclude field shape_id from trips.txt, and exclude shapes.txt from files
	// add to the map of excluded fields
	if e.ExcludeShapes() {
		e.excludedFields["trips.txt"] = append(e.excludedFields["trips.txt"], "shape_id")
		e.excludedFiles = append(e.excludedFiles, "shapes.txt")
	}

	e.parsed = true

	return nil
}

func (e *ExtractParams) IsParsedAndValid() bool {
	return e.parsed
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
