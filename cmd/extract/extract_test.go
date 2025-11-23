package extract

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"slices"
	"testing"
)

func Test_filterFiles(t *testing.T) {
	genZip := func(name string) *zip.File {
		return &zip.File{FileHeader: zip.FileHeader{Name: name}}
	}
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		srcFiles    []*zip.File
		filterFiles []string
		include     bool
		want        []*zip.File
	}{
		{
			name: "include files",
			srcFiles: []*zip.File{
				genZip("a.txt"),
				genZip("b.txt"),
				genZip("c.txt"),
			},
			filterFiles: []string{"a.txt", "c.txt"},
			include:     true,
			want: []*zip.File{
				genZip("a.txt"),
				genZip("c.txt"),
			},
		},
		{
			name: "exclude files",
			srcFiles: []*zip.File{
				genZip("a.txt"),
				genZip("b.txt"),
				genZip("c.txt"),
			},
			filterFiles: []string{"a.txt", "c.txt"},
			include:     false,
			want: []*zip.File{
				genZip("b.txt"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterFiles(tt.srcFiles, tt.filterFiles, tt.include)
			// TODO: update the condition below to compare got with tt.want.
			if !slices.EqualFunc(got, tt.want, func(a, b *zip.File) bool {
				return a.Name == b.Name
			}) {
				t.Errorf("filterFiles() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseFieldsFieldList(t *testing.T) {
	tests := []struct {
		name      string
		fieldList []string
		want      map[string][]string
		wantErr   bool
	}{
		{
			name:      "valid field list",
			fieldList: []string{"file1,field1,field2", "file2,field3"},
			want: map[string][]string{
				"file1": {"field1", "field2"},
				"file2": {"field3"},
			},
			wantErr: false,
		},
		{
			name:      "invalid field list format",
			fieldList: []string{"file1,field1,field2", "file2"},
			want:      nil,
			wantErr:   true,
		},
		{
			name:      "empty field list",
			fieldList: []string{},
			want:      map[string][]string{},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := parseFieldsFieldList(tt.fieldList)
			if (gotErr != nil) != tt.wantErr {
				t.Errorf("parseFieldsFieldList() error = %v, wantErr %v", gotErr, tt.wantErr)
			}
			if !tt.wantErr && !mapsEqual(got, tt.want) {
				t.Errorf("parseFieldsFieldList() = %v, want %v", got, tt.want)
			}
		})
	}
}

type zipTestFunc func(reader *zip.Reader, filename string, f *zip.File) error // error if test fails
type zipTests map[string]zipTestFunc                                          // filename, test function

func Test_extractCommand(t *testing.T) {
	t.Chdir(os.Getenv("PWD_CODE"))

	readBuffer := new(bytes.Buffer)
	//read whole file into buffer
	inputFile, err := os.Open("feed.zip")
	if err != nil {
		t.Fatalf("failed to open input zip file: %v", err)
	}
	defer inputFile.Close()
	_, err = readBuffer.ReadFrom(inputFile)
	if err != nil {
		t.Fatalf("failed to read input zip file: %v", err)
	}
	newZipReader := func() *zip.Reader {
		zipReader, err := zip.NewReader(bytes.NewReader(readBuffer.Bytes()), int64(readBuffer.Len()))
		if err != nil {
			t.Fatalf("failed to create zip reader: %v", err)
		}
		return zipReader
	}

	tests := []struct {
		name string

		inputZip func() *zip.Reader
		//outputZip func() (*zip.Writer, *bytes.Buffer, func()) // returns zip writer and a cleanup function
		flags *ExtractParams

		wantErr        bool
		outputZipTests zipTests // if len > 0, runs tests on output zip, if key "ALL", runs single test on whole zip
	}{
		//THESE SHOULD BE TESTS FOR THE Validate() FUNCTION
		{
			name:     "exclude-files and include-files mutually exclusive",
			inputZip: newZipReader,
			flags: &ExtractParams{
				excludedFiles: []string{"stops.txt"},
				includedFiles: []string{"routes.txt"}},
			wantErr: true,
		},
		{
			name:     "exclude-shapes and exclude-files shapes.txt mutually exclusive",
			inputZip: newZipReader,
			flags: &ExtractParams{
				excludeShapes: true,
				excludedFiles: []string{"shapes.txt"}},
			wantErr: true,
		},
		{
			name:     "shapes.txt cannot be excluded directly",
			inputZip: newZipReader,
			flags: &ExtractParams{
				excludedFiles: []string{"shapes.txt"}},
			wantErr: true,
		},
		{
			name:     "same field cannot be both included and excluded",
			inputZip: newZipReader,
			flags: &ExtractParams{
				includedFields: map[string][]string{
					"stops.txt": {"stop_name"},
				},
				excludedFields: map[string][]string{
					"stops.txt": {"stop_name"},
				}},

			wantErr: true,
		},
		{
			name:     "just exclude files",
			inputZip: newZipReader,
			//exclude shapes not yet implemented
			flags:   &ExtractParams{excludedFiles: []string{"stops.txt"} /*excludeShapes: true*/},
			wantErr: false,
			outputZipTests: zipTests{
				"ALL": allExcept("stops.txt" /*"shapes.txt"*/),
			},
		},
		{
			name:     "no exclude or include files, just fields filters, also they are valid",
			inputZip: newZipReader,
			flags: &ExtractParams{
				includedFields: map[string][]string{
					"stops.txt": {"stop_name", "stop_id"},
				},
				excludedFields: map[string][]string{
					"routes.txt": {"route_color"},
				},
			},
			wantErr: false,
			outputZipTests: zipTests{
				"routes.txt": hasHeader("route_id", "agency_id", "route_long_name", "route_type", "route_text_color"), // route_desc excluded
				"stops.txt":  hasHeader("stop_name", "stop_id"),
			},
		},
		{
			name:     "preserve invalid columns in fare_leg_rules.txt",
			inputZip: newZipReader,
			flags:    &ExtractParams{includedFiles: []string{"fare_leg_rules.txt"}},
			wantErr:  false,
			outputZipTests: zipTests{
				"fare_leg_rules.txt": hasHeader("fare_product_id", "min_distance", "max_distance", "distance_type"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inputZip := tt.inputZip()
			buffer := new(bytes.Buffer)
			zipWriter := zip.NewWriter(buffer)

			err := extract(inputZip, zipWriter, tt.flags)
			zipWriter.Close()

			if (err != nil) != tt.wantErr {
				t.Errorf("extract() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(tt.outputZipTests) > 0 && !tt.wantErr {
				zipReader, err := zip.NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
				if err != nil {
					t.Errorf("failed to create zip reader: %v", err)
					return
				}
				//defer zipReader.Close()
				if testFunc, ok := tt.outputZipTests["ALL"]; ok {
					if err := testFunc(zipReader, "", nil); err != nil {
						t.Errorf("output zip test failed: %v", err)
					}
				} else {
					fileMap := make(map[string]*zip.File, len(zipReader.File))
					for _, f := range zipReader.File {
						fileMap[f.Name] = f
					}
					for filename, testFunc := range tt.outputZipTests {
						if err := testFunc(zipReader, filename, fileMap[filename]); err != nil {
							t.Errorf("output zip test for file %s failed: %v", filename, err)
						}
					}
				}
			}
		})
	}
}

func allExcept(expectedExcludedFiles ...string) zipTestFunc {
	return func(r *zip.Reader, _ string, _ *zip.File) error {
		excludedFilesMap := make(map[string]bool, len(expectedExcludedFiles))
		for _, f := range expectedExcludedFiles {
			excludedFilesMap[f] = true
		}
		for _, f := range r.File {
			if excludedFilesMap[f.Name] {
				return fmt.Errorf("file %s was not expected to be in zip", f.Name)
			}
		}
		return nil
	}
}

func hasHeader(expectedHeaderVals ...string) zipTestFunc {
	return func(r *zip.Reader, _ string, zipFile *zip.File) error {
		zipFileReader, err := zipFile.Open()
		if err != nil {
			return err
		}
		defer zipFileReader.Close()
		csvReader := csv.NewReader(zipFileReader)

		// Read the header
		header, err := csvReader.Read()
		if err != nil {
			return err
		}

		if len(header) != len(expectedHeaderVals) {
			return fmt.Errorf("header length %d does not match expected length %d", len(header), len(expectedHeaderVals))
		}

		// Compare with expected, order doesn't matter
		for _, val := range expectedHeaderVals {
			if !slices.Contains(header, val) {
				return fmt.Errorf("expected header value %s not found in %v", val, header)
			}
		}
		return nil
	}
}

/*func fileFinder(z *zip.Reader, filename string) *zip.File {
	for _, f := range z.File {
		if f.Name == filename {
			return f
		}
	}
	return nil
}*/

func exists(filename string) zipTestFunc {
	return func(r *zip.Reader, _ string, f *zip.File) error {
		if f != nil {
			return nil
		}
		return fmt.Errorf("file %s does not exist in zip", filename)
	}
}

func mapsEqual(a, b map[string][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, aValue := range a {
		bValue, exists := b[key]
		if !exists || !slices.Equal(aValue, bValue) {
			return false
		}
	}
	return true
}
