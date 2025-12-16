package file_test

import (
	"io"
	"slices"
	"testing"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal/extract/file"
	"github.com/InternatManhole/dujpp-gtfs-tool/internal/logging"
)

type testExtractParams struct {
	excludeEmptyFiles  bool
	excludeEmptyFields bool

	includedFields []string
	excludedFields []string
}

func TestFileExtractor_Run(t *testing.T) {
	var reporter logging.LogReporter = func(level logging.StatusLevel, format string, a ...any) {
		//fmt.Println(status)
	}
	tests := []struct {
		name string // description of this test case
		// Named input parameters for receiver constructor.
		extrator *file.FileExtractor

		// Named input parameters for target function.
		fileReader   io.Reader
		writerCreate func() (io.Writer, func())
		wantErr      bool
	}{
		{
			name: "no exclude or include files, just fields filters, also they are valid",
			extrator: file.NewFileExtractorAll(
				"stops.txt",
				reporter,
				false,
				false,
				[]string{"stop_id", "stop_name"},
				[]string{"stop_desc"},
			),
			fileReader:   nil, // TODO: implement mock reader
			writerCreate: nil, // TODO: implement mock writer
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fe := tt.extrator
			gotErr := fe.Run(tt.fileReader, tt.writerCreate)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Run() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Run() succeeded unexpectedly")
			}
		})
	}
}

func TestGenerateFieldMapping(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		// Enforce that exclusion and inclusion are mutually exclusive.
		extractedHeader []string
		includedFields  []string
		excludedFields  []string
		want            []bool
		want2           int
	}{
		{
			name:            "include stop_id and stop_name",
			extractedHeader: []string{"stop_id", "stop_name", "stop_desc", "stop_lat", "stop_lon"},
			includedFields:  []string{"stop_id", "stop_name"},
			//excludedFields:  []string{"stop_desc"},
			want:  []bool{true, true, false, false, false},
			want2: 2,
		},
		{
			name:            "exclude stop_desc",
			extractedHeader: []string{"stop_id", "stop_name", "stop_desc", "stop_lat", "stop_lon"},
			//includedFields:  []string{"stop_id", "stop_name"},
			excludedFields: []string{"stop_desc"},
			want:           []bool{true, true, false, true, true},
			want2:          4,
		},
		{
			name:            "no include or exclude fields",
			extractedHeader: []string{"stop_id", "stop_name", "stop_desc", "stop_lat", "stop_lon"},
			//includedFields:  []string{"stop_id", "stop_name"},
			//excludedFields:  []string{"stop_desc"},
			want:  []bool{true, true, true, true, true},
			want2: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got2 := file.GenerateFieldMapping(tt.extractedHeader, tt.includedFields, tt.excludedFields)
			// TODO: update the condition below to compare got with tt.want.
			if slices.Equal(got, tt.want) == false {
				t.Errorf("GenerateFieldMapping() = %v, want %v", got, tt.want)
			}
			if got2 != tt.want2 {
				t.Errorf("GenerateFieldMapping() = %v, want %v", got2, tt.want2)
			}
		})
	}
}
