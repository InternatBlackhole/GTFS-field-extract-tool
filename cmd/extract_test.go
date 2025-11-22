package cmd

import (
	"archive/zip"
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
