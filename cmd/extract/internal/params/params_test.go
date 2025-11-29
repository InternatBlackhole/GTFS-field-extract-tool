package params

import (
	"slices"
	"testing"
)

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

func Test_ParseAndValidate(t *testing.T) {
	tests := []struct {
		name    string
		params  *ExtractParams
		wantErr bool
	}{
		{
			name: "exclude-files and include-files mutually exclusive",
			params: &ExtractParams{
				excludedFiles: []string{"stops.txt"},
				includedFiles: []string{"routes.txt"}},
			wantErr: true,
		},
		{
			name: "exclude-shapes and exclude-files shapes.txt mutually exclusive",
			params: &ExtractParams{
				excludeShapes: true,
				excludedFiles: []string{"shapes.txt"}},
			wantErr: true,
		},
		{
			name: "shapes.txt cannot be excluded directly",
			params: &ExtractParams{
				excludedFiles: []string{"shapes.txt"}},
			wantErr: true,
		},
		{
			name: "same field cannot be both included and excluded",
			params: &ExtractParams{
				includedFields: map[string][]string{
					"stops.txt": {"stop_name"},
				},
				excludedFields: map[string][]string{
					"stops.txt": {"stop_name"},
				}},

			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.params.ParseAndValidate()
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractParams.Parse() error = %v, wantErr %v", err, tt.wantErr)
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
