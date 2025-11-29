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
