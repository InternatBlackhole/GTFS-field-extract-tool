package internal_test

import (
	"slices"
	"testing"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/extract/internal"
)

func TestAssignByBoolMask(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		assignee []any
		input    []any
		mask     []bool
		want     []any
	}{
		{
			name:     "basic test",
			assignee: make([]any, 3),
			input:    []any{1, 2, 3, 4, 5},
			mask:     []bool{true, false, true, false, true},
			want:     []any{1, 3, 5},
		},
		{
			name:     "all true mask",
			assignee: make([]any, 5),
			input:    []any{"a", "b", "c", "d", "e"},
			mask:     []bool{true, true, true, true, true},
			want:     []any{"a", "b", "c", "d", "e"},
		},
		{
			name:     "all false mask",
			assignee: make([]any, 0),
			input:    []any{10, 20, 30},
			mask:     []bool{false, false, false},
			want:     []any{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := internal.AssignByBoolMask(tt.assignee, tt.input, tt.mask)
			// TODO: update the condition below to compare got with tt.want.
			if !slices.Equal(got, tt.want) {
				t.Errorf("AssignByBoolMask() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyBoolMaskToSlice(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		input []any
		mask  []bool
		want  []any
	}{
		{
			name:  "basic test",
			input: []any{1, 2, 3, 4, 5},
			mask:  []bool{true, false, true, false, true},
			want:  []any{1, 3, 5},
		},
		{
			name:  "all true mask",
			input: []any{"a", "b", "c", "d", "e"},
			mask:  []bool{true, true, true, true, true},
			want:  []any{"a", "b", "c", "d", "e"},
		},
		{
			name:  "all false mask",
			input: []any{10, 20, 30},
			mask:  []bool{false, false, false},
			want:  []any{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := internal.ApplyBoolMaskToSlice(tt.input, tt.mask)
			// TODO: update the condition below to compare got with tt.want.
			if !slices.Equal(got, tt.want) {
				t.Errorf("ApplyBoolMaskToSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}
