package internal

import "slices"

// ApplyBoolMaskToSlice returns a new slice containing only the elements of the input slice
// for which the corresponding value in the mask is true.
func ApplyBoolMaskToSlice[T any](input []T, mask []bool) []T {
	if len(input) != len(mask) {
		panic("input slice and mask must have the same length")
	}
	result := make([]T, 0, len(input))
	for i, v := range input {
		if mask[i] {
			result = append(result, v)
		}
	}
	return slices.Clip(result)
}

// AssignByBoolMask returns assignee slice with elements from input slice assigned to positions
// where the corresponding value in the mask is true, in the same order as they appear in input slice.
func AssignByBoolMask[T any](assignee []T, input []T, mask []bool) []T {
	if len(input) != len(mask) {
		panic("input slice and mask must have the same length")
	}
	assigneeIndex := 0
	for i, v := range input {
		if mask[i] {
			assignee[assigneeIndex] = v
			assigneeIndex++
		}
	}
	return assignee
}

func ToAny[T any](s []T) []any {
	r := make([]any, len(s))
	for i, v := range s {
		r[i] = v
	}
	return r
}
