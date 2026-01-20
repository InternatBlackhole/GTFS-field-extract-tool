package filesmerger

import (
	"bytes"
	"encoding/csv"
	"io"
	"strings"
	"testing"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/merge/internal/mergeparams"
	"github.com/InternatManhole/dujpp-gtfs-tool/internal/logging"
)

// Tests initilizer
func TestMain(m *testing.M) {
	logging.SetNewLoggerWithLevel(logging.EvenMoreVerbose)
	// No special setup needed currently
	m.Run()
}

func TestMergeFiles_WithPrefixes(t *testing.T) {
	csv1 := "_id,field1\n1,A\n2,B\n"
	csv2 := "_id,field2\n1,C\n3,D\n"

	readers := []string{csv1, csv2}
	inputReaders := make([]*strings.Reader, 0, len(readers))
	for _, s := range readers {
		inputReaders = append(inputReaders, strings.NewReader(s))
	}

	// writer
	var out bytes.Buffer
	writerCreate := func() (io.Writer, func()) {
		return &out, func() {}
	}

	// prefixes provided for two input files
	params := *mergeparams.NewMergeParams([]string{"p1_", "p2_"}, false)
	fm := NewFilesMergerWithParams(params)

	// convert []*strings.Reader to []io.Reader
	in := make([]io.Reader, len(inputReaders))
	for i, r := range inputReaders {
		in[i] = r
	}

	if err := fm.MergeFiles(in, writerCreate); err != nil {
		t.Fatalf("MergeFiles failed: %v", err)
	}

	// parse output CSV
	outR := csv.NewReader(strings.NewReader(out.String()))
	records, err := outR.ReadAll()
	if err != nil {
		t.Fatalf("failed to parse output CSV: %v", err)
	}

	// expected header: id,field1,field2
	if len(records) == 0 {
		t.Fatalf("no records written")
	}
	header := records[0]
	expectedHeader := []string{"_id", "field1", "field2"}
	if !equalStrings(header, expectedHeader) {
		t.Fatalf("unexpected header: got %v want %v", header, expectedHeader)
	}

	// collect ids from body
	ids := make(map[string]any)
	for _, r := range records[1:] {
		if len(r) == 0 {
			continue
		}
		ids[r[0]] = nil
	}

	// Expect original 1 and 2, and prefixed  p2_1 and 3
	want := []string{"1", "2", "p2_1", "3"}
	for _, w := range want {
		if _, ok := ids[w]; !ok {
			t.Fatalf("missing expected id %q in output: ids=%v", w, ids)
		}
	}
}

func TestMergeFiles_ForceKeepsDuplicates(t *testing.T) {
	csv1 := "id,field1\n1,A\n"
	csv2 := "id,field1\n1,B\n"

	in := []io.Reader{strings.NewReader(csv1), strings.NewReader(csv2)}

	var out bytes.Buffer
	writerCreate := func() (io.Writer, func()) { return &out, func() {} }

	params := *mergeparams.NewMergeParams([]string{"p_"}, true)
	fm := NewFilesMergerWithParams(params)

	if err := fm.MergeFiles(in, writerCreate); err != nil {
		t.Fatalf("MergeFiles failed: %v", err)
	}

	outR := csv.NewReader(strings.NewReader(out.String()))
	records, err := outR.ReadAll()
	if err != nil {
		t.Fatalf("failed to parse output CSV: %v", err)
	}

	if len(records) < 3 {
		t.Fatalf("expected at least header + 2 rows, got %d", len(records))
	}

	// ensure both duplicate id values appear somewhere in the output
	foundFirst := false
	foundSecond := false
	for _, r := range records[1:] {
		if r[0] == "1" && r[1] == "A" {
			foundFirst = true
		}
		if r[0] == "1" && r[1] == "B" {
			foundSecond = true
		}
	}
	if !foundFirst || !foundSecond {
		t.Fatalf("expected both duplicate records to be present, foundFirst=%v foundSecond=%v", foundFirst, foundSecond)
	}
}

// helper
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
