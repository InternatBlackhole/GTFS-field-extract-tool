package merger

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/merge/internal/mergeparams"
	"github.com/InternatManhole/dujpp-gtfs-tool/internal/logging"
)

func createZipBytes(files map[string]string) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, content := range files {
		f, _ := zw.Create(name)
		f.Write([]byte(content))
	}
	zw.Close()
	return buf.Bytes()
}

func TestMain(m *testing.M) {
	logging.SetNewLoggerWithLevel(logging.EvenMoreVerbose)
}

func TestMerger_Merge_Stops(t *testing.T) {

	// two archives each with stops.txt where stop_id 1 conflicts
	files1 := map[string]string{
		"stops.txt": "stop_id,stop_name\n1,Alpha\n2,Beta\n",
	}
	files2 := map[string]string{
		"stops.txt": "stop_id,stop_name\n1,Gamma\n3,Delta\n",
	}

	b1 := createZipBytes(files1)
	b2 := createZipBytes(files2)

	zr1, err := zip.NewReader(bytes.NewReader(b1), int64(len(b1)))
	if err != nil {
		t.Fatalf("failed to create zip reader 1: %v", err)
	}
	zr2, err := zip.NewReader(bytes.NewReader(b2), int64(len(b2)))
	if err != nil {
		t.Fatalf("failed to create zip reader 2: %v", err)
	}

	var outBuf bytes.Buffer
	zw := zip.NewWriter(&outBuf)

	params := mergeparams.NewMergeParams([]string{"p1_", "p2_"}, false)
	m := NewMerger(params)

	if err := m.Merge([]*zip.Reader{zr1, zr2}, zw); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// close writer to flush
	if err := zw.Close(); err != nil {
		t.Fatalf("failed to close output zip writer: %v", err)
	}

	// open merged zip
	outBytes := outBuf.Bytes()
	zrOut, err := zip.NewReader(bytes.NewReader(outBytes), int64(len(outBytes)))
	if err != nil {
		t.Fatalf("failed to open output zip: %v", err)
	}

	// find stops.txt
	var stopsContent string
	for _, f := range zrOut.File {
		if f.Name == "stops.txt" {
			rc, err := f.Open()
			if err != nil {
				t.Fatalf("failed to open merged stops.txt: %v", err)
			}
			b, _ := ioutil.ReadAll(rc)
			rc.Close()
			stopsContent = string(b)
		}
	}

	if stopsContent == "" {
		t.Fatalf("merged stops.txt not found in output zip")
	}

	// parse CSV and check ids present
	r := csv.NewReader(strings.NewReader(stopsContent))
	recs, err := r.ReadAll()
	if err != nil {
		t.Fatalf("failed to parse merged stops.csv: %v", err)
	}

	// header check
	if len(recs) == 0 || recs[0][0] != "stop_id" {
		t.Fatalf("unexpected header in merged stops: %v", recs)
	}

	// collect ids
	ids := make(map[string]struct{})
	for _, row := range recs[1:] {
		if len(row) == 0 {
			continue
		}
		ids[row[0]] = struct{}{}
	}

	want := []string{"1", "2", "p2_1", "3"}
	for _, w := range want {
		if _, ok := ids[w]; !ok {
			t.Fatalf("expected id %q in merged stops, ids=%v", w, ids)
		}
	}

	// Subtest: blank prefix for first archive (index 0) only
	t.Run("BlankPrefixFirstArchive", func(t *testing.T) {
		// recreate zip readers
		b1 := createZipBytes(files1)
		b2 := createZipBytes(files2)

		zr1, err := zip.NewReader(bytes.NewReader(b1), int64(len(b1)))
		if err != nil {
			t.Fatalf("failed to create zip reader 1: %v", err)
		}
		zr2, err := zip.NewReader(bytes.NewReader(b2), int64(len(b2)))
		if err != nil {
			t.Fatalf("failed to create zip reader 2: %v", err)
		}

		var outBuf2 bytes.Buffer
		zw2 := zip.NewWriter(&outBuf2)

		params2 := mergeparams.NewMergeParams([]string{"", "p2_"}, false)
		m2 := NewMerger(params2)

		if err := m2.Merge([]*zip.Reader{zr1, zr2}, zw2); err != nil {
			t.Fatalf("Merge failed for blank-prefix subtest: %v", err)
		}
		if err := zw2.Close(); err != nil {
			t.Fatalf("failed to close output zip writer: %v", err)
		}

		outBytes := outBuf2.Bytes()
		zrOut, err := zip.NewReader(bytes.NewReader(outBytes), int64(len(outBytes)))
		if err != nil {
			t.Fatalf("failed to open output zip: %v", err)
		}

		var stopsContent2 string
		for _, f := range zrOut.File {
			if f.Name == "stops.txt" {
				rc, err := f.Open()
				if err != nil {
					t.Fatalf("failed to open merged stops.txt: %v", err)
				}
				b, _ := ioutil.ReadAll(rc)
				rc.Close()
				stopsContent2 = string(b)
			}
		}

		if stopsContent2 == "" {
			t.Fatalf("merged stops.txt not found in output zip (blank-prefix subtest)")
		}

		r2 := csv.NewReader(strings.NewReader(stopsContent2))
		recs2, err := r2.ReadAll()
		if err != nil {
			t.Fatalf("failed to parse merged stops.csv: %v", err)
		}

		ids2 := make(map[string]struct{})
		for _, row := range recs2[1:] {
			if len(row) == 0 {
				continue
			}
			ids2[row[0]] = struct{}{}
		}

		// Expect 1, 2, p2_1, 3 when first archive is unprefixed
		want2 := []string{"1", "2", "p2_1", "3"}
		for _, w := range want2 {
			if _, ok := ids2[w]; !ok {
				t.Fatalf("expected id %q in blank-prefix merged stops, ids=%v", w, ids2)
			}
		}
	})

	// Subtest: force mode keeps duplicates from both archives (same id appears twice)
	t.Run("ForceKeepsDuplicates_Merger", func(t *testing.T) {
		// recreate zip readers
		b1 := createZipBytes(files1)
		b2 := createZipBytes(files2)

		zr1, err := zip.NewReader(bytes.NewReader(b1), int64(len(b1)))
		if err != nil {
			t.Fatalf("failed to create zip reader 1: %v", err)
		}
		zr2, err := zip.NewReader(bytes.NewReader(b2), int64(len(b2)))
		if err != nil {
			t.Fatalf("failed to create zip reader 2: %v", err)
		}

		var outBuf3 bytes.Buffer
		zw3 := zip.NewWriter(&outBuf3)

		params3 := mergeparams.NewMergeParams([]string{"p1_", "p2_"}, true)
		m3 := NewMerger(params3)

		if err := m3.Merge([]*zip.Reader{zr1, zr2}, zw3); err != nil {
			t.Fatalf("Merge failed for force-mode subtest: %v", err)
		}
		if err := zw3.Close(); err != nil {
			t.Fatalf("failed to close output zip writer: %v", err)
		}

		outBytes := outBuf3.Bytes()
		zrOut, err := zip.NewReader(bytes.NewReader(outBytes), int64(len(outBytes)))
		if err != nil {
			t.Fatalf("failed to open output zip: %v", err)
		}

		var stopsContent3 string
		for _, f := range zrOut.File {
			if f.Name == "stops.txt" {
				rc, err := f.Open()
				if err != nil {
					t.Fatalf("failed to open merged stops.txt: %v", err)
				}
				b, _ := ioutil.ReadAll(rc)
				rc.Close()
				stopsContent3 = string(b)
			}
		}

		if stopsContent3 == "" {
			t.Fatalf("merged stops.txt not found in output zip (force-mode subtest)")
		}

		r3 := csv.NewReader(strings.NewReader(stopsContent3))
		recs3, err := r3.ReadAll()
		if err != nil {
			t.Fatalf("failed to parse merged stops.csv: %v", err)
		}

		// collect id->names
		names := make(map[string]map[string]bool)
		for _, row := range recs3[1:] {
			if len(row) == 0 {
				continue
			}
			id := row[0]
			name := ""
			if len(row) > 1 {
				name = row[1]
			}
			if _, ok := names[id]; !ok {
				names[id] = make(map[string]bool)
			}
			names[id][name] = true
		}

		// For id "1" we expect both "Alpha" and "Gamma" to be present when force=true
		if !names["1"]["Alpha"] || !names["1"]["Gamma"] {
			t.Fatalf("expected both conflicting records for id '1' to be present under force mode; got names=%v", names["1"])
		}
	})
}
