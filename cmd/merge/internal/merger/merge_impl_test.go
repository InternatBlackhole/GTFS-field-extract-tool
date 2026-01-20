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
}
