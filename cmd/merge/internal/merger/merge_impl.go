package merger

import (
	"archive/zip"
	"io"

	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/merge/internal/mergeparams"
	"github.com/InternatManhole/dujpp-gtfs-tool/cmd/merge/internal/merger/filesmerger"
	"github.com/InternatManhole/dujpp-gtfs-tool/internal/logging"
)

type Merger struct {
	params *mergeparams.MergeParams
}

func NewMerger(params *mergeparams.MergeParams) *Merger {
	return &Merger{
		params: params,
	}
}

func (m *Merger) Merge(inputArchives []*zip.Reader, outputArchive *zip.Writer) error {
	logger := logging.GetLogger()
	// Plan:
	// 1. Read the same .txt files from all inputArchives
	// 2. Merge their contents according to m.params
	// 3. Write the merged contents to outputArchive

	// If m.params.IsForce is true, ignore ID conflicts
	logger.Info("Merging GTFS files with prefixes: %v, force: %v", m.params.GetPrefixes(), m.params.IsForce())

	// First collect all unique file names across all input archives
	allFileNames := map[string][]io.Reader{} // used as a set
	for _, inputArchive := range inputArchives {
		for _, file := range inputArchive.File {
			rc, err := file.Open()
			if err != nil {
				logger.Error("Failed to open file %s: %v", file.Name, err)
				return err
			}
			allFileNames[file.Name] = append(allFileNames[file.Name], rc)
			defer rc.Close()
		}
	}

	// For each unique file name, merge contents from all input archives
	for fileName, rcs := range allFileNames {
		logger.Info("Merging file: %s, in %d archives", fileName, len(rcs))
		fileMerger := filesmerger.NewFilesMergerWithParams(*m.params)
		err := fileMerger.MergeFiles(rcs, func() (io.Writer, func()) {
			w, err := outputArchive.Create(fileName)
			if err != nil {
				logger.Error("Failed to create file %s in output archive: %v", fileName, err)
				return nil, func() {}
			}
			return w, func() {}
		})
		if err != nil {
			logger.Error("Failed to merge file %s: %v", fileName, err)
			return err
		}
	}

	return nil
}
