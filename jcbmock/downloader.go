package jcbmock

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// Downloads and caches the mock server, so that it is retrievable
// for automatic testing

const defaultMockVersion = "1.5.23"
const defaultMockFile = "CouchbaseMock-" + defaultMockVersion + ".jar"
const defaultMockUrl = "https://packages.couchbase.com/clients/c/mock/" + defaultMockFile

// GetMockPath ensures that the mock path is available
func GetMockPath() (path string, err error) {
	var url string
	if path = os.Getenv("GOCB_MOCK_PATH"); path == "" {
		path = strings.Join([]string{os.TempDir(), defaultMockFile}, string(os.PathSeparator))
	}
	if url = os.Getenv("GOCB_MOCK_URL"); url == "" {
		url = defaultMockUrl
	}

	path, err = filepath.Abs(path)
	if err != nil {
		throwMockError("Couldn't get absolute path (!)", err)
	}

	info, err := os.Stat(path)
	if err == nil && info.Size() > 0 {
		return path, nil
	} else if err != nil && !os.IsNotExist(err) {
		throwMockError("Couldn't resolve existing path", err)
	}

	if err := os.Remove(path); err != nil {
		log.Printf("Couldn't remove existing mock: %v", err)
	}
	log.Printf("Downloading %s to %s", url, path)

	resp, err := http.Get(defaultMockUrl)
	if err != nil {
		throwMockError("Couldn't create HTTP request (or other error)", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != 200 {
		throwMockError(fmt.Sprintf("Got HTTP %d from URL", resp.StatusCode), errors.New("non-200 response"))
	}

	out, err := os.Create(path)
	if err != nil {
		throwMockError("Couldn't open output file", err)
	}
	if err := out.Close(); err != nil {
		log.Printf("Failed to close file: %v", err)
	}

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		throwMockError("Couldn't write response", err)
	}

	return path, nil
}
