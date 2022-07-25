package gocbcore

import (
	"fmt"
	"strings"
	"testing"
)

func TestGenerateIDs(t *testing.T) {
	var ids []string
	prefix := "rangecollectionretry"
	var counter int
	for {
		id := fmt.Sprintf("%s-%d", prefix, counter)
		counter++
		if cbCrc([]byte(id)) == 12 {
			ids = append(ids, id)
		}
		if len(ids) == 10 {
			fmt.Println(strings.Join(ids, "\",\""))
			return
		}
	}
}
