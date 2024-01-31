package main

import (
	"errors"
	"io"
	"os"
	"testing"
)

const testFile = "../../gunnarmorling/1brc/measurements1K.txt"

// on my machine on average it takes 4-5.5 seconds
// to read the test file
func TestReadFile(t *testing.T) {
	file, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	pageSize := os.Getpagesize()
	buf := make([]byte, pageSize)
	for {
		_, err := file.Read(buf[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			t.Fatalf("failed read file: %v", err)
		}
	}
}

func TestSolve(t *testing.T) {
	err := Solve(testFile)
	if err != nil {
		t.Fatalf("failed to solve: %v", err)
	}
}
