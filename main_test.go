package main

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"

	"golang.org/x/sync/errgroup"
)

const testFile = "../../gunnarmorling/1brc/measurements1B.txt"

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

func DupFile(file *os.File) (*os.File, error) {
	nfd, err := syscall.Dup(int(file.Fd()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(nfd), file.Name()), nil
}

// Read file from multiple goroutines
func TestReadFileConcurrent(t *testing.T) {
	file, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	s2 := fi.Size()
	s1 := s2 / 2
	s2 -= s1

	pageSize := os.Getpagesize()
	read := func(file *os.File, offset, size int64) error {
		buf := make([]byte, pageSize)
		_, err := file.Seek(offset, io.SeekStart)
		if err != nil {
			return err
		}
		for size > 0 {
			n, err := file.Read(buf[:min(pageSize, int(size))])
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			size -= int64(n)
		}
		return nil
	}

	dup, err := DupFile(file)
	if err != nil {
		t.Fatalf("failed to dup file: %v", err)
	}
	defer dup.Close()

	eg, _ := errgroup.WithContext(context.Background())

	eg.Go(func() error {
		return read(file, 0, s1)
	})
	eg.Go(func() error {
		return read(dup, s1, s2)
	})

	err = eg.Wait()
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
}
