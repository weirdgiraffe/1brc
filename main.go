package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"unsafe"
)

func debugf(format string, args ...any) {
	// fmt.Printf(format, args...)
}

func ValueToFloat(value []byte) (float64, error) {
	s := unsafe.String(unsafe.SliceData(value), len(value))
	return strconv.ParseFloat(s, 64)
}

const valueSep = ';'

var ErrSeparatorNotFound = errors.New("separator not found")

type Item struct {
	name  []byte
	value float64
}

func ParseLine(line []byte) (out Item, err error) {
	sep := bytes.IndexByte(line, valueSep)
	if sep == -1 {
		return out, ErrSeparatorNotFound
	}
	_ = line[sep+1:] // reduce amount of bound checks
	out.name = line[:sep]
	out.value, err = ValueToFloat(line[sep+1:])
	return out, err
}

type Info struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int
}

func InfoFromItem(item Item) Info {
	return Info{
		Min:   item.value,
		Max:   item.value,
		Sum:   item.value,
		Count: 1,
	}
}
func (info *Info) Update(item Item) {
	if info.Min > item.value {
		info.Min = item.value
	}
	if info.Max < item.value {
		info.Max = item.value
	}
	info.Sum += item.value
	info.Count += 1
}

func HandleLine(line []byte, dict map[string]Info) error {
	item, err := ParseLine(line)
	if err != nil {
		return fmt.Errorf("failed to parse line: %w", err)
	}
	name := unsafe.String(unsafe.SliceData(item.name), len(item.name))
	info, ok := dict[name]
	if ok {
		info.Update(item)
	} else {
		info = InfoFromItem(item)
	}
	dict[name] = info
	return nil
}

func HandleBuf(buf []byte, dict map[string]Info) (consumed int, err error) {
	for {
		le := bytes.IndexByte(buf[consumed:], '\n')
		if le == -1 {
			debugf("line end not found\n")
			return consumed, nil
		}
		line := buf[consumed : consumed+le]
		err = HandleLine(line, dict)
		if err != nil {
			return consumed, fmt.Errorf("failed to parse line %q: %w", line, err)
		}
		consumed += le + 1
	}
}

func Solve(filename string) error {
	memHint := int(1e4)
	var dict = make(map[string]Info, memHint)

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	pageSize := os.Getpagesize()
	buf := make([]byte, pageSize)
	offt := 0
	for {
		n, err := file.Read(buf[offt:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("failed to read file: %w", err)
		}

		consumed, err := HandleBuf(buf[:offt+n], dict)
		if err != nil {
			return fmt.Errorf("failed to parse buf: %w", err)
		}
		copy(buf, buf[consumed:])
		offt += n - consumed
		debugf("offt=%d buf: %q\n", offt, buf[:offt])
	}
}

func main() {
}
