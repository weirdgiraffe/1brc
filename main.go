package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
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
	name  string
	value float64
}

func ParseLine(line []byte) (out Item, err error) {
	sep := bytes.IndexByte(line, valueSep)
	if sep == -1 {
		return out, ErrSeparatorNotFound
	}

	out.name = string(line[:sep])
	out.value, err = ValueToFloat(line[sep+1:])
	return out, err
}

type Info struct {
	Name  string
	Min   float64
	Max   float64
	Sum   float64
	Count float64
}

func InfoFromItem(item Item) Info {
	return Info{
		Name:  item.name,
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

type InfoStore struct {
	infos []Info
}

func NewInfoStore() *InfoStore {
	return &InfoStore{
		infos: make([]Info, 0, 1e4),
	}
}

func (store *InfoStore) At(i int) *Info {
	return &store.infos[i]
}

func (store *InfoStore) Update(item Item) {
	n := len(store.infos)
	i := sort.Search(n, func(i int) bool {
		return strings.Compare(store.At(i).Name, item.name) >= 0
	})

	if i == n {
		// all items are smaller than item, so add item to the end
		info := InfoFromItem(item)
		store.infos = append(store.infos, info)
		return
	}

	found := store.At(i)
	if found.Name == item.name {
		found.Update(item)
		return
	}

	// insert item at i
	info := InfoFromItem(item)
	store.infos = append(store.infos, Info{})
	copy(store.infos[i+1:], store.infos[i:n])
	store.infos[i] = info
}

func (store *InfoStore) Print() {
	fmt.Print("{")
	for i, info := range store.infos {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s=%.1f/%.1f/%.1f",
			info.Name,
			info.Min,
			info.Sum/info.Count,
			info.Max)
	}
	fmt.Println("}")
}

func HandleLine(line []byte, store *InfoStore) error {
	item, err := ParseLine(line)
	if err != nil {
		return fmt.Errorf("failed to parse line: %w", err)
	}
	store.Update(item)
	return nil
}

func HandleBuf(buf []byte, store *InfoStore) (consumed int, err error) {
	for {
		le := bytes.IndexByte(buf[consumed:], '\n')
		if le == -1 {
			debugf("line end not found\n")
			return consumed, nil
		}
		line := buf[consumed : consumed+le]
		err = HandleLine(line, store)
		if err != nil {
			return consumed, fmt.Errorf("failed to parse line %q: %w", line, err)
		}
		consumed += le + 1
	}
}

func Solve(filename string) error {
	store := NewInfoStore()

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
				fmt.Printf("store has %d items\n", len(store.infos))
				store.Print()
				return nil
			}
			return fmt.Errorf("failed to read file: %w", err)
		}

		consumed, err := HandleBuf(buf[:offt+n], store)
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
