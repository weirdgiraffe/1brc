package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"

	"golang.org/x/sync/errgroup"
)

// after profiling it appears that strconv.ParseFloat
// is taking way to much time for our use case
func ParseFloat(value []byte) float64 {
	const minus = '-'
	f := 0.0
	m := 1.0
	i := 0
	if value[i] == minus {
		m = -1.0
		i++
	}
	n := len(value)
	for ; i < n-2; i++ {
		f *= 10
		f += float64(value[i] - '0')
	}
	f += float64(value[n-1]-'0') / 10
	return m * f
}

const (
	valueSep = ';'
	endLine  = '\n'
)

var ErrSeparatorNotFound = errors.New("separator not found")

type Item struct {
	name  []byte
	value float64
}

func ParseLine(line []byte) (out Item, err error) {
	sep := indexByte(line, valueSep)
	if sep == -1 {
		return out, ErrSeparatorNotFound
	}

	out.name = line[:sep]
	out.value = ParseFloat(line[sep+1:])
	return out, err
}

type Info struct {
	Min   float64
	Max   float64
	Sum   float64
	Count float64
}

func InfoFromItem(item Item) *Info {
	return &Info{
		Min:   item.value,
		Max:   item.value,
		Sum:   item.value,
		Count: 1,
	}
}
func (info *Info) Update(value float64) {
	info.Sum += value
	info.Count += 1
	if info.Min > value {
		info.Min = value
	}
	if info.Max < value {
		info.Max = value
	}
}

func (info *Info) Merge(other *Info) {
	if info.Min > other.Min {
		info.Min = other.Min
	}
	if info.Max < other.Max {
		info.Max = other.Max
	}
	info.Sum += other.Sum
	info.Count += other.Count
}

type InfoStore struct {
	m map[string]*Info
}

func NewInfoStore() *InfoStore {
	return &InfoStore{
		m: make(map[string]*Info, 1000),
	}
}

func (store *InfoStore) Merge(name string, other *Info) {
	if mine, ok := store.m[name]; ok {
		mine.Merge(other)
	} else {
		store.m[name] = other
	}
}

func (store *InfoStore) Update(item Item) {
	if info, ok := store.m[string(item.name)]; ok {
		info.Update(item.value)
	} else {
		name := string(item.name)
		store.m[name] = InfoFromItem(item)
	}
}

func (store *InfoStore) Print() {
	type InfoWithName struct {
		*Info
		Name string
	}
	l := make([]InfoWithName, 0, len(store.m))
	for name, info := range store.m {
		l = append(l, InfoWithName{
			Info: info,
			Name: name,
		})
	}
	sort.Slice(l, func(i, j int) bool {
		return l[i].Name < l[j].Name
	})

	os.Stdout.Write([]byte{'{'})
	for i, info := range l {
		if i != 0 {
			os.Stdout.Write([]byte{','})
		}
		fmt.Printf("%s=%.1f/%.1f/%.1f",
			info.Name,
			info.Min,
			info.Sum/info.Count,
			info.Max)
	}
	os.Stdout.Write([]byte{'}', '\n'})
}

func HandleLine(line []byte, store *InfoStore) error {
	item, err := ParseLine(line)
	if err != nil {
		return fmt.Errorf("failed to parse line %q: %w", line, err)
	}
	store.Update(item)
	return nil
}

func indexByte(b []byte, c byte) int {
	for i, bc := range b {
		if bc == c {
			return i
		}
	}
	return -1
}

func HandlePage(page *Page, store *InfoStore) error {
	buf := page.b[:page.n]
	consumed := 0

	for {
		le := indexByte(buf[consumed:], '\n')
		if le == -1 {
			break
		}

		err := HandleLine(buf[consumed:consumed+le], store)
		if err != nil {
			return err
		}
		consumed += le + 1
	}

	err := HandleLine(buf[consumed:], store)
	if err != nil {
		return err
	}

	page.n = pageSize
	pagePool.Put(page)
	return nil
}

func DoWork(pages <-chan *Page, store *InfoStore) error {
	for page := range pages {
		err := HandlePage(page, store)
		if err != nil {
			return err
		}
	}
	return nil
}

func MergeStores(l []*InfoStore) *InfoStore {
	store := NewInfoStore()
	for i := range l {
		for name, info := range l[i].m {
			store.Merge(name, info)
		}
	}
	return store
}

var pageSize = os.Getpagesize()
var pagePool = sync.Pool{
	New: func() any {
		p := Page{
			b: make([]byte, pageSize),
			n: pageSize,
		}
		return &p
	},
}

type Page struct {
	b []byte
	n int
}

func Solve(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	workers := runtime.GOMAXPROCS(-1)
	pages := make(chan *Page, 10*workers)
	eg, ectx := errgroup.WithContext(context.Background())

	stores := make([]*InfoStore, workers)
	for i := 0; i < workers; i++ {
		store := NewInfoStore()
		stores[i] = store
		eg.Go(func() error {
			return DoWork(pages, store)
		})
	}

	eg.Go(func() (err error) {
		defer close(pages)
		prefix := make([]byte, 0, 100)
		var n int
		for {
			p := pagePool.Get().(*Page)
			buf := p.b

			copy(buf, prefix)
			offt := len(prefix)
			prefix = prefix[:0]

			n, err = file.Read(buf[offt:])
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
			}

			i := bytes.LastIndexByte(buf[:offt+n], endLine)
			if i != -1 {
				prefix = append(prefix, buf[i+1:offt+n]...)
				p.n = i
			}

			select {
			case <-ectx.Done():
				return ectx.Err()
			case pages <- p:
			}
		}
	})

	err = eg.Wait()
	if err != nil {
		return fmt.Errorf("failed to solve: %w", err)
	}

	store := MergeStores(stores)
	store.Print()
	return nil
}

func main() {
	filename := os.Args[1]
	if err := Solve(filename); err != nil {
		fmt.Fprintf(os.Stderr, "failed to solve: %v\n", err)
		os.Exit(1)
	}
}
