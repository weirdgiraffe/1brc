package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
)

// usage of strconv.ParseFloat is way to expensive
// for this program, so just optimize it for the
// specific format.
// NOTE: it may panic on unexpected input
func ParseFloat(value []byte) (f float64) {
	m := 1.0
	i := 0
	if value[i] == '-' {
		m = -1.0
		i++
	}
	n := len(value)
	for ; i < n-2; i++ {
		f = (f * 10.0) + float64(value[i]-'0')
	}
	return m * (f + float64(value[n-1]-'0')/10.0)
}

const shortLineLen = 200

// bytes.IndexByte seems to have some native implementation
// so it won't get inlined and is slower based on benchamarks
func lineEndIndex(b []byte) int {
	// NOTE: as line must have city name, min start offset is 5 (X;Y.Z)
	for i := 5; i < len(b); i++ {
		if b[i] == '\n' {
			return i
		}
	}
	return -1
}

func ParseLine(line []byte) (name []byte, value float64) {
	// NOTE: normally it's way less digits than letters
	sep := bytes.LastIndexByte(line, ';')
	if sep == -1 {
		panic("value separator not found")
	}
	name = line[:sep]
	value = ParseFloat(line[sep+1:])
	return name, value
}

type Stats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count float64
}

func NewStats(value float64) *Stats {
	return &Stats{
		Min:   value,
		Max:   value,
		Sum:   value,
		Count: 1,
	}
}

func (st *Stats) Update(value float64) {
	if st.Min > value {
		st.Min = value
	}
	if st.Max < value {
		st.Max = value
	}
	st.Sum += value
	st.Count += 1
}

func (st *Stats) Merge(other *Stats) {
	if st.Min > other.Min {
		st.Min = other.Min
	}
	if st.Max < other.Max {
		st.Max = other.Max
	}
	st.Sum += other.Sum
	st.Count += other.Count
}

type Calc struct {
	cities map[string]*Stats
}

func NewCalc() *Calc {
	return &Calc{
		cities: make(map[string]*Stats, 1000),
	}
}

func (c *Calc) Update(name []byte, value float64) {
	if st, ok := c.cities[string(name)]; ok {
		st.Update(value)
	} else {
		name := string(name)
		c.cities[name] = NewStats(value)
	}
}

func (c *Calc) Merge(name string, other *Stats) {
	if mine, ok := c.cities[name]; ok {
		mine.Merge(other)
	} else {
		c.cities[name] = other
	}
}

func (c *Calc) HandleLine(line []byte) {
	c.Update(ParseLine(line))
}

func (c *Calc) HandlePage(page *Page) {
	buf := page.Bytes()

	consumed := 0
	for {
		le := lineEndIndex(buf[consumed:])
		if le == -1 {
			break
		}
		c.HandleLine(buf[consumed : consumed+le])
		consumed += le + 1
	}
	c.HandleLine(buf[consumed:])

	page.Put()
}

func (c *Calc) HandlePages(pages <-chan *Page) {
	for page := range pages {
		c.HandlePage(page)
	}
}

func (c *Calc) WriteResults(w io.Writer) {
	type NamedStats struct {
		*Stats
		Name string
	}
	l := make([]NamedStats, 0, len(c.cities))
	for name, st := range c.cities {
		l = append(l, NamedStats{
			Stats: st,
			Name:  name,
		})
	}
	sort.Slice(l, func(i, j int) bool {
		return l[i].Name < l[j].Name
	})

	w.Write([]byte{'{'})
	for i, st := range l {
		if i != 0 {
			w.Write([]byte{','})
		}
		fmt.Fprintf(w, "%s=%.1f/%.1f/%.1f",
			st.Name,
			st.Min,
			st.Sum/st.Count,
			st.Max)
	}
	w.Write([]byte("}\n"))
}

var pageSize = os.Getpagesize()
var pagePool = sync.Pool{
	New: func() any {
		p := Page{
			buf: make([]byte, pageSize),
			n:   pageSize,
		}
		return &p
	},
}

type Page struct {
	buf []byte
	n   int
}

func GetPage() *Page {
	return pagePool.Get().(*Page)
}

func (p Page) Bytes() []byte {
	return p.buf[:p.n]
}

func (p *Page) Put() {
	p.n = pageSize
	pagePool.Put(p)
}

func ReadPages(r io.Reader, capacity int) <-chan *Page {
	pages := make(chan *Page, capacity)
	go func() {
		defer close(pages)

		prefix := make([]byte, 0, 100)
		for {
			p := GetPage()
			buf := p.buf

			copy(buf, prefix)
			offt := len(prefix)
			prefix = prefix[:0]

			n, err := r.Read(buf[offt:])
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				panic(err)
			}

			i := bytes.LastIndexByte(buf[:offt+n], '\n')
			if i != -1 {
				prefix = append(prefix, buf[i+1:offt+n]...)
				p.n = i
			}

			pages <- p
		}

	}()
	return pages
}

func MergeParts(l []*Calc) *Calc {
	c := NewCalc()
	for i := range l {
		for name, info := range l[i].cities {
			c.Merge(name, info)
		}
	}
	return c
}

func Calculate(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	workers := runtime.GOMAXPROCS(-1)
	pages := ReadPages(file, 10*workers)
	parts := make([]*Calc, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		c := NewCalc()
		parts[i] = c
		go func() {
			defer wg.Done()
			c.HandlePages(pages)
		}()
	}
	wg.Wait()

	c := MergeParts(parts)
	c.WriteResults(os.Stdout)
	return nil
}

func main() {
	filename := os.Args[1]
	if err := Calculate(filename); err != nil {
		fmt.Fprintf(os.Stderr, "failed to solve: %v\n", err)
		os.Exit(1)
	}
}
