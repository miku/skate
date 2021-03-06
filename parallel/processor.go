// Package parallel implements helpers for fast processing of line oriented inputs.
package parallel

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"runtime"
	"sync"
	"time"
)

// BytesBatch is a slice of byte slices.
type BytesBatch struct {
	b [][]byte
}

// NewBytesBatch creates a new BytesBatch with a given capacity.
func NewBytesBatch() *BytesBatch {
	return NewBytesBatchCapacity(0)
}

// NewBytesBatchCapacity creates a new BytesBatch with a given capacity.
func NewBytesBatchCapacity(cap int) *BytesBatch {
	return &BytesBatch{b: make([][]byte, 0, cap)}
}

// Add adds an element to the batch.
func (bb *BytesBatch) Add(b []byte) {
	bb.b = append(bb.b, b)
}

// Reset empties this batch.
func (bb *BytesBatch) Reset() {
	bb.b = nil
}

// Size returns the number of elements in the batch.
func (bb *BytesBatch) Size() int {
	return len(bb.b)
}

// Slice returns a slice of byte slices.
func (bb *BytesBatch) Slice() [][]byte {
	b := make([][]byte, len(bb.b))
	for i := 0; i < len(bb.b); i++ {
		b[i] = bb.b[i]
	}
	return b
}

// Processor can process lines in parallel.
type Processor struct {
	BatchSize       int
	RecordSeparator byte
	NumWorkers      int
	SkipEmptyLines  bool
	Verbose         bool
	LogFunc         func()
	r               io.Reader
	w               io.Writer
	f               func([]byte) ([]byte, error)
}

// NewProcessor creates a new line processor.
func NewProcessor(r io.Reader, w io.Writer, f func([]byte) ([]byte, error)) *Processor {
	return &Processor{
		BatchSize:       10000,
		RecordSeparator: '\n',
		NumWorkers:      runtime.NumCPU(),
		SkipEmptyLines:  true,
		r:               r,
		w:               w,
		f:               f,
	}
}

// Run starts the workers, crunching through the input.
func (p *Processor) Run() error {
	// wErr signals a worker or writer error. If an error occurs, the items in
	// the queue are still process, just no items are added to the queue. There
	// is only one way to toggle this, from false to true, so we don't care
	// about race conditions here.
	var wErr error

	worker := func(queue chan [][]byte, out chan []byte, f func([]byte) ([]byte, error), wg *sync.WaitGroup) {
		defer wg.Done()
		for batch := range queue {
			for _, b := range batch {
				r, err := f(b)
				if err != nil {
					wErr = err
				}
				out <- r
			}
		}
	}
	writer := func(w io.Writer, bc chan []byte, done chan bool) {
		bw := bufio.NewWriter(w)
		for b := range bc {
			if _, err := bw.Write(b); err != nil {
				wErr = err
			}
		}
		if err := bw.Flush(); err != nil {
			wErr = err
		}
		done <- true
	}
	var (
		queue   = make(chan [][]byte)
		out     = make(chan []byte)
		done    = make(chan bool)
		total   int64
		started = time.Now()
		wg      sync.WaitGroup
		batch   = NewBytesBatchCapacity(p.BatchSize)
		br      = bufio.NewReader(p.r)
	)
	go writer(p.w, out, done)
	for i := 0; i < p.NumWorkers; i++ {
		wg.Add(1)
		go worker(queue, out, p.f, &wg)
	}
	for {
		b, err := br.ReadBytes(p.RecordSeparator)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(bytes.TrimSpace(b)) == 0 && p.SkipEmptyLines {
			continue
		}
		batch.Add(b)
		if batch.Size() == p.BatchSize {
			total += int64(p.BatchSize)
			// To avoid checking on each loop, we only check for worker or
			// write errors here.
			if wErr != nil {
				break
			}
			queue <- batch.Slice()
			batch.Reset()
			if p.Verbose {
				log.Printf("dispatched %d lines (%0.2f lines/s)",
					total, float64(total)/time.Since(started).Seconds())
				if p.LogFunc != nil {
					p.LogFunc()
				}
			}
		}
	}
	queue <- batch.Slice()
	batch.Reset()

	close(queue)
	wg.Wait()
	close(out)
	<-done

	return wErr
}
