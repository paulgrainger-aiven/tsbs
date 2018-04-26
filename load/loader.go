package load

import (
	"bufio"
	"flag"
	"sync"
	"time"
)

const (
	// DefaultBatchSize is the default size of batches to be inserted
	DefaultBatchSize = 10000
)

// Benchmark is an interface that represents the skeleton of a program
// needed to run an insert or load benchmark.
type Benchmark interface {
	Work(*sync.WaitGroup, int)
	Scan(batchSize int, limit int64, br *bufio.Reader) int64
	Close()
}

// CleaningBenchmark is an interface for programs that need to cleanup before
// printing the summary. It should be combined with Benchmark
type CleaningBenchmark interface {
	Benchmark
	Cleanup()
}

// BenchmarkRunner is responsible for initializing and storing common
// flags across all database systems and ultimately running a supplied Benchmark
type BenchmarkRunner struct {
	dbName          string
	batchSize       int
	workers         int
	limit           int64
	doLoad          bool
	reportingPeriod time.Duration
}

var loader = &BenchmarkRunner{}

// GetBenchmarkRunner returns the singleton BenchmarkRunner for use in a benchmark program
// with a batch size of 10000
func GetBenchmarkRunner() *BenchmarkRunner {
	return GetBenchmarkRunnerWithBatchSize(DefaultBatchSize)
}

// GetBenchmarkRunnerWithBatchSize returns the singleton BenchmarkRunner for use in a benchmark program
// with a non-default batch size.
func GetBenchmarkRunnerWithBatchSize(batchSize int) *BenchmarkRunner {
	flag.StringVar(&loader.dbName, "db-name", "benchmark", "Name of database")

	flag.IntVar(&loader.batchSize, "batch-size", batchSize, "Number of items to batch together in a single insert")
	flag.IntVar(&loader.workers, "workers", 1, "Number of parallel clients inserting")
	flag.Int64Var(&loader.limit, "limit", -1, "Number of items to insert (default unlimited).")
	flag.BoolVar(&loader.doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed")
	flag.DurationVar(&loader.reportingPeriod, "reporting-period", 10*time.Second, "Period to report write stats")

	return loader
}

// DatabaseName returns the value of the --db-name flag (name of the database to store data)
func (l *BenchmarkRunner) DatabaseName() string {
	return l.dbName
}

// DoLoad returns the value of the --do-load flag (whether to actually load or not)
func (l *BenchmarkRunner) DoLoad() bool {
	return l.doLoad
}

// NumWorkers returns the value of the --workers flag (how many parallel insert clients there are)
func (l *BenchmarkRunner) NumWorkers() int {
	return l.workers
}

// RunBenchmark takes in a Benchmark b, a bufio.Reader br, and holders for number of metrics and rows
// and uses those to run the load benchmark
func (l *BenchmarkRunner) RunBenchmark(b Benchmark, br *bufio.Reader, metricCount, rowCount *uint64) {
	dr := NewDataReader(l.workers, b)
	dr.Start(br, l.batchSize, l.limit, l.reportingPeriod, metricCount, rowCount)
	switch c := b.(type) {
	case CleaningBenchmark:
		c.Cleanup()
	}
	dr.Summary(l.workers, metricCount, rowCount)
}
