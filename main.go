package main

import (
	"context"
	"flag"
	"google.golang.org/api/option"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"./btconfig"
	"./stat"
	"cloud.google.com/go/bigtable"
)

var (
	runFor = flag.Duration("run_for", 5*time.Second,
		"how long to run the load test for; 0 to run forever until SIGTERM")
	scratchTable = flag.String("scratch_table", "loadtest-scratch", "name of table to use; should not already exist")
	poolSize     = flag.Int("pool_size", 1, "size of the gRPC connection pool to use for the data client")
	reqCount     = flag.Int("req_count", 100, "number of concurrent requests")
	keyList			 = flag.String("key_list", "key-list.txt", "list of available keys; stored inside a file.")

	config *btconfig.Config
	client *bigtable.Client
)

func main() {
	var err error
	config, err = btconfig.Load()
	if err != nil {
		log.Fatal(err)
	}
	config.RegisterFlags()

	flag.Parse()
	if err := config.CheckFlags(btconfig.ProjectAndInstanceRequired); err != nil {
		log.Fatal(err)
	}
	if config.Creds != "" {
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", config.Creds)
	}
	if flag.NArg() != 0 {
		flag.Usage()
		os.Exit(1)
	}

	content, err := ioutil.ReadFile(*keyList)
	if err != nil {
		log.Fatal(err)
	}
	rows := strings.Split(string(content), "\n")
	rand.Seed(time.Now().Unix())

	var options []option.ClientOption
	if *poolSize > 1 {
		options = append(options,
			option.WithGRPCConnectionPool(*poolSize))
	}


	log.Printf("Dialing connections...")
	client, err = bigtable.NewClient(context.Background(), config.Project, config.Instance, options...)
	if err != nil {
		log.Fatalf("Making bigtable.Client: %v", err)
	}
	defer client.Close()

	log.Printf("Starting load test... (run for %v)", *runFor)
	tbl := client.Open(*scratchTable)
	sem := make(chan int, *reqCount) // limit the number of requests happening at once

	var reads stats
	stopTime := time.Now().Add(*runFor)

	var wg sync.WaitGroup
	for time.Now().Before(stopTime) || *runFor == 0 {
		sem <- 1
		wg.Add(1)

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			ok := true
			opStart := time.Now()
			var stats *stats
			defer func() {
				stats.Record(ok, time.Since(opStart))
			}()

			row := rows[rand.Intn(len(rows))]

			stats = &reads
			_, err := tbl.ReadRow(context.Background(), row, bigtable.RowFilter(bigtable.LatestNFilter(1)))
			if err != nil {
				log.Printf("Error doing read: %v", err)
				ok = false
			}

		}()
	}
	wg.Wait()

	readsAgg := stat.NewAggregate("reads", reads.ds, reads.tries-reads.ok)
	log.Printf("Reads (%d ok / %d tries):\n%v", reads.ok, reads.tries, readsAgg)

}

var allStats int64 // atomic

type stats struct {
	mu        sync.Mutex
	tries, ok int
	ds        []time.Duration
}

func (s *stats) Record(ok bool, d time.Duration) {
	s.mu.Lock()
	s.tries++
	if ok {
		s.ok++
	}
	s.ds = append(s.ds, d)
	s.mu.Unlock()

	if n := atomic.AddInt64(&allStats, 1); n%1000 == 0 {
		log.Printf("Progress: done %d ops", n)
	}
}
