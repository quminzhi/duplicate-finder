package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// Based on finder2, we do not pre-define workers but add a processFile routine every time when a regular file is found.
// The potential risk is that the number of file descriptor is limited by OS. We will use a limit channel (
// like a counter) to limit the number of working go routines.
//
// Limit the number of go routines with limit channel:
//
// limits <- true
// defer func() { <-limits }()
//
// Use wait group the track all go routines

type pair struct {
	hash string // md5 hash of the file
	path string // path to the file
}

type pathList []string
type result map[string]pathList // md5 hash to a list of paths to files with the exactly same md5 value

// hashFile will read the file and return the path and md5 value
func hashFile(path string) pair {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		log.Fatal(err)
	}
	hashVal := fmt.Sprintf("%x", hash.Sum(nil))

	return pair{hash: hashVal, path: path}
}

// processFile hashes file and send pair to pairs channel
func processFile(path string, pairs chan<- pair, wg *sync.WaitGroup, limits chan bool) {
	defer wg.Done()

	limits <- true
	defer func() { <-limits }()

	pairs <- hashFile(path)
}

// collectHashes is a collector collecting pairs from workers and sending them back to the master when no pair arrives.
func collectHashes(pairs <-chan pair, results chan<- result) {
	hashes := make(result)
	for p := range pairs {
		hashes[p.hash] = append(hashes[p.hash], p.path)
	}
	results <- hashes
}

// searchTree searches files along the dir. When it is a directory, create a new go routine to solve subdirectory.
// When it is a file, process it with a go routine.
func searchTree(dir string, pairs chan<- pair, wg *sync.WaitGroup, limits chan bool) error {
	defer wg.Done()

	// This is a closure
	visit := func(path string, f os.FileInfo, err error) error {
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		// Ignore dir itself to avoid infinite loop (. in the directory)
		if f.Mode().IsDir() && path != dir {
			wg.Add(1)
			go searchTree(path, pairs, wg, limits)
			// Stop and further walk will be done in go routines created above.
			return filepath.SkipDir
		}

		if f.Mode().IsRegular() && f.Size() > 0 {
			wg.Add(1)
			go processFile(path, pairs, wg, limits)
		}

		return nil
	}

	limits <- true
	defer func() { <-limits }()

	return filepath.Walk(dir, visit)
}

func run(dir string) result {
	workers := 2 * runtime.GOMAXPROCS(0)
	limits := make(chan bool, workers)
	pairs := make(chan pair)
	results := make(chan result)

	wg := new(sync.WaitGroup)

	go collectHashes(pairs, results)

	// See searchTree, once searchTree is called, wg.done() will be called once in the searchTree
	wg.Add(1)
	if err := searchTree(dir, pairs, wg, limits); err != nil {
		log.Fatal(err)
	}
	wg.Wait()

	// After all workers down, close the pairs so that collector knows nothing more to collect and stop reading from
	// pairs channel. Only master knows when all workers are done, so master is responsible for closing the pairs channel.
	// Workers are not able to do it.
	close(pairs)

	return <-results
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: finder <path>")
	}

	hashes := run(os.Args[1])

	for hash, paths := range hashes {
		if len(paths) > 1 {
			fmt.Println(hash[len(hash)-7:], len(paths))
			for _, path := range paths {
				fmt.Println("  ", path)
			}
		}
	}
}
