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
)

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

// processFile serves as a worker reading from paths channel, hashing file, writing the pair to pairs channel,
// and telling master it's done when ready to exit
func processFile(paths <-chan string, pairs chan<- pair, done chan<- bool) {
	for path := range paths {
		pairs <- hashFile(path)
	}
	done <- true
}

// collectHashes is a collector collecting pairs from workers and sending them back to the master when no pair arrives.
func collectHashes(pairs <-chan pair, results chan<- result) {
	hashes := make(result)
	for p := range pairs {
		hashes[p.hash] = append(hashes[p.hash], p.path)
	}
	results <- hashes
}

// searchTree searches files along the dir and write file into the path channel. Return error if walk error
func searchTree(dir string, paths chan<- string) error {
	visit := func(path string, f os.FileInfo, err error) error {
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if f.Mode().IsRegular() && f.Size() > 0 {
			paths <- path
		}

		return nil
	}

	return filepath.Walk(dir, visit)
}

func run(dir string) result {
	workers := 2 * runtime.GOMAXPROCS(0)
	paths := make(chan string)
	pairs := make(chan pair)
	done := make(chan bool)
	results := make(chan result)

	// Start workers
	for i := 0; i < workers; i++ {
		go processFile(paths, pairs, done)
	}

	go collectHashes(pairs, results)

	if err := searchTree(dir, paths); err != nil {
		return nil
	}

	// Close paths so that workers are able to finish reading from paths channel
	close(paths)

	// Inspect how many workers finished
	for i := 0; i < workers; i++ {
		<-done
	}
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
