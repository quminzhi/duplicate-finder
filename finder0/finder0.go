package main

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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

// searchTree searches files along the dir, hashes them, and return the result hash map
func searchTree(dir string) (result, error) {
	hashes := make(result)

	visit := func(path string, f os.FileInfo, err error) error {
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if f.Mode().IsRegular() && f.Size() > 0 {
			h := hashFile(path)
			hashes[h.hash] = append(hashes[h.hash], h.path)
		}

		return nil
	}

	err := filepath.Walk(dir, visit)
	return hashes, err
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: finder <path>")
	}

	hashes, err := searchTree(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	for hash, paths := range hashes {
		if len(paths) > 1 {
			fmt.Println(hash[len(hash)-7:], len(paths))
			for _, path := range paths {
				fmt.Println("  ", path)
			}
		}
	}
}
