package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

// Store data in the project directory instead of /tmp
const dataDir = "./rivage_data"

func main() {
	// Ensure the data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}

	http.HandleFunc("/data/", func(w http.ResponseWriter, r *http.Request) {
		filename := filepath.Base(r.URL.Path)
		path := filepath.Join(dataDir, filename)

		// Handle Uploads (Workers sending results)
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			f, err := os.Create(path)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer f.Close()
			_, err = io.Copy(f, r.Body)
			if err != nil {
				log.Printf("Upload error: %v", err)
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		// Handle Downloads (Workers fetching data)
		if r.Method == http.MethodGet {
			http.ServeFile(w, r, path)
			return
		}

		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	})

	log.Printf("[DataStore] Listening on :8081. Saving files to %s/", dataDir)
	log.Fatal(http.ListenAndServe(":8081", nil))
}
