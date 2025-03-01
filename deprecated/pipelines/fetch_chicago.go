package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
)

func fetchData(url string, ch chan<- map[string]interface{}, wg *sync.WaitGroup) {
	defer wg.Done()

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error fetching data from %s: %v", url, err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body from %s: %v", url, err)
		return
	}

	var data []map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		log.Printf("Error unmarshalling JSON from %s: %v", url, err)
		return
	}

	if len(data) > 0 {
		ch <- map[string]interface{}{"url": url, "data": data} // Ensure data is passed correctly
	}
}
