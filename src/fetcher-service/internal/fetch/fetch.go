package fetch

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"
)

func FetchData(url string, ch chan<- map[string]interface{}) {
	// The Taxi Trips dataset takes a long time to fetch, so this may need to be increased
	client := &http.Client{
		Timeout: 300 * time.Second, // Set a timeout for the HTTP request
	}

	// Fetch data from the URL
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("Error fetching data from %s: %v", url, err)
		return
	}
	defer resp.Body.Close()

	// Check the status code of the response
	if resp.StatusCode != http.StatusOK {
		log.Printf("Error fetching data from %s: received status code %d", url, resp.StatusCode)
		return
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body from %s: %v", url, err)
		return
	}

	// Unmarshal the JSON data
	var data []map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		log.Printf("Error unmarshalling JSON from %s: %v", url, err)
		return
	}

	// Send the data over the channel
	if len(data) > 0 {
		ch <- map[string]interface{}{"url": url, "data": data}
	} else {
		ch <- map[string]interface{}{"url": url, "data": nil}
	}
	close(ch) // Close the channel after sending data
}
