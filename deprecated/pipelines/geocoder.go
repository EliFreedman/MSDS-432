package main

import (
	"fmt"
	"log"
	"os"

	"github.com/kelvins/geocoder"
)

func getZipCode(lat, lng float64) (string, error) {

	geocoder.ApiKey = os.Getenv("GEOCODER_API_KEY")
	if geocoder.ApiKey == "" {
		log.Fatal("GEOCODER_API_KEY is not set")
	}

	location := geocoder.Location{
		Latitude:  lat,
		Longitude: lng,
	}

	addresses, err := geocoder.GeocodingReverse(location)
	if err != nil {
		return "", err
	}

	if len(addresses) > 0 {
		address := addresses[0]
		zip_code := address.PostalCode
		if zip_code != "" {
			return zip_code, nil
		}
	}

	return "", fmt.Errorf("postal code not found")
}
