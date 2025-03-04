package transform

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/kelvins/geocoder"
)

type TaxiTripsJsonRecords []struct {
	Trip_id                    string    `json:"trip_id"`
	Trip_start_timestamp       time.Time `json:"trip_start_timestamp"`
	Trip_end_timestamp         time.Time `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string    `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string    `json:"pickup_centroid_longitude"`
	Pickup_community_area      string    `json:"pickup_community_area"`
	Dropoff_centroid_latitude  string    `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string    `json:"dropoff_centroid_longitude"`
	Dropoff_community_area     string    `json:"dropoff_community_area"`
	Pickup_zipcode             string    `json:"pickup_zipcode"`
	Dropoff_zipcode            string    `json:"dropoff_zipcode"`
}

type BuildingPermitsJsonRecords []struct {
	Id                     string    `json:"id"`
	Permit_status          string    `json:"permit_status"`
	Permit_type            string    `json:"permit_type"`
	Review_type            string    `json:"review_type"`
	Application_start_date time.Time `json:"application_start_date"`
	Issue_date             time.Time `json:"issue_date"`
	Work_type              string    `json:"work_type"`
	Total_fee              float64   `json:"total_fee"`
	Reported_cost          string    `json:"reported_cost"`
	Community_area         string    `json:"community_area"`
	Latitude               string    `json:"latitude"`
	Longitude              string    `json:"longitude"`
	Zipcode                string    `json:"zipcode"`
}

type TransportationTripsJsonRecords []struct {
	Trip_id                    string    `json:"trip_id"`
	Trip_start_timestamp       time.Time `json:"trip_start_timestamp"`
	Trip_end_timestamp         time.Time `json:"trip_end_timestamp"`
	Pickup_census_tract        string    `json:"pickup_census_tract"`
	Dropoff_census_tract       string    `json:"dropoff_census_tract"`
	Pickup_community_area      string    `json:"pickup_community_area"`
	Dropoff_community_area     string    `json:"dropoff_community_area"`
	Pickup_centroid_latitude   string    `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string    `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string    `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string    `json:"dropoff_centroid_longitude"`
	Pickup_zipcode             string    `json:"pickup_zipcode"`
	Dropoff_zipcode            string    `json:"dropoff_zipcode"`
}

// parseTime takes a string and returns time.Time
func parseTime(value string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05.000",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000Z07:00",
		"2006-01-02T15:04:05Z07:00",
	}

	var t time.Time
	var err error
	for _, format := range formats {
		t, err = time.Parse(format, value)
		if err == nil {
			return t, nil
		}
	}

	// Return zero value if parsing fails
	return time.Time{}, fmt.Errorf("invalid time format: %v", value)
}

func TransformData(message []byte, source string) (interface{}, error) {

	geocoder.ApiKey = os.Getenv("GEOCODER_API_KEY")
	if geocoder.ApiKey == "" {
		log.Fatal("GEOCODER_API_KEY is not set")
	}

	// Unmarshal the message
	var data []map[string]interface{}
	err := json.Unmarshal(message, &data)
	if err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Convert []map[string]interface{} → []interface{}
	dataAsInterface := make([]interface{}, len(data))
	for i, v := range data {
		dataAsInterface[i] = v
	}

	switch source {
	case "taxi_trips":
		return transformTaxiTrips(dataAsInterface)
	case "covid_cases":
		return transformCovidCases(data)
	case "covid_vulnerability_index":
		return transformCovidVI(data)
	case "building_permits":
		return transformBuildingPermits(dataAsInterface)
	case "census_data":
		return transformCensusData(data)
	case "transportation_trips":
		return transformTransportationTrips(dataAsInterface)
	case "public_health_statistics":
		return transformPHS(data)
	default:
		return nil, fmt.Errorf("unknown data source: %s", source)
	}
}

func transformTaxiTrips(data interface{}) (TaxiTripsJsonRecords, error) {
	var records TaxiTripsJsonRecords
	var droppedRecords int

	for row := 0; row < len(data.([]interface{})); row++ {
		record := data.([]interface{})[row].(map[string]interface{})

		// Extract pickup latitude and longitude
		pickupLat, _ := strconv.ParseFloat(record["pickup_centroid_latitude"].(string), 64)
		pickupLong, _ := strconv.ParseFloat(record["pickup_centroid_longitude"].(string), 64)
		pickup_location := geocoder.Location{
			Latitude:  pickupLat,
			Longitude: pickupLong,
		}

		// Extract dropoff latitude and longitude
		dropoffLat, _ := strconv.ParseFloat(record["dropoff_centroid_latitude"].(string), 64)
		dropoffLong, _ := strconv.ParseFloat(record["dropoff_centroid_longitude"].(string), 64)
		dropoff_location := geocoder.Location{
			Latitude:  dropoffLat,
			Longitude: dropoffLong,
		}

		pickupAddress, _ := geocoder.GeocodingReverse(pickup_location)
		dropoffAddress, _ := geocoder.GeocodingReverse(dropoff_location)

		// Handling locations that could not resolve addresses
		if len(pickupAddress) == 0 {
			log.Printf("No results found for pickup at latitude : %f and longitude : %f \n", pickupLat, pickupLong)
			droppedRecords++
			continue
		}
		if len(dropoffAddress) == 0 {
			log.Printf("No results found for dropoff at latitude : %f and longitude : %f \n", dropoffLat, dropoffLong)
			droppedRecords++
			continue
		}

		pickup_zipcode := pickupAddress[0].PostalCode
		dropoff_zipcode := dropoffAddress[0].PostalCode

		// Parse the timestamps from strings to time.Time
		tripStartTimestamp, err := parseTime(record["trip_start_timestamp"].(string))
		if err != nil {
			return nil, fmt.Errorf("error parsing trip_start_timestamp: %v", err)
		}
		tripEndTimestamp, err := parseTime(record["trip_end_timestamp"].(string))
		if err != nil {
			return nil, fmt.Errorf("error parsing trip_end_timestamp: %v", err)
		}

		// Create a cleaned trip record
		trip := struct {
			Trip_id                    string    `json:"trip_id"`
			Trip_start_timestamp       time.Time `json:"trip_start_timestamp"`
			Trip_end_timestamp         time.Time `json:"trip_end_timestamp"`
			Pickup_centroid_latitude   string    `json:"pickup_centroid_latitude"`
			Pickup_centroid_longitude  string    `json:"pickup_centroid_longitude"`
			Pickup_community_area      string    `json:"pickup_community_area"`
			Dropoff_centroid_latitude  string    `json:"dropoff_centroid_latitude"`
			Dropoff_centroid_longitude string    `json:"dropoff_centroid_longitude"`
			Dropoff_community_area     string    `json:"dropoff_community_area"`
			Pickup_zipcode             string    `json:"pickup_zipcode"`
			Dropoff_zipcode            string    `json:"dropoff_zipcode"`
		}{
			Trip_id:                    record["trip_id"].(string),
			Trip_start_timestamp:       tripStartTimestamp,
			Trip_end_timestamp:         tripEndTimestamp,
			Pickup_centroid_latitude:   record["pickup_centroid_latitude"].(string),
			Pickup_centroid_longitude:  record["pickup_centroid_longitude"].(string),
			Pickup_community_area:      record["pickup_community_area"].(string),
			Dropoff_centroid_latitude:  record["dropoff_centroid_latitude"].(string),
			Dropoff_centroid_longitude: record["dropoff_centroid_longitude"].(string),
			Dropoff_community_area:     record["dropoff_community_area"].(string),
			Pickup_zipcode:             pickup_zipcode,
			Dropoff_zipcode:            dropoff_zipcode,
		}

		records = append(records, trip)
	}

	return records, nil
}

func transformCovidCases(data interface{}) (interface{}, error) {
	// No transformation needed for the COVID cases data
	return data, nil
}

func transformCovidVI(data interface{}) (interface{}, error) {
	// No transformation needed for the COVID vulnerability index data
	return data, nil
}

func transformBuildingPermits(data interface{}) (BuildingPermitsJsonRecords, error) {
	var records BuildingPermitsJsonRecords
	var droppedRecords int

	for row := 0; row < len(data.([]interface{})); row++ {
		record := data.([]interface{})[row].(map[string]interface{})

		// Extract latitude and longitude
		lat, _ := strconv.ParseFloat(record["latitude"].(string), 64)
		long, _ := strconv.ParseFloat(record["longitude"].(string), 64)
		location := geocoder.Location{
			Latitude:  lat,
			Longitude: long,
		}

		address, _ := geocoder.GeocodingReverse(location)

		// Handling locations that could not resolve addresses
		if len(address) == 0 {
			log.Printf("No results found for latitude : %f and longitude : %f \n", lat, long)
			droppedRecords++
			continue
		}

		zipcode := address[0].PostalCode

		applicationStartDate, err := parseTime(record["application_start_date"].(string))
		if err != nil {
			return nil, fmt.Errorf("error parsing application_start_date: %v", err)
		}
		issueDate, err := parseTime(record["issue_date"].(string))
		if err != nil {
			return nil, fmt.Errorf("error parsing issue_date: %v", err)
		}

		// Create a cleaned building permit record
		permit := struct {
			Id                     string    `json:"id"`
			Permit_status          string    `json:"permit_status"`
			Permit_type            string    `json:"permit_type"`
			Review_type            string    `json:"review_type"`
			Application_start_date time.Time `json:"application_start_date"`
			Issue_date             time.Time `json:"issue_date"`
			Work_type              string    `json:"work_type"`
			Total_fee              float64   `json:"total_fee"`
			Reported_cost          string    `json:"reported_cost"`
			Community_area         string    `json:"community_area"`
			Latitude               string    `json:"latitude"`
			Longitude              string    `json:"longitude"`
			Zipcode                string    `json:"zipcode"`
		}{
			Id:                     record["id"].(string),
			Permit_status:          record["permit_status"].(string),
			Permit_type:            record["permit_type"].(string),
			Review_type:            record["review_type"].(string),
			Application_start_date: applicationStartDate,
			Issue_date:             issueDate,
			Work_type:              record["work_type"].(string),
			Total_fee:              record["total_fee"].(float64),
			Reported_cost:          record["reported_cost"].(string),
			Community_area:         record["community_area"].(string),
			Latitude:               record["latitude"].(string),
			Longitude:              record["longitude"].(string),
			Zipcode:                zipcode,
		}

		records = append(records, permit)
	}

	return records, nil
}

func transformCensusData(data interface{}) (interface{}, error) {
	// No transformation needed for the census data
	return data, nil
}

func transformTransportationTrips(data interface{}) (TransportationTripsJsonRecords, error) {
	var records TransportationTripsJsonRecords
	var droppedRecords int

	for row := 0; row < len(data.([]interface{})); row++ {
		record := data.([]interface{})[row].(map[string]interface{})

		// Extract pickup latitude and longitude
		pickupLat, _ := strconv.ParseFloat(record["pickup_centroid_latitude"].(string), 64)
		pickupLong, _ := strconv.ParseFloat(record["pickup_centroid_longitude"].(string), 64)
		pickup_location := geocoder.Location{
			Latitude:  pickupLat,
			Longitude: pickupLong,
		}

		// Extract dropoff latitude and longitude
		dropoffLat, _ := strconv.ParseFloat(record["dropoff_centroid_latitude"].(string), 64)
		dropoffLong, _ := strconv.ParseFloat(record["dropoff_centroid_longitude"].(string), 64)
		dropoff_location := geocoder.Location{
			Latitude:  dropoffLat,
			Longitude: dropoffLong,
		}

		pickupAddress, _ := geocoder.GeocodingReverse(pickup_location)
		dropoffAddress, _ := geocoder.GeocodingReverse(dropoff_location)

		// Handling locations that could not resolve addresses
		if len(pickupAddress) == 0 {
			log.Printf("No results found for pickup at latitude : %f and longitude : %f \n", pickupLat, pickupLong)
			droppedRecords++
			continue
		}
		if len(dropoffAddress) == 0 {
			log.Printf("No results found for dropoff at latitude : %f and longitude : %f \n", dropoffLat, dropoffLong)
			droppedRecords++
			continue
		}

		pickup_zipcode := pickupAddress[0].PostalCode
		dropoff_zipcode := dropoffAddress[0].PostalCode

		// Parse the timestamps from strings to time.Time
		tripStartTimestamp, err := parseTime(record["trip_start_timestamp"].(string))
		if err != nil {
			return nil, fmt.Errorf("error parsing trip_start_timestamp: %v", err)
		}
		tripEndTimestamp, err := parseTime(record["trip_end_timestamp"].(string))
		if err != nil {
			return nil, fmt.Errorf("error parsing trip_end_timestamp: %v", err)
		}

		// Create a cleaned transportation trip record
		trip := struct {
			Trip_id                    string    `json:"trip_id"`
			Trip_start_timestamp       time.Time `json:"trip_start_timestamp"`
			Trip_end_timestamp         time.Time `json:"trip_end_timestamp"`
			Pickup_census_tract        string    `json:"pickup_census_tract"`
			Dropoff_census_tract       string    `json:"dropoff_census_tract"`
			Pickup_community_area      string    `json:"pickup_community_area"`
			Dropoff_community_area     string    `json:"dropoff_community_area"`
			Pickup_centroid_latitude   string    `json:"pickup_centroid_latitude"`
			Pickup_centroid_longitude  string    `json:"pickup_centroid_longitude"`
			Dropoff_centroid_latitude  string    `json:"dropoff_centroid_latitude"`
			Dropoff_centroid_longitude string    `json:"dropoff_centroid_longitude"`
			Pickup_zipcode             string    `json:"pickup_zipcode"`
			Dropoff_zipcode            string    `json:"dropoff_zipcode"`
		}{
			Trip_id:                    record["trip_id"].(string),
			Trip_start_timestamp:       tripStartTimestamp,
			Trip_end_timestamp:         tripEndTimestamp,
			Pickup_census_tract:        record["pickup_census_tract"].(string),
			Dropoff_census_tract:       record["dropoff_census_tract"].(string),
			Pickup_community_area:      record["pickup_community_area"].(string),
			Dropoff_community_area:     record["dropoff_community_area"].(string),
			Pickup_centroid_latitude:   record["pickup_centroid_latitude"].(string),
			Pickup_centroid_longitude:  record["pickup_centroid_longitude"].(string),
			Dropoff_centroid_latitude:  record["dropoff_centroid_latitude"].(string),
			Dropoff_centroid_longitude: record["dropoff_centroid_longitude"].(string),
			Pickup_zipcode:             pickup_zipcode,
			Dropoff_zipcode:            dropoff_zipcode,
		}

		records = append(records, trip)
	}

	return records, nil
}

func transformPHS(data interface{}) (interface{}, error) {
	// No transformation needed for the public health statistics data
	return data, nil
}
