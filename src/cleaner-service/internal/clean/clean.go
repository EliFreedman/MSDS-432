package clean

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"
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
}

type CovidCasesJsonRecords []struct {
	Zip_code                           string    `json:"zip_code"`
	Week_number                        string    `json:"week_number"`
	Week_start                         time.Time `json:"week_start"`
	Week_end                           time.Time `json:"week_end"`
	Cases_weekly                       int64     `json:"cases_weekly"`
	Cases_cumulative                   int64     `json:"cases_cumulative"`
	Case_rate_weekly                   float64   `json:"case_rate_weekly"`
	Case_rate_cumulative               float64   `json:"case_rate_cumulative"`
	Tests_weekly                       int64     `json:"tests_weekly"`
	Tests_cumulative                   int64     `json:"tests_cumulative"`
	Test_rate_weekly                   float64   `json:"test_rate_weekly"`
	Test_rate_cumulative               float64   `json:"test_rate_cumulative"`
	Percent_tested_positive_weekly     float64   `json:"percent_tested_positive_weekly"`
	Percent_tested_positive_cumulative float64   `json:"percent_tested_positive_cumulative"`
	Deaths_weekly                      int64     `json:"deaths_weekly"`
	Deaths_cumulative                  int64     `json:"deaths_cumulative"`
	Death_rate_weekly                  float64   `json:"death_rate_weekly"`
	Death_rate_cumulative              float64   `json:"death_rate_cumulative"`
	Population                         int64     `json:"population"`
	Row_id                             string    `json:"row_id"`
	Latitude                           float64   `json:"latitude"`
	Longitude                          float64   `json:"longitude"`
}

type CovidVIJsonRecords []struct {
	Community_area_or_zip string `json:"community_area_or_zip"`
	Community_area_name   string `json:"community_area_name"`
	CCVI_category         string `json:"ccvi_category"`
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
}

type CensusDataJsonRecords []struct {
	Community_area_number            string  `json:"community_area_number"`
	Community_area_name              string  `json:"community_area_name"`
	Percent_households_below_poverty float64 `json:"percent_households_below_poverty"`
	Percent_aged_16_unemployed       float64 `json:"percent_aged_16_unemployed"`
	Per_capita_income                int64   `json:"per_capita_income"`
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
}

type PHSJsonRecords []struct {
	Community_area      string  `json:"community_area"`
	Community_area_name string  `json:"community_area_name"`
	Below_poverty_level float64 `json:"below_poverty_level"`
	Per_capita_income   float64 `json:"per_capita_income"`
	Unemployment        float64 `json:"unemployment"`
}

// CleanData processes and cleans the data based on its source
func CleanData(data []byte, source string) (interface{}, error) {
	var raw map[string]interface{}
	err := json.Unmarshal(data, &raw)
	if err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	log.Printf("Cleaning data from source: %s", source)

	switch source {
	case "taxi_trips":
		return cleanTaxiTrips(raw)
	case "covid_cases":
		return cleanCovidCases(raw)
	case "covid_vulnerability_index":
		return cleanCovidVI(raw)
	case "building_permits":
		return cleanBuildingPermits(raw)
	case "census_data":
		return cleanCensusData(raw)
	case "transportation_trips":
		return cleanTransportationTrips(raw)
	case "public_health_statistics":
		return cleanPHS(raw)
	default:
		return nil, fmt.Errorf("unknown data source: %s", source)
	}
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

// parseInt takes an interface and returns int64
func parseInt(value interface{}) (int64, error) {
	switch v := value.(type) {
	case string:
		if v == "" {
			return -1, nil // Return -1 for missing value
		}
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("invalid int value: %v", v)
		}
		return int64(i), nil
	case int:
		return int64(v), nil
	case float64: // Handle JSON decoding where numbers are float64
		return int64(v), nil
	default:
		return 0, fmt.Errorf("invalid type for int conversion: %T", v)
	}
}

// parseFloat takes an interface and returns float64
func parseFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case string:
		if v == "" {
			return -1, nil // Return -1 for missing value
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid float value: %v", v)
		}
		return f, nil
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("invalid type for float conversion: %T", v)
	}
}

// parseString takes an interface and returns string
func parseString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		if v == "" {
			return "", nil // Return empty string for missing value
		}
		return v, nil
	default:
		return "", fmt.Errorf("invalid type for string conversion: %T", v)
	}
}

func cleanTaxiTrips(data map[string]interface{}) (TaxiTripsJsonRecords, error) {
	log.Printf("Applying cleaning rules for Taxi Trips: %+v", data)
	var records TaxiTripsJsonRecords
	var droppedRecords int

	for row := 0; row < len(data["data"].([]interface{})); row++ {
		record := data["data"].([]interface{})[row]
		recMap := record.(map[string]interface{})

		// Clean each field
		tripID, err := parseString(recMap["trip_id"])
		if err != nil {
			log.Printf("Missing or invalid trip_id: %v", recMap["trip_id"])
			droppedRecords++
			continue
		}
		tripStartTimestampStr, ok := recMap["trip_start_timestamp"].(string)
		if !ok {
			log.Printf("Missing or invalid trip_start_timestamp: %v", recMap["trip_start_timestamp"])
			droppedRecords++
			continue
		}
		tripStartTimestamp, err := parseTime(tripStartTimestampStr)
		if err != nil {
			log.Printf("Failed to parse trip_start_timestamp: %v", err)
			droppedRecords++
			continue
		}
		tripEndTimestampStr, ok := recMap["trip_end_timestamp"].(string)
		if !ok {
			log.Printf("Missing or invalid trip_end_timestamp: %v", recMap["trip_end_timestamp"])
			droppedRecords++
			continue
		}
		tripEndTimestamp, err := parseTime(tripEndTimestampStr)
		if err != nil {
			log.Printf("Failed to parse trip_end_timestamp: %v", err)
			droppedRecords++
			continue
		}
		pickupCentroidLatitude, err := parseString(recMap["pickup_centroid_latitude"])
		if err != nil {
			log.Printf("Missing or invalid pickup_centroid_latitude: %v", recMap["pickup_centroid_latitude"])
			droppedRecords++
			continue
		}
		pickupCentroidLongitude, err := parseString(recMap["pickup_centroid_longitude"])
		if err != nil {
			log.Printf("Missing or invalid pickup_centroid_longitude: %v", recMap["pickup_centroid_longitude"])
			droppedRecords++
			continue
		}
		pickupCommunityArea, err := parseString(recMap["pickup_community_area"])
		if err != nil {
			log.Printf("Missing or invalid pickup_community_area: %v", recMap["pickup_community_area"])
			droppedRecords++
			continue
		}
		dropoffCentroidLatitude, err := parseString(recMap["dropoff_centroid_latitude"])
		if err != nil {
			log.Printf("Missing or invalid dropoff_centroid_latitude: %v", recMap["dropoff_centroid_latitude"])
			droppedRecords++
			continue
		}
		dropoffCentroidLongitude, err := parseString(recMap["dropoff_centroid_longitude"])
		if err != nil {
			log.Printf("Missing or invalid dropoff_centroid_longitude: %v", recMap["dropoff_centroid_longitude"])
			droppedRecords++
			continue
		}
		dropoffCommunityArea, err := parseString(recMap["dropoff_community_area"])
		if err != nil {
			log.Printf("Missing or invalid dropoff_community_area: %v", recMap["dropoff_community_area"])
			droppedRecords++
			continue
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
		}{
			Trip_id:                    tripID,
			Trip_start_timestamp:       tripStartTimestamp,
			Trip_end_timestamp:         tripEndTimestamp,
			Pickup_centroid_latitude:   pickupCentroidLatitude,
			Pickup_centroid_longitude:  pickupCentroidLongitude,
			Pickup_community_area:      pickupCommunityArea,
			Dropoff_centroid_latitude:  dropoffCentroidLatitude,
			Dropoff_centroid_longitude: dropoffCentroidLongitude,
			Dropoff_community_area:     dropoffCommunityArea,
		}
		records = append(records, trip)
	}
	log.Printf("Cleaned Taxi Trips Records: %+v", records)
	log.Printf("Number of dropped records: %d", droppedRecords)
	return records, nil
}

func cleanCovidCases(data map[string]interface{}) (CovidCasesJsonRecords, error) {
	log.Printf("Applying cleaning rules for Covid Cases: %+v", data)
	var records CovidCasesJsonRecords
	var droppedRecords int

	for row := 0; row < len(data["data"].([]interface{})); row++ {
		record := data["data"].([]interface{})[row]
		recMap := record.(map[string]interface{})

		// Clean each field
		zipCode, err := parseString(recMap["zip_code"])
		if err != nil {
			log.Printf("Missing or invalid zip_code: %v", recMap["zip_code"])
			droppedRecords++
			continue
		}
		weekNumber, err := parseString(recMap["week_number"])
		if err != nil {
			log.Printf("Missing or invalid week_number: %v", recMap["week_number"])
			droppedRecords++
			continue
		}
		weekStartStr, ok := recMap["week_start"].(string)
		if !ok {
			log.Printf("Missing or invalid week_start: %v", recMap["week_start"])
			droppedRecords++
			continue
		}
		weekStart, err := parseTime(weekStartStr)
		if err != nil {
			log.Printf("Failed to parse week_start: %v", err)
			droppedRecords++
			continue
		}
		weekEndStr, ok := recMap["week_end"].(string)
		if !ok {
			log.Printf("Missing or invalid week_end: %v", recMap["week_end"])
			droppedRecords++
			continue
		}
		weekEnd, err := parseTime(weekEndStr)
		if err != nil {
			log.Printf("Failed to parse week_end: %v", err)
			droppedRecords++
			continue
		}
		casesWeekly, err := parseInt(recMap["cases_weekly"])
		if err != nil {
			log.Printf("Failed to convert cases_weekly: %v", err)
			droppedRecords++
			continue
		}
		casesCumulative, err := parseInt(recMap["cases_cumulative"])
		if err != nil {
			log.Printf("Failed to convert cases_cumulative: %v", err)
			droppedRecords++
			continue
		}
		caseRateWeekly, err := parseFloat(recMap["case_rate_weekly"])
		if err != nil {
			log.Printf("Failed to convert case_rate_weekly: %v", err)
			droppedRecords++
			continue
		}
		caseRateCumulative, err := parseFloat(recMap["case_rate_cumulative"])
		if err != nil {
			log.Printf("Failed to convert case_rate_cumulative: %v", err)
			droppedRecords++
			continue
		}
		testsWeekly, err := parseInt(recMap["tests_weekly"])
		if err != nil {
			log.Printf("Failed to convert tests_weekly: %v", err)
			droppedRecords++
			continue
		}
		testsCumulative, err := parseInt(recMap["tests_cumulative"])
		if err != nil {
			log.Printf("Failed to convert tests_cumulative: %v", err)
			droppedRecords++
			continue
		}
		testRateWeekly, err := parseFloat(recMap["test_rate_weekly"])
		if err != nil {
			log.Printf("Failed to convert test_rate_weekly: %v", err)
			droppedRecords++
			continue
		}
		testRateCumulative, err := parseFloat(recMap["test_rate_cumulative"])
		if err != nil {
			log.Printf("Failed to convert test_rate_cumulative: %v", err)
			droppedRecords++
			continue
		}
		percentTestedPositiveWeekly, err := parseFloat(recMap["percent_tested_positive_weekly"])
		if err != nil {
			log.Printf("Failed to convert percent_tested_positive_weekly: %v", err)
			droppedRecords++
			continue
		}
		percentTestedPositiveCumulative, err := parseFloat(recMap["percent_tested_positive_cumulative"])
		if err != nil {
			log.Printf("Failed to convert percent_tested_positive_cumulative: %v", err)
			droppedRecords++
			continue
		}
		deathsWeekly, err := parseInt(recMap["deaths_weekly"])
		if err != nil {
			log.Printf("Failed to convert deaths_weekly: %v", err)
			droppedRecords++
			continue
		}
		deathsCumulative, err := parseInt(recMap["deaths_cumulative"])
		if err != nil {
			log.Printf("Failed to convert deaths_cumulative: %v", err)
			droppedRecords++
			continue
		}
		deathRateWeekly, err := parseFloat(recMap["death_rate_weekly"])
		if err != nil {
			log.Printf("Failed to convert death_rate_weekly: %v", err)
			droppedRecords++
			continue
		}
		deathRateCumulative, err := parseFloat(recMap["death_rate_cumulative"])
		if err != nil {
			log.Printf("Failed to convert death_rate_cumulative: %v", err)
			droppedRecords++
			continue
		}
		population, err := parseInt(recMap["population"])
		if err != nil {
			log.Printf("Failed to convert population: %v", err)
			droppedRecords++
			continue
		}
		rowID, err := parseString(recMap["row_id"])
		if err != nil {
			log.Printf("Missing or invalid row_id: %v", recMap["row_id"])
			droppedRecords++
			continue
		}
		location, ok := recMap["zip_code_location"].(map[string]interface{})
		if !ok {
			log.Printf("Missing or invalid location: %v", recMap["location"])
			droppedRecords++
			continue
		}
		coordinates, ok := location["coordinates"].([]interface{})
		if !ok || len(coordinates) != 2 {
			log.Printf("Invalid coordinates in location: %v", location["coordinates"])
			droppedRecords++
			continue
		}
		latitude, err := parseFloat(coordinates[1])
		if err != nil {
			log.Printf("Invalid latitude in coordinates: %v", coordinates[1])
			droppedRecords++
			continue
		}
		longitude, err := parseFloat(coordinates[0])
		if err != nil {
			log.Printf("Invalid longitude in coordinates: %v", coordinates[0])
			droppedRecords++
			continue
		}

		// Create a cleaned Covid Cases record
		covid_cases := struct {
			Zip_code                           string    `json:"zip_code"`
			Week_number                        string    `json:"week_number"`
			Week_start                         time.Time `json:"week_start"`
			Week_end                           time.Time `json:"week_end"`
			Cases_weekly                       int64     `json:"cases_weekly"`
			Cases_cumulative                   int64     `json:"cases_cumulative"`
			Case_rate_weekly                   float64   `json:"case_rate_weekly"`
			Case_rate_cumulative               float64   `json:"case_rate_cumulative"`
			Tests_weekly                       int64     `json:"tests_weekly"`
			Tests_cumulative                   int64     `json:"tests_cumulative"`
			Test_rate_weekly                   float64   `json:"test_rate_weekly"`
			Test_rate_cumulative               float64   `json:"test_rate_cumulative"`
			Percent_tested_positive_weekly     float64   `json:"percent_tested_positive_weekly"`
			Percent_tested_positive_cumulative float64   `json:"percent_tested_positive_cumulative"`
			Deaths_weekly                      int64     `json:"deaths_weekly"`
			Deaths_cumulative                  int64     `json:"deaths_cumulative"`
			Death_rate_weekly                  float64   `json:"death_rate_weekly"`
			Death_rate_cumulative              float64   `json:"death_rate_cumulative"`
			Population                         int64     `json:"population"`
			Row_id                             string    `json:"row_id"`
			Latitude                           float64   `json:"latitude"`
			Longitude                          float64   `json:"longitude"`
		}{
			Zip_code:                           zipCode,
			Week_number:                        weekNumber,
			Week_start:                         weekStart,
			Week_end:                           weekEnd,
			Cases_weekly:                       casesWeekly,
			Cases_cumulative:                   casesCumulative,
			Case_rate_weekly:                   caseRateWeekly,
			Case_rate_cumulative:               caseRateCumulative,
			Tests_weekly:                       testsWeekly,
			Tests_cumulative:                   testsCumulative,
			Test_rate_weekly:                   testRateWeekly,
			Test_rate_cumulative:               testRateCumulative,
			Percent_tested_positive_weekly:     percentTestedPositiveWeekly,
			Percent_tested_positive_cumulative: percentTestedPositiveCumulative,
			Deaths_weekly:                      deathsWeekly,
			Deaths_cumulative:                  deathsCumulative,
			Death_rate_weekly:                  deathRateWeekly,
			Death_rate_cumulative:              deathRateCumulative,
			Population:                         population,
			Row_id:                             rowID,
			Latitude:                           latitude,
			Longitude:                          longitude,
		}
		records = append(records, covid_cases)
	}
	log.Printf("Cleaned Covid Cases Records: %+v", records)
	log.Printf("Number of dropped records: %d", droppedRecords)
	return records, nil
}

func cleanCovidVI(data map[string]interface{}) (CovidVIJsonRecords, error) {
	log.Printf("Applying cleaning rules for Covid Vulnerability Index: %+v", data)
	var records CovidVIJsonRecords
	var droppedRecords int

	for row := 0; row < len(data["data"].([]interface{})); row++ {
		record := data["data"].([]interface{})[row]
		recMap := record.(map[string]interface{})

		// Clean each field
		communityAreaOrZip, err := parseString(recMap["community_area_or_zip"])
		if err != nil {
			log.Printf("Missing or invalid community_area_or_zip: %v", recMap["community_area_or_zip"])
			droppedRecords++
			continue
		}
		communityAreaName, err := parseString(recMap["community_area_name"])
		if err != nil {
			log.Printf("Missing or invalid community_area_name: %v", recMap["community_area_name"])
			droppedRecords++
			continue
		}
		ccviCategory, err := parseString(recMap["ccvi_category"])
		if err != nil {
			log.Printf("Missing or invalid ccvi_category: %v", recMap["ccvi_category"])
			droppedRecords++
			continue
		}

		// Create a cleaned Covid VI record
		covidVI := struct {
			Community_area_or_zip string `json:"community_area_or_zip"`
			Community_area_name   string `json:"community_area_name"`
			CCVI_category         string `json:"ccvi_category"`
		}{
			Community_area_or_zip: communityAreaOrZip,
			Community_area_name:   communityAreaName,
			CCVI_category:         ccviCategory,
		}
		records = append(records, covidVI)
	}
	log.Printf("Cleaned Covid VI Records: %+v", records)
	log.Printf("Number of dropped records: %d", droppedRecords)
	return records, nil
}

func cleanBuildingPermits(data map[string]interface{}) (BuildingPermitsJsonRecords, error) {
	log.Printf("Applying cleaning rules for Building Permits: %+v", data)
	var records BuildingPermitsJsonRecords
	var droppedRecords int

	for row := 0; row < len(data["data"].([]interface{})); row++ {
		record := data["data"].([]interface{})[row]
		recMap := record.(map[string]interface{})

		// Clean each field
		id, err := parseString(recMap["id"])
		if err != nil {
			log.Printf("Missing or invalid id: %v", recMap["id"])
			droppedRecords++
			continue
		}
		permitStatus, err := parseString(recMap["permit_status"])
		if err != nil {
			permitStatus = "" // Not all records contain permit_status
		}
		permitType, err := parseString(recMap["permit_type"])
		if err != nil {
			log.Printf("Missing or invalid permit_type: %v", recMap["permit_type"])
			droppedRecords++
			continue
		}
		reviewType, err := parseString(recMap["review_type"])
		if err != nil {
			log.Printf("Missing or invalid review_type: %v", recMap["review_type"])
			droppedRecords++
			continue
		}
		applicationStartDateStr, ok := recMap["application_start_date"].(string)
		if !ok {
			log.Printf("Missing or invalid application_start_date: %v", recMap["application_start_date"])
			droppedRecords++
			continue
		}
		applicationStartDate, err := parseTime(applicationStartDateStr)
		if err != nil {
			log.Printf("Failed to parse application_start_date: %v", err)
			droppedRecords++
			continue
		}
		issueDateStr, ok := recMap["issue_date"].(string)
		if !ok {
			log.Printf("Missing or invalid issue_date: %v", recMap["issue_date"])
			droppedRecords++
			continue
		}
		issueDate, err := parseTime(issueDateStr)
		if err != nil {
			log.Printf("Failed to parse issue_date: %v", err)
			droppedRecords++
			continue
		}
		workType, err := parseString(recMap["work_type"])
		if err != nil {
			log.Printf("Missing or invalid work_type: %v", recMap["work_type"])
			droppedRecords++
			continue
		}
		totalFee, err := parseFloat(recMap["total_fee"])
		if err != nil {
			log.Printf("Missing or invalid total_fee: %v", recMap["total_fee"])
			droppedRecords++
			continue
		}
		reportedCost, err := parseString(recMap["reported_cost"])
		if err != nil {
			reportedCost = "" // Not available for all records
		}
		communityArea, err := parseString(recMap["community_area"])
		if err != nil {
			communityArea = "" // Missing value handled later
		}
		latitude, err := parseString(recMap["latitude"])
		if err != nil {
			latitude = "" // Missing value handled later
		}
		longitude, err := parseString(recMap["longitude"])
		if err != nil {
			longitude = "" // Missing value handled later
		}
		// Drop records with missing location fields
		if (communityArea == "") || ((latitude == "") && (longitude == "")) {
			log.Printf("All location fields are missing, dropping record")
			droppedRecords++
			continue
		}

		// Create a cleaned Building Permits record
		buildingPermit := struct {
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
		}{
			Id:                     id,
			Permit_status:          permitStatus,
			Permit_type:            permitType,
			Review_type:            reviewType,
			Application_start_date: applicationStartDate,
			Issue_date:             issueDate,
			Work_type:              workType,
			Total_fee:              totalFee,
			Reported_cost:          reportedCost,
			Community_area:         communityArea,
			Latitude:               latitude,
			Longitude:              longitude,
		}
		records = append(records, buildingPermit)
	}
	log.Printf("Cleaned Building Permits Records: %+v", records)
	log.Printf("Number of dropped records: %d", droppedRecords)
	return records, nil
}

func cleanCensusData(data map[string]interface{}) (CensusDataJsonRecords, error) {
	log.Printf("Applying cleaning rules for Census Data: %+v", data)
	var records CensusDataJsonRecords
	var droppedRecords int

	for row := 0; row < len(data["data"].([]interface{})); row++ {
		record := data["data"].([]interface{})[row]
		recMap := record.(map[string]interface{})

		// Clean each field
		communityAreaNumber, err := parseString(recMap["ca"])
		if err != nil {
			log.Printf("Missing or invalid community_area_number: %v", recMap["community_area_number"])
			droppedRecords++
			continue
		}
		communityAreaName, err := parseString(recMap["community_area_name"])
		if err != nil {
			log.Printf("Missing or invalid community_area_name: %v", recMap["community_area_name"])
			droppedRecords++
			continue
		}
		percentHouseholdsBelowPoverty, err := parseFloat(recMap["percent_households_below_poverty"])
		if err != nil {
			log.Printf("Failed to convert percent_households_below_poverty: %v", err)
			droppedRecords++
			continue
		}
		percentAged16Unemployed, err := parseFloat(recMap["percent_aged_16_unemployed"])
		if err != nil {
			log.Printf("Failed to convert percent_aged_16_unemployed: %v", err)
			droppedRecords++
			continue
		}
		perCapitaIncome, err := parseInt(recMap["per_capita_income_"])
		if err != nil {
			log.Printf("Failed to convert per_capita_income: %v", err)
			droppedRecords++
			continue
		}

		// Create a cleaned Census Data record
		censusData := struct {
			Community_area_number            string  `json:"community_area_number"`
			Community_area_name              string  `json:"community_area_name"`
			Percent_households_below_poverty float64 `json:"percent_households_below_poverty"`
			Percent_aged_16_unemployed       float64 `json:"percent_aged_16_unemployed"`
			Per_capita_income                int64   `json:"per_capita_income"`
		}{
			Community_area_number:            communityAreaNumber,
			Community_area_name:              communityAreaName,
			Percent_households_below_poverty: percentHouseholdsBelowPoverty,
			Percent_aged_16_unemployed:       percentAged16Unemployed,
			Per_capita_income:                perCapitaIncome,
		}
		records = append(records, censusData)
	}
	log.Printf("Cleaned Census Data Records: %+v", records)
	log.Printf("Number of dropped records: %d", droppedRecords)
	return records, nil
}

func cleanTransportationTrips(data map[string]interface{}) (TransportationTripsJsonRecords, error) {
	log.Printf("Applying cleaning rules for Transportation Trips: %+v", data)
	var records TransportationTripsJsonRecords
	var droppedRecords int

	for row := 0; row < len(data["data"].([]interface{})); row++ {
		record := data["data"].([]interface{})[row]
		recMap := record.(map[string]interface{})

		// Clean each field
		tripID, err := parseString(recMap["trip_id"])
		if err != nil {
			log.Printf("Missing or invalid trip_id: %v", recMap["trip_id"])
			droppedRecords++
			continue
		}
		tripStartTimestampStr, ok := recMap["trip_start_timestamp"].(string)
		if !ok {
			log.Printf("Missing or invalid trip_start_timestamp: %v", recMap["trip_start_timestamp"])
			droppedRecords++
			continue
		}
		tripStartTimestamp, err := parseTime(tripStartTimestampStr)
		if err != nil {
			log.Printf("Failed to parse trip_start_timestamp: %v", err)
			droppedRecords++
			continue
		}
		tripEndTimestampStr, ok := recMap["trip_end_timestamp"].(string)
		if !ok {
			log.Printf("Missing or invalid trip_end_timestamp: %v", recMap["trip_end_timestamp"])
			droppedRecords++
			continue
		}
		tripEndTimestamp, err := parseTime(tripEndTimestampStr)
		if err != nil {
			log.Printf("Failed to parse trip_end_timestamp: %v", err)
			droppedRecords++
			continue
		}
		pickupCensusTract, err := parseString(recMap["pickup_census_tract"])
		if err != nil {
			log.Printf("Missing or invalid pickup_census_tract: %v", recMap["pickup_census_tract"])
			droppedRecords++
			continue
		}
		dropoffCensusTract, err := parseString(recMap["dropoff_census_tract"])
		if err != nil {
			log.Printf("Missing or invalid dropoff_census_tract: %v", recMap["dropoff_census_tract"])
			droppedRecords++
			continue
		}
		pickupCommunityArea, err := parseString(recMap["pickup_community_area"])
		if err != nil {
			log.Printf("Missing or invalid pickup_community_area: %v", recMap["pickup_community_area"])
			droppedRecords++
			continue
		}
		dropoffCommunityArea, err := parseString(recMap["dropoff_community_area"])
		if err != nil {
			log.Printf("Missing or invalid dropoff_community_area: %v", recMap["dropoff_community_area"])
			droppedRecords++
			continue
		}
		pickupCentroidLatitude, err := parseString(recMap["pickup_centroid_latitude"])
		if err != nil {
			log.Printf("Missing or invalid pickup_centroid_latitude: %v", recMap["pickup_centroid_latitude"])
			droppedRecords++
			continue
		}
		pickupCentroidLongitude, err := parseString(recMap["pickup_centroid_longitude"])
		if err != nil {
			log.Printf("Missing or invalid pickup_centroid_longitude: %v", recMap["pickup_centroid_longitude"])
			droppedRecords++
			continue
		}
		dropoffCentroidLatitude, err := parseString(recMap["dropoff_centroid_latitude"])
		if err != nil {
			log.Printf("Missing or invalid dropoff_centroid_latitude: %v", recMap["dropoff_centroid_latitude"])
			droppedRecords++
			continue
		}
		dropoffCentroidLongitude, err := parseString(recMap["dropoff_centroid_longitude"])
		if err != nil {
			log.Printf("Missing or invalid dropoff_centroid_longitude: %v", recMap["dropoff_centroid_longitude"])
			droppedRecords++
			continue
		}

		// Create a cleaned Transportation Trips record
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
		}{
			Trip_id:                    tripID,
			Trip_start_timestamp:       tripStartTimestamp,
			Trip_end_timestamp:         tripEndTimestamp,
			Pickup_census_tract:        pickupCensusTract,
			Dropoff_census_tract:       dropoffCensusTract,
			Pickup_community_area:      pickupCommunityArea,
			Dropoff_community_area:     dropoffCommunityArea,
			Pickup_centroid_latitude:   pickupCentroidLatitude,
			Pickup_centroid_longitude:  pickupCentroidLongitude,
			Dropoff_centroid_latitude:  dropoffCentroidLatitude,
			Dropoff_centroid_longitude: dropoffCentroidLongitude,
		}
		records = append(records, trip)
	}
	log.Printf("Cleaned Transportation Trips Records: %+v", records)
	log.Printf("Number of dropped records: %d", droppedRecords)
	return records, nil
}

func cleanPHS(data map[string]interface{}) (PHSJsonRecords, error) {
	log.Printf("Applying cleaning rules for Public Health Statistics: %+v", data)
	var records PHSJsonRecords
	var droppedRecords int

	for row := 0; row < len(data["data"].([]interface{})); row++ {
		record := data["data"].([]interface{})[row]
		recMap := record.(map[string]interface{})

		// Clean each field
		communityArea, err := parseString(recMap["community_area"])
		if err != nil {
			log.Printf("Missing or invalid community_area: %v", recMap["community_area"])
			droppedRecords++
			continue
		}
		communityAreaName, err := parseString(recMap["community_area_name"])
		if err != nil {
			log.Printf("Missing or invalid community_area_name: %v", recMap["community_area_name"])
			droppedRecords++
			continue
		}
		belowPovertyLevel, err := parseFloat(recMap["below_poverty_level"])
		if err != nil {
			log.Printf("Failed to convert below_poverty_level: %v", err)
			droppedRecords++
			continue
		}
		perCapitaIncome, err := parseFloat(recMap["per_capita_income"])
		if err != nil {
			log.Printf("Failed to convert per_capita_income: %v", err)
			droppedRecords++
			continue
		}
		unemployment, err := parseFloat(recMap["unemployment"])
		if err != nil {
			log.Printf("Failed to convert unemployment: %v", err)
			droppedRecords++
			continue
		}

		// Create a cleaned Public Health Statistics record
		phs := struct {
			Community_area      string  `json:"community_area"`
			Community_area_name string  `json:"community_area_name"`
			Below_poverty_level float64 `json:"below_poverty_level"`
			Per_capita_income   float64 `json:"per_capita_income"`
			Unemployment        float64 `json:"unemployment"`
		}{
			Community_area:      communityArea,
			Community_area_name: communityAreaName,
			Below_poverty_level: belowPovertyLevel,
			Per_capita_income:   perCapitaIncome,
			Unemployment:        unemployment,
		}
		records = append(records, phs)
	}
	log.Printf("Cleaned Public Health Statistics Records: %+v", records)
	log.Printf("Number of dropped records: %d", droppedRecords)
	return records, nil
}
