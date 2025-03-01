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
	Cases_weekly                       int32     `json:"cases_weekly"`
	Cases_cumulative                   int32     `json:"cases_cumulative"`
	Case_rate_weekly                   float64   `json:"case_rate_weekly"`
	Case_rate_cumulative               float64   `json:"case_rate_cumulative"`
	Tests_weekly                       int32     `json:"tests_weekly"`
	Tests_cumulative                   int32     `json:"tests_cumulative"`
	Test_rate_weekly                   float64   `json:"test_rate_weekly"`
	Test_rate_cumulative               float64   `json:"test_rate_cumulative"`
	Percent_tested_positive_weekly     float64   `json:"percent_tested_positive_weekly"`
	Percent_tested_positive_cumulative float64   `json:"percent_tested_positive_cumulative"`
	Deaths_weekly                      int32     `json:"deaths_weekly"`
	Deaths_cumulative                  int32     `json:"deaths_cumulative"`
	Death_rate_weekly                  float64   `json:"death_rate_weekly"`
	Death_rate_cumulative              float64   `json:"death_rate_cumulative"`
	Population                         int32     `json:"population"`
	Row_id                             string    `json:"row_id"`
	Latitude                           float64   `json:"latitude"`
	Longitude                          float64   `json:"longitude"`
}

type CovidVIJsonRecords []struct {
	Geography_type                        string  `json:"geography_type"`
	Community_area_or_zip                 string  `json:"community_area_or_zip"`
	Community_area_name                   string  `json:"community_area_name"`
	CCVI_score                            float64 `json:"ccvi_score"`
	CCVI_category                         string  `json:"ccvi_category"`
	Rank_socioeconomic_status             int     `json:"rank_socioeconomic_status"`
	Rank_household_composition            int     `json:"rank_household_composition"`
	Rank_adults_no_pcp                    int     `json:"rank_adults_no_pcp"`
	Rank_cumulative_mobility_ratio        int     `json:"rank_cumulative_mobility_ratio"`
	Rank_frontline_essential_workers      int     `json:"rank_frontline_essential_workers"`
	Rank_age_65_plus                      int     `json:"rank_age_65_plus"`
	Rank_comorbid_conditions              int     `json:"rank_comorbid_conditions"`
	Rank_covid_19_incidence_rate          int     `json:"rank_covid_19_incidence_rate"`
	Rank_covid_19_hospital_admission_rate int     `json:"rank_covid_19_hospital_admission_rate"`
	Rank_covid_19_crude_mortality_rate    int     `json:"rank_covid_19_crude_mortality_rate"`
	Latitude                              float64 `json:"latitude"`
	Longitude                             float64 `json:"longitude"`
}

type BuildingPermitsJsonRecords []struct {
	Id                     string    `json:"id"`
	Permit_code            string    `json:"permit_"`
	Permit_status          string    `json:"permit_status"`
	Permit_milestone       string    `json:"permit_milestone"`
	Permit_type            string    `json:"permit_type"`
	Review_type            string    `json:"review_type"`
	Application_start_date time.Time `json:"application_start_date"`
	Issue_date             time.Time `json:"issue_date"`
	Processing_time        string    `json:"processing_time"`
	Street_number          string    `json:"street_number"`
	Street_direction       string    `json:"street_direction"`
	Street_name            string    `json:"street_name"`
	Work_type              string    `json:"work_type"`
	Work_description       string    `json:"work_description"`
	Building_fee_paid      float64   `json:"building_fee_paid"`
	Zoning_fee_paid        float64   `json:"zoning_fee_paid"`
	Other_fee_paid         float64   `json:"other_fee_paid"`
	Subtotal_paid          float64   `json:"subtotal_paid"`
	Building_fee_unpaid    float64   `json:"building_fee_unpaid"`
	Zoning_fee_unpaid      float64   `json:"zoning_fee_unpaid"`
	Other_fee_unpaid       float64   `json:"other_fee_unpaid"`
	Subtotal_unpaid        float64   `json:"subtotal_unpaid"`
	Building_fee_waived    float64   `json:"building_fee_waived"`
	Building_fee_subtotal  float64   `json:"building_fee_subtotal"`
	Zoning_fee_subtotal    float64   `json:"zoning_fee_subtotal"`
	Other_fee_subtotal     float64   `json:"other_fee_subtotal"`
	Zoning_fee_waived      float64   `json:"zoning_fee_waived"`
	Other_fee_waived       float64   `json:"other_fee_waived"`
	Subtotal_waived        float64   `json:"subtotal_waived"`
	Total_fee              float64   `json:"total_fee"`
	Contact_1_type         string    `json:"contact_1_type"`
	Contact_1_name         string    `json:"contact_1_name"`
	Contact_1_city         string    `json:"contact_1_city"`
	Contact_1_state        string    `json:"contact_1_state"`
	Contact_1_zipcode      string    `json:"contact_1_zipcode"`
	Reported_cost          string    `json:"reported_cost"`
	Community_area         string    `json:"community_area"`
	Census_tract           string    `json:"census_tract"`
	Ward                   string    `json:"ward"`
	Xcoordinate            string    `json:"xcoordinate"`
	Ycoordinate            string    `json:"ycoordinate"`
	Latitude               string    `json:"latitude"`
	Longitude              string    `json:"longitude"`
}

type CensusDataJsonRecords []struct {
	Community_area_number                       string  `json:"community_area_number"`
	Community_area_name                         string  `json:"community_area_name"`
	Percent_of_housing_crowded                  float64 `json:"percent_of_housing_crowded"`
	Percent_households_below_poverty            float64 `json:"percent_households_below_poverty"`
	Percent_aged_16_unemployed                  float64 `json:"percent_aged_16_unemployed"`
	Percent_aged_25_without_high_school_diploma float64 `json:"percent_aged_25_without_high_school_diploma"`
	Percent_aged_under_18_or_over_64            float64 `json:"percent_aged_under_18_or_over_64"`
	Per_capita_income                           int     `json:"per_capita_income"`
	Hardship_index                              int     `json:"hardship_index"`
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
	Community_area                             string  `json:"community_area"`
	Community_area_name                        string  `json:"community_area_name"`
	Birth_rate                                 float64 `json:"birth_rate"`
	General_fertility_rate                     float64 `json:"general_fertility_rate"`
	Low_birth_weight                           float64 `json:"low_birth_weight"`
	Prenatal_care_beginning_in_first_trimester float64 `json:"prenatal_care_beginning_in_first_trimester"`
	Preterm_births                             float64 `json:"preterm_births"`
	Teen_birth_rate                            float64 `json:"teen_birth_rate"`
	Assault_homicide                           float64 `json:"assault_homicide"`
	Breast_cancer_in_females                   float64 `json:"breast_cancer_in_females"`
	Cancer_all_sites                           float64 `json:"cancer_all_sites"`
	Colorectal_cancer                          float64 `json:"colorectal_cancer"`
	Diabetes_related                           float64 `json:"diabetes_related"`
	Firearm_related                            float64 `json:"firearm_related"`
	Infant_mortality_rate                      float64 `json:"infant_mortality_rate"`
	Lung_cancer                                float64 `json:"lung_cancer"`
	Prostate_cancer_in_males                   float64 `json:"prostate_cancer_in_males"`
	Stroke_cerebrovascular_disease             float64 `json:"stroke_cerebrovascular_disease"`
	Childhood_blood_lead_level_screening       float64 `json:"childhood_blood_lead_level_screening"`
	Childhood_lead_poisoning                   float64 `json:"childhood_lead_poisoning"`
	Gonorrhea_in_females                       float64 `json:"gonorrhea_in_females"`
	Gonorrhea_in_males                         float64 `json:"gonorrhea_in_males"`
	Tuberculosis                               float64 `json:"tuberculosis"`
	Below_poverty_level                        float64 `json:"below_poverty_level"`
	Crowded_housing                            float64 `json:"crowded_housing"`
	Dependency                                 float64 `json:"dependency"`
	No_high_school_diploma                     float64 `json:"no_high_school_diploma"`
	Per_capita_income                          float64 `json:"per_capita_income"`
	Unemployment                               float64 `json:"unemployment"`
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

// parseTime parses a string into a time.Time object using multiple formats
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
	return t, err
}

// parseInt is a helper function to parse an int from an interface{}
func parseInt(value interface{}) (int, error) {
	switch v := value.(type) {
	case string:
		return strconv.Atoi(v)
	case int:
		return v, nil
	default:
		return 0, fmt.Errorf("invalid type for int conversion: %T", v)
	}
}

// parseFloat is a helper function to parse a float64 from an interface{}
func parseFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case string:
		return strconv.ParseFloat(v, 64)
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("invalid type for float conversion: %T", v)
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
		tripID, ok := recMap["trip_id"].(string)
		if !ok {
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
		pickupCentroidLatitude, ok := recMap["pickup_centroid_latitude"].(string)
		if !ok {
			log.Printf("Missing or invalid pickup_centroid_latitude: %v", recMap["pickup_centroid_latitude"])
			droppedRecords++
			continue
		}
		pickupCentroidLongitude, ok := recMap["pickup_centroid_longitude"].(string)
		if !ok {
			log.Printf("Missing or invalid pickup_centroid_longitude: %v", recMap["pickup_centroid_longitude"])
			droppedRecords++
			continue
		}
		pickupCommunityArea, ok := recMap["pickup_community_area"].(string)
		if !ok {
			log.Printf("Missing or invalid pickup_community_area: %v", recMap["pickup_community_area"])
			droppedRecords++
			continue
		}
		dropoffCentroidLatitude, ok := recMap["dropoff_centroid_latitude"].(string)
		if !ok {
			log.Printf("Missing or invalid dropoff_centroid_latitude: %v", recMap["dropoff_centroid_latitude"])
			droppedRecords++
			continue
		}
		dropoffCentroidLongitude, ok := recMap["dropoff_centroid_longitude"].(string)
		if !ok {
			log.Printf("Missing or invalid dropoff_centroid_longitude: %v", recMap["dropoff_centroid_longitude"])
			droppedRecords++
			continue
		}
		dropoffCommunityArea, ok := recMap["dropoff_community_area"].(string)
		if !ok {
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
		zipCode, ok := recMap["zip_code"].(string)
		if !ok {
			log.Printf("Missing or invalid zip_code: %v", recMap["zip_code"])
			droppedRecords++
			continue
		}
		weekNumber, ok := recMap["week_number"].(string)
		if !ok {
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
		rowID, ok := recMap["row_id"].(string)
		if !ok {
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
		latitude, ok := coordinates[1].(float64)
		if !ok {
			log.Printf("Invalid latitude in coordinates: %v", coordinates[1])
			droppedRecords++
			continue
		}
		longitude, ok := coordinates[0].(float64)
		if !ok {
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
			Cases_weekly                       int32     `json:"cases_weekly"`
			Cases_cumulative                   int32     `json:"cases_cumulative"`
			Case_rate_weekly                   float64   `json:"case_rate_weekly"`
			Case_rate_cumulative               float64   `json:"case_rate_cumulative"`
			Tests_weekly                       int32     `json:"tests_weekly"`
			Tests_cumulative                   int32     `json:"tests_cumulative"`
			Test_rate_weekly                   float64   `json:"test_rate_weekly"`
			Test_rate_cumulative               float64   `json:"test_rate_cumulative"`
			Percent_tested_positive_weekly     float64   `json:"percent_tested_positive_weekly"`
			Percent_tested_positive_cumulative float64   `json:"percent_tested_positive_cumulative"`
			Deaths_weekly                      int32     `json:"deaths_weekly"`
			Deaths_cumulative                  int32     `json:"deaths_cumulative"`
			Death_rate_weekly                  float64   `json:"death_rate_weekly"`
			Death_rate_cumulative              float64   `json:"death_rate_cumulative"`
			Population                         int32     `json:"population"`
			Row_id                             string    `json:"row_id"`
			Latitude                           float64   `json:"latitude"`
			Longitude                          float64   `json:"longitude"`
		}{
			Zip_code:                           zipCode,
			Week_number:                        weekNumber,
			Week_start:                         weekStart,
			Week_end:                           weekEnd,
			Cases_weekly:                       int32(casesWeekly),
			Cases_cumulative:                   int32(casesCumulative),
			Case_rate_weekly:                   caseRateWeekly,
			Case_rate_cumulative:               caseRateCumulative,
			Tests_weekly:                       int32(testsWeekly),
			Tests_cumulative:                   int32(testsCumulative),
			Test_rate_weekly:                   testRateWeekly,
			Test_rate_cumulative:               testRateCumulative,
			Percent_tested_positive_weekly:     percentTestedPositiveWeekly,
			Percent_tested_positive_cumulative: percentTestedPositiveCumulative,
			Deaths_weekly:                      int32(deathsWeekly),
			Deaths_cumulative:                  int32(deathsCumulative),
			Death_rate_weekly:                  deathRateWeekly,
			Death_rate_cumulative:              deathRateCumulative,
			Population:                         int32(population),
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
		geographyType, ok := recMap["geography_type"].(string)
		if !ok {
			log.Printf("Missing or invalid geography_type: %v", recMap["geography_type"])
			droppedRecords++
			continue
		}
		communityAreaOrZip, ok := recMap["community_area_or_zip"].(string)
		if !ok {
			log.Printf("Missing or invalid community_area_or_zip: %v", recMap["community_area_or_zip"])
			droppedRecords++
			continue
		}
		communityAreaName, ok := recMap["community_area_name"].(string)
		if !ok {
			// Can extrapolate from community_area_or_zip
			communityAreaName = ""
		}
		ccviScore, err := parseFloat(recMap["ccvi_score"])
		if err != nil {
			log.Printf("Failed to convert ccvi_score: %v", err)
			droppedRecords++
			continue
		}
		ccviCategory, ok := recMap["ccvi_category"].(string)
		if !ok {
			log.Printf("Missing or invalid ccvi_category: %v", recMap["ccvi_category"])
			droppedRecords++
			continue
		}
		rankSocioeconomicStatus, err := parseInt(recMap["rank_socioeconomic_status"])
		if err != nil {
			rankSocioeconomicStatus = -1
		}
		rankHouseholdComposition, err := parseInt(recMap["rank_household_composition"])
		if err != nil {
			rankHouseholdComposition = -1
		}
		rankAdultsNoPcp, err := parseInt(recMap["rank_adults_no_pcp"])
		if err != nil {
			rankAdultsNoPcp = -1
		}
		rankCumulativeMobilityRatio, err := parseInt(recMap["rank_cumulative_mobility_ratio"])
		if err != nil {
			rankCumulativeMobilityRatio = -1
		}
		rankFrontlineEssentialWorkers, err := parseInt(recMap["rank_frontline_essential_workers"])
		if err != nil {
			rankFrontlineEssentialWorkers = -1
		}
		rankAge65Plus, err := parseInt(recMap["rank_age_65_plus"])
		if err != nil {
			rankAge65Plus = -1
		}
		rankComorbidConditions, err := parseInt(recMap["rank_comorbid_conditions"])
		if err != nil {
			rankComorbidConditions = -1
		}
		rankCovid19IncidenceRate, err := parseInt(recMap["rank_covid_19_incidence_rate"])
		if err != nil {
			log.Printf("Failed to convert rank_covid_19_incidence_rate: %v", err)
			droppedRecords++
			continue
		}
		rankCovid19HospitalAdmissionRate, err := parseInt(recMap["rank_covid_19_hospital_admission_rate"])
		if err != nil {
			rankCovid19HospitalAdmissionRate = -1
		}
		rankCovid19CrudeMortalityRate, err := parseInt(recMap["rank_covid_19_crude_mortality_rate"])
		if err != nil {
			rankCovid19CrudeMortalityRate = -1
		}
		location, ok := recMap["location"].(map[string]interface{})
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
		latitude, ok := coordinates[1].(float64)
		if !ok {
			log.Printf("Invalid latitude in coordinates: %v", coordinates[1])
			droppedRecords++
			continue
		}
		longitude, ok := coordinates[0].(float64)
		if !ok {
			log.Printf("Invalid longitude in coordinates: %v", coordinates[0])
			droppedRecords++
			continue
		}

		// Create a cleaned Covid VI record
		covidVI := struct {
			Geography_type                        string  `json:"geography_type"`
			Community_area_or_zip                 string  `json:"community_area_or_zip"`
			Community_area_name                   string  `json:"community_area_name"`
			CCVI_score                            float64 `json:"ccvi_score"`
			CCVI_category                         string  `json:"ccvi_category"`
			Rank_socioeconomic_status             int     `json:"rank_socioeconomic_status"`
			Rank_household_composition            int     `json:"rank_household_composition"`
			Rank_adults_no_pcp                    int     `json:"rank_adults_no_pcp"`
			Rank_cumulative_mobility_ratio        int     `json:"rank_cumulative_mobility_ratio"`
			Rank_frontline_essential_workers      int     `json:"rank_frontline_essential_workers"`
			Rank_age_65_plus                      int     `json:"rank_age_65_plus"`
			Rank_comorbid_conditions              int     `json:"rank_comorbid_conditions"`
			Rank_covid_19_incidence_rate          int     `json:"rank_covid_19_incidence_rate"`
			Rank_covid_19_hospital_admission_rate int     `json:"rank_covid_19_hospital_admission_rate"`
			Rank_covid_19_crude_mortality_rate    int     `json:"rank_covid_19_crude_mortality_rate"`
			Latitude                              float64 `json:"latitude"`
			Longitude                             float64 `json:"longitude"`
		}{
			Geography_type:                        geographyType,
			Community_area_or_zip:                 communityAreaOrZip,
			Community_area_name:                   communityAreaName,
			CCVI_score:                            ccviScore,
			CCVI_category:                         ccviCategory,
			Rank_socioeconomic_status:             rankSocioeconomicStatus,
			Rank_household_composition:            rankHouseholdComposition,
			Rank_adults_no_pcp:                    rankAdultsNoPcp,
			Rank_cumulative_mobility_ratio:        rankCumulativeMobilityRatio,
			Rank_frontline_essential_workers:      rankFrontlineEssentialWorkers,
			Rank_age_65_plus:                      rankAge65Plus,
			Rank_comorbid_conditions:              rankComorbidConditions,
			Rank_covid_19_incidence_rate:          rankCovid19IncidenceRate,
			Rank_covid_19_hospital_admission_rate: rankCovid19HospitalAdmissionRate,
			Rank_covid_19_crude_mortality_rate:    rankCovid19CrudeMortalityRate,
			Latitude:                              latitude,
			Longitude:                             longitude,
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
		id, ok := recMap["id"].(string)
		if !ok {
			log.Printf("Missing or invalid id: %v", recMap["id"])
			droppedRecords++
			continue
		}
		permitCode, ok := recMap["permit_"].(string)
		if !ok {
			log.Printf("Missing or invalid permit_code: %v", recMap["permit_code"])
			droppedRecords++
			continue
		}
		permitStatus, ok := recMap["permit_status"].(string)
		if !ok {
			permitStatus = ""
		}
		permitMilestone, ok := recMap["permit_milestone"].(string)
		if !ok {
			permitMilestone = ""
		}
		permitType, ok := recMap["permit_type"].(string)
		if !ok {
			log.Printf("Missing or invalid permit_type: %v", recMap["permit_type"])
			droppedRecords++
			continue
		}
		reviewType, ok := recMap["review_type"].(string)
		if !ok {
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
		processingTime, ok := recMap["processing_time"].(string)
		if !ok {
			log.Printf("Missing or invalid processing_time: %v", recMap["processing_time"])
			droppedRecords++
			continue
		}
		streetNumber, ok := recMap["street_number"].(string)
		if !ok {
			log.Printf("Missing or invalid street_number: %v", recMap["street_number"])
			droppedRecords++
			continue
		}
		streetDirection, ok := recMap["street_direction"].(string)
		if !ok {
			log.Printf("Missing or invalid street_direction: %v", recMap["street_direction"])
			droppedRecords++
			continue
		}
		streetName, ok := recMap["street_name"].(string)
		if !ok {
			log.Printf("Missing or invalid street_name: %v", recMap["street_name"])
			droppedRecords++
			continue
		}
		workType, ok := recMap["work_type"].(string)
		if !ok {
			workType = ""
		}
		workDescription, ok := recMap["work_description"].(string)
		if !ok {
			log.Printf("Missing or invalid work_description: %v", recMap["work_description"])
			droppedRecords++
			continue
		}
		buildingFeePaid, err := parseFloat(recMap["building_fee_paid"])
		if err != nil {
			log.Printf("Failed to convert building_fee_paid: %v", err)
			droppedRecords++
			continue
		}
		zoningFeePaid, err := parseFloat(recMap["zoning_fee_paid"])
		if err != nil {
			log.Printf("Failed to convert zoning_fee_paid: %v", err)
			droppedRecords++
			continue
		}
		otherFeePaid, err := parseFloat(recMap["other_fee_paid"])
		if err != nil {
			log.Printf("Failed to convert other_fee_paid: %v", err)
			droppedRecords++
			continue
		}
		subtotalPaid, err := parseFloat(recMap["subtotal_paid"])
		if err != nil {
			log.Printf("Failed to convert subtotal_paid: %v", err)
			droppedRecords++
			continue
		}
		buildingFeeUnpaid, err := parseFloat(recMap["building_fee_unpaid"])
		if err != nil {
			log.Printf("Failed to convert building_fee_unpaid: %v", err)
			droppedRecords++
			continue
		}
		zoningFeeUnpaid, err := parseFloat(recMap["zoning_fee_unpaid"])
		if err != nil {
			log.Printf("Failed to convert zoning_fee_unpaid: %v", err)
			droppedRecords++
			continue
		}
		otherFeeUnpaid, err := parseFloat(recMap["other_fee_unpaid"])
		if err != nil {
			log.Printf("Failed to convert other_fee_unpaid: %v", err)
			droppedRecords++
			continue
		}
		subtotalUnpaid, err := parseFloat(recMap["subtotal_unpaid"])
		if err != nil {
			log.Printf("Failed to convert subtotal_unpaid: %v", err)
			droppedRecords++
			continue
		}
		buildingFeeWaived, err := parseFloat(recMap["building_fee_waived"])
		if err != nil {
			log.Printf("Failed to convert building_fee_waived: %v", err)
			droppedRecords++
			continue
		}
		buildingFeeSubtotal, err := parseFloat(recMap["building_fee_subtotal"])
		if err != nil {
			log.Printf("Failed to convert building_fee_subtotal: %v", err)
			droppedRecords++
			continue
		}
		zoningFeeSubtotal, err := parseFloat(recMap["zoning_fee_subtotal"])
		if err != nil {
			log.Printf("Failed to convert zoning_fee_subtotal: %v", err)
			droppedRecords++
			continue
		}
		otherFeeSubtotal, err := parseFloat(recMap["other_fee_subtotal"])
		if err != nil {
			log.Printf("Failed to convert other_fee_subtotal: %v", err)
			droppedRecords++
			continue
		}
		zoningFeeWaived, err := parseFloat(recMap["zoning_fee_waived"])
		if err != nil {
			log.Printf("Failed to convert zoning_fee_waived: %v", err)
			droppedRecords++
			continue
		}
		otherFeeWaived, err := parseFloat(recMap["other_fee_waived"])
		if err != nil {
			log.Printf("Failed to convert other_fee_waived: %v", err)
			droppedRecords++
			continue
		}
		subtotalWaived, err := parseFloat(recMap["subtotal_waived"])
		if err != nil {
			log.Printf("Failed to convert subtotal_waived: %v", err)
			droppedRecords++
			continue
		}
		totalFee, err := parseFloat(recMap["total_fee"])
		if err != nil {
			log.Printf("Failed to convert total_fee: %v", err)
			droppedRecords++
			continue
		}
		contact1Type, ok := recMap["contact_1_type"].(string)
		if !ok {
			contact1Type = ""
		}
		contact1Name, ok := recMap["contact_1_name"].(string)
		if !ok {
			contact1Name = ""
		}
		contact1City, ok := recMap["contact_1_city"].(string)
		if !ok {
			contact1City = ""
		}
		contact1State, ok := recMap["contact_1_state"].(string)
		if !ok {
			contact1State = ""
		}
		contact1Zipcode, ok := recMap["contact_1_zipcode"].(string)
		if !ok {
			contact1Zipcode = ""
		}
		reportedCost, ok := recMap["reported_cost"].(string)
		if !ok {
			log.Printf("Missing or invalid reported_cost: %v", recMap["reported_cost"])
			droppedRecords++
			continue
		}
		communityArea, ok := recMap["community_area"].(string)
		if !ok {
			communityArea = ""
		}
		censusTract, ok := recMap["census_tract"].(string)
		if !ok {
			censusTract = ""
		}
		ward, ok := recMap["ward"].(string)
		if !ok {
			ward = ""
		}
		xcoordinate, ok := recMap["xcoordinate"].(string)
		if !ok {
			xcoordinate = ""
		}
		ycoordinate, ok := recMap["ycoordinate"].(string)
		if !ok {
			ycoordinate = ""
		}
		latitude, ok := recMap["latitude"].(string)
		if !ok {
			latitude = ""
		}
		longitude, ok := recMap["longitude"].(string)
		if !ok {
			longitude = ""
		}
		// Drop records with missing location fields
		if communityArea == "" && censusTract == "" && ward == "" && xcoordinate == "" && ycoordinate == "" && latitude == "" && longitude == "" {
			log.Printf("All location fields are missing, dropping record")
			droppedRecords++
			continue
		}

		// Create a cleaned Building Permits record
		buildingPermit := struct {
			Id                     string    `json:"id"`
			Permit_code            string    `json:"permit_"`
			Permit_status          string    `json:"permit_status"`
			Permit_milestone       string    `json:"permit_milestone"`
			Permit_type            string    `json:"permit_type"`
			Review_type            string    `json:"review_type"`
			Application_start_date time.Time `json:"application_start_date"`
			Issue_date             time.Time `json:"issue_date"`
			Processing_time        string    `json:"processing_time"`
			Street_number          string    `json:"street_number"`
			Street_direction       string    `json:"street_direction"`
			Street_name            string    `json:"street_name"`
			Work_type              string    `json:"work_type"`
			Work_description       string    `json:"work_description"`
			Building_fee_paid      float64   `json:"building_fee_paid"`
			Zoning_fee_paid        float64   `json:"zoning_fee_paid"`
			Other_fee_paid         float64   `json:"other_fee_paid"`
			Subtotal_paid          float64   `json:"subtotal_paid"`
			Building_fee_unpaid    float64   `json:"building_fee_unpaid"`
			Zoning_fee_unpaid      float64   `json:"zoning_fee_unpaid"`
			Other_fee_unpaid       float64   `json:"other_fee_unpaid"`
			Subtotal_unpaid        float64   `json:"subtotal_unpaid"`
			Building_fee_waived    float64   `json:"building_fee_waived"`
			Building_fee_subtotal  float64   `json:"building_fee_subtotal"`
			Zoning_fee_subtotal    float64   `json:"zoning_fee_subtotal"`
			Other_fee_subtotal     float64   `json:"other_fee_subtotal"`
			Zoning_fee_waived      float64   `json:"zoning_fee_waived"`
			Other_fee_waived       float64   `json:"other_fee_waived"`
			Subtotal_waived        float64   `json:"subtotal_waived"`
			Total_fee              float64   `json:"total_fee"`
			Contact_1_type         string    `json:"contact_1_type"`
			Contact_1_name         string    `json:"contact_1_name"`
			Contact_1_city         string    `json:"contact_1_city"`
			Contact_1_state        string    `json:"contact_1_state"`
			Contact_1_zipcode      string    `json:"contact_1_zipcode"`
			Reported_cost          string    `json:"reported_cost"`
			Community_area         string    `json:"community_area"`
			Census_tract           string    `json:"census_tract"`
			Ward                   string    `json:"ward"`
			Xcoordinate            string    `json:"xcoordinate"`
			Ycoordinate            string    `json:"ycoordinate"`
			Latitude               string    `json:"latitude"`
			Longitude              string    `json:"longitude"`
		}{
			Id:                     id,
			Permit_code:            permitCode,
			Permit_status:          permitStatus,
			Permit_milestone:       permitMilestone,
			Permit_type:            permitType,
			Review_type:            reviewType,
			Application_start_date: applicationStartDate,
			Issue_date:             issueDate,
			Processing_time:        processingTime,
			Street_number:          streetNumber,
			Street_direction:       streetDirection,
			Street_name:            streetName,
			Work_type:              workType,
			Work_description:       workDescription,
			Building_fee_paid:      buildingFeePaid,
			Zoning_fee_paid:        zoningFeePaid,
			Other_fee_paid:         otherFeePaid,
			Subtotal_paid:          subtotalPaid,
			Building_fee_unpaid:    buildingFeeUnpaid,
			Zoning_fee_unpaid:      zoningFeeUnpaid,
			Other_fee_unpaid:       otherFeeUnpaid,
			Subtotal_unpaid:        subtotalUnpaid,
			Building_fee_waived:    buildingFeeWaived,
			Building_fee_subtotal:  buildingFeeSubtotal,
			Zoning_fee_subtotal:    zoningFeeSubtotal,
			Other_fee_subtotal:     otherFeeSubtotal,
			Zoning_fee_waived:      zoningFeeWaived,
			Other_fee_waived:       otherFeeWaived,
			Subtotal_waived:        subtotalWaived,
			Total_fee:              totalFee,
			Contact_1_type:         contact1Type,
			Contact_1_name:         contact1Name,
			Contact_1_city:         contact1City,
			Contact_1_state:        contact1State,
			Contact_1_zipcode:      contact1Zipcode,
			Reported_cost:          reportedCost,
			Community_area:         communityArea,
			Census_tract:           censusTract,
			Ward:                   ward,
			Xcoordinate:            xcoordinate,
			Ycoordinate:            ycoordinate,
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
		communityAreaNumber, ok := recMap["ca"].(string)
		if !ok {
			log.Printf("Missing or invalid community_area_number: %v", recMap["community_area_number"])
			droppedRecords++
			continue
		}
		communityAreaName, ok := recMap["community_area_name"].(string)
		if !ok {
			log.Printf("Missing or invalid community_area_name: %v", recMap["community_area_name"])
			droppedRecords++
			continue
		}
		percentOfHousingCrowded, err := parseFloat(recMap["percent_of_housing_crowded"])
		if err != nil {
			log.Printf("Failed to convert percent_of_housing_crowded: %v", err)
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
		percentAged25WithoutHighSchoolDiploma, err := parseFloat(recMap["percent_aged_25_without_high_school_diploma"])
		if err != nil {
			log.Printf("Failed to convert percent_aged_25_without_high_school_diploma: %v", err)
			droppedRecords++
			continue
		}
		percentAgedUnder18OrOver64, err := parseFloat(recMap["percent_aged_under_18_or_over_64"])
		if err != nil {
			log.Printf("Failed to convert percent_aged_under_18_or_over_64: %v", err)
			droppedRecords++
			continue
		}
		perCapitaIncome, err := parseInt(recMap["per_capita_income_"])
		if err != nil {
			log.Printf("Failed to convert per_capita_income: %v", err)
			droppedRecords++
			continue
		}
		hardshipIndex, err := parseInt(recMap["hardship_index"])
		if err != nil {
			log.Printf("Failed to convert hardship_index: %v", err)
			droppedRecords++
			continue
		}

		// Create a cleaned Census Data record
		censusData := struct {
			Community_area_number                       string  `json:"community_area_number"`
			Community_area_name                         string  `json:"community_area_name"`
			Percent_of_housing_crowded                  float64 `json:"percent_of_housing_crowded"`
			Percent_households_below_poverty            float64 `json:"percent_households_below_poverty"`
			Percent_aged_16_unemployed                  float64 `json:"percent_aged_16_unemployed"`
			Percent_aged_25_without_high_school_diploma float64 `json:"percent_aged_25_without_high_school_diploma"`
			Percent_aged_under_18_or_over_64            float64 `json:"percent_aged_under_18_or_over_64"`
			Per_capita_income                           int     `json:"per_capita_income"`
			Hardship_index                              int     `json:"hardship_index"`
		}{
			Community_area_number:                       communityAreaNumber,
			Community_area_name:                         communityAreaName,
			Percent_of_housing_crowded:                  percentOfHousingCrowded,
			Percent_households_below_poverty:            percentHouseholdsBelowPoverty,
			Percent_aged_16_unemployed:                  percentAged16Unemployed,
			Percent_aged_25_without_high_school_diploma: percentAged25WithoutHighSchoolDiploma,
			Percent_aged_under_18_or_over_64:            percentAgedUnder18OrOver64,
			Per_capita_income:                           perCapitaIncome,
			Hardship_index:                              hardshipIndex,
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
		tripID, ok := recMap["trip_id"].(string)
		if !ok {
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
		pickupCensusTract, ok := recMap["pickup_census_tract"].(string)
		if !ok {
			pickupCensusTract = ""
		}
		dropoffCensusTract, ok := recMap["dropoff_census_tract"].(string)
		if !ok {
			dropoffCensusTract = ""
		}
		pickupCommunityArea, ok := recMap["pickup_community_area"].(string)
		if !ok {
			log.Printf("Missing or invalid pickup_community_area: %v", recMap["pickup_community_area"])
			droppedRecords++
			continue
		}
		dropoffCommunityArea, ok := recMap["dropoff_community_area"].(string)
		if !ok {
			log.Printf("Missing or invalid dropoff_community_area: %v", recMap["dropoff_community_area"])
			droppedRecords++
			continue
		}
		pickupCentroidLatitude, ok := recMap["pickup_centroid_latitude"].(string)
		if !ok {
			log.Printf("Missing or invalid pickup_centroid_latitude: %v", recMap["pickup_centroid_latitude"])
			droppedRecords++
			continue
		}
		pickupCentroidLongitude, ok := recMap["pickup_centroid_longitude"].(string)
		if !ok {
			log.Printf("Missing or invalid pickup_centroid_longitude: %v", recMap["pickup_centroid_longitude"])
			droppedRecords++
			continue
		}
		dropoffCentroidLatitude, ok := recMap["dropoff_centroid_latitude"].(string)
		if !ok {
			log.Printf("Missing or invalid dropoff_centroid_latitude: %v", recMap["dropoff_centroid_latitude"])
			droppedRecords++
			continue
		}
		dropoffCentroidLongitude, ok := recMap["dropoff_centroid_longitude"].(string)
		if !ok {
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
		communityArea, ok := recMap["community_area"].(string)
		if !ok {
			log.Printf("Missing or invalid community_area: %v", recMap["community_area"])
			droppedRecords++
			continue
		}
		communityAreaName, ok := recMap["community_area_name"].(string)
		if !ok {
			log.Printf("Missing or invalid community_area_name: %v", recMap["community_area_name"])
			droppedRecords++
			continue
		}
		birthRate, err := parseFloat(recMap["birth_rate"])
		if err != nil {
			birthRate = -1
		}
		generalFertilityRate, err := parseFloat(recMap["general_fertility_rate"])
		if err != nil {
			generalFertilityRate = -1
		}
		lowBirthWeight, err := parseFloat(recMap["low_birth_weight"])
		if err != nil {
			lowBirthWeight = -1
		}
		prenatalCareBeginningInFirstTrimester, err := parseFloat(recMap["prenatal_care_beginning_in_first_trimester"])
		if err != nil {
			prenatalCareBeginningInFirstTrimester = -1
		}
		pretermBirths, err := parseFloat(recMap["preterm_births"])
		if err != nil {
			pretermBirths = -1
		}
		teenBirthRate, err := parseFloat(recMap["teen_birth_rate"])
		if err != nil {
			teenBirthRate = -1
		}
		assaultHomicide, err := parseFloat(recMap["assault_homicide"])
		if err != nil {
			assaultHomicide = -1
		}
		breastCancerInFemales, err := parseFloat(recMap["breast_cancer_in_females"])
		if err != nil {
			breastCancerInFemales = -1
		}
		cancerAllSites, err := parseFloat(recMap["cancer_all_sites"])
		if err != nil {
			cancerAllSites = -1
		}
		colorectalCancer, err := parseFloat(recMap["colorectal_cancer"])
		if err != nil {
			colorectalCancer = -1
		}
		diabetesRelated, err := parseFloat(recMap["diabetes_related"])
		if err != nil {
			diabetesRelated = -1
		}
		firearmRelated, err := parseFloat(recMap["firearm_related"])
		if err != nil {
			firearmRelated = -1
		}
		infantMortalityRate, err := parseFloat(recMap["infant_mortality_rate"])
		if err != nil {
			infantMortalityRate = -1
		}
		lungCancer, err := parseFloat(recMap["lung_cancer"])
		if err != nil {
			lungCancer = -1
		}
		prostateCancerInMales, err := parseFloat(recMap["prostate_cancer_in_males"])
		if err != nil {
			prostateCancerInMales = -1
		}
		strokeCerebrovascularDisease, err := parseFloat(recMap["stroke_cerebrovascular_disease"])
		if err != nil {
			strokeCerebrovascularDisease = -1
		}
		childhoodBloodLeadLevelScreening, err := parseFloat(recMap["childhood_blood_lead_level_screening"])
		if err != nil {
			childhoodBloodLeadLevelScreening = -1
		}
		childhoodLeadPoisoning, err := parseFloat(recMap["childhood_lead_poisoning"])
		if err != nil {
			childhoodLeadPoisoning = -1
		}
		gonorrheaInFemales, err := parseFloat(recMap["gonorrhea_in_females"])
		if err != nil {
			gonorrheaInFemales = -1
		}
		gonorrheaInMales, err := parseFloat(recMap["gonorrhea_in_males"])
		if err != nil {
			gonorrheaInMales = -1
		}
		tuberculosis, err := parseFloat(recMap["tuberculosis"])
		if err != nil {
			tuberculosis = -1
		}
		belowPovertyLevel, err := parseFloat(recMap["below_poverty_level"])
		if err != nil {
			log.Printf("Failed to convert below_poverty_level: %v", err)
			droppedRecords++
			continue
		}
		crowdedHousing, err := parseFloat(recMap["crowded_housing"])
		if err != nil {
			crowdedHousing = -1
		}
		dependency, err := parseFloat(recMap["dependency"])
		if err != nil {
			dependency = -1
		}
		noHighSchoolDiploma, err := parseFloat(recMap["no_high_school_diploma"])
		if err != nil {
			noHighSchoolDiploma = -1
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
			Community_area                             string  `json:"community_area"`
			Community_area_name                        string  `json:"community_area_name"`
			Birth_rate                                 float64 `json:"birth_rate"`
			General_fertility_rate                     float64 `json:"general_fertility_rate"`
			Low_birth_weight                           float64 `json:"low_birth_weight"`
			Prenatal_care_beginning_in_first_trimester float64 `json:"prenatal_care_beginning_in_first_trimester"`
			Preterm_births                             float64 `json:"preterm_births"`
			Teen_birth_rate                            float64 `json:"teen_birth_rate"`
			Assault_homicide                           float64 `json:"assault_homicide"`
			Breast_cancer_in_females                   float64 `json:"breast_cancer_in_females"`
			Cancer_all_sites                           float64 `json:"cancer_all_sites"`
			Colorectal_cancer                          float64 `json:"colorectal_cancer"`
			Diabetes_related                           float64 `json:"diabetes_related"`
			Firearm_related                            float64 `json:"firearm_related"`
			Infant_mortality_rate                      float64 `json:"infant_mortality_rate"`
			Lung_cancer                                float64 `json:"lung_cancer"`
			Prostate_cancer_in_males                   float64 `json:"prostate_cancer_in_males"`
			Stroke_cerebrovascular_disease             float64 `json:"stroke_cerebrovascular_disease"`
			Childhood_blood_lead_level_screening       float64 `json:"childhood_blood_lead_level_screening"`
			Childhood_lead_poisoning                   float64 `json:"childhood_lead_poisoning"`
			Gonorrhea_in_females                       float64 `json:"gonorrhea_in_females"`
			Gonorrhea_in_males                         float64 `json:"gonorrhea_in_males"`
			Tuberculosis                               float64 `json:"tuberculosis"`
			Below_poverty_level                        float64 `json:"below_poverty_level"`
			Crowded_housing                            float64 `json:"crowded_housing"`
			Dependency                                 float64 `json:"dependency"`
			No_high_school_diploma                     float64 `json:"no_high_school_diploma"`
			Per_capita_income                          float64 `json:"per_capita_income"`
			Unemployment                               float64 `json:"unemployment"`
		}{
			Community_area:         communityArea,
			Community_area_name:    communityAreaName,
			Birth_rate:             birthRate,
			General_fertility_rate: generalFertilityRate,
			Low_birth_weight:       lowBirthWeight,
			Prenatal_care_beginning_in_first_trimester: prenatalCareBeginningInFirstTrimester,
			Preterm_births:                       pretermBirths,
			Teen_birth_rate:                      teenBirthRate,
			Assault_homicide:                     assaultHomicide,
			Breast_cancer_in_females:             breastCancerInFemales,
			Cancer_all_sites:                     cancerAllSites,
			Colorectal_cancer:                    colorectalCancer,
			Diabetes_related:                     diabetesRelated,
			Firearm_related:                      firearmRelated,
			Infant_mortality_rate:                infantMortalityRate,
			Lung_cancer:                          lungCancer,
			Prostate_cancer_in_males:             prostateCancerInMales,
			Stroke_cerebrovascular_disease:       strokeCerebrovascularDisease,
			Childhood_blood_lead_level_screening: childhoodBloodLeadLevelScreening,
			Childhood_lead_poisoning:             childhoodLeadPoisoning,
			Gonorrhea_in_females:                 gonorrheaInFemales,
			Gonorrhea_in_males:                   gonorrheaInMales,
			Tuberculosis:                         tuberculosis,
			Below_poverty_level:                  belowPovertyLevel,
			Crowded_housing:                      crowdedHousing,
			Dependency:                           dependency,
			No_high_school_diploma:               noHighSchoolDiploma,
			Per_capita_income:                    perCapitaIncome,
			Unemployment:                         unemployment,
		}
		records = append(records, phs)
	}
	log.Printf("Cleaned Public Health Statistics Records: %+v", records)
	log.Printf("Number of dropped records: %d", droppedRecords)
	return records, nil
}
