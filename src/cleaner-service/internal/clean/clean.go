package clean

import (
	"encoding/json"
	"fmt"
	"log"
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
	Zip_code                           int       `json:"zip_code"`
	Week_number                        int       `json:"week_number"`
	Week_start                         time.Time `json:"week_start"`
	Week_end                           time.Time `json:"week_end"`
	Cases_weekly                       int       `json:"cases_weekly"`
	Cases_cumulative                   int       `json:"cases_cumulative"`
	Case_rate_weekly                   float64   `json:"case_rate_weekly"`
	Case_rate_cumulative               int       `json:"case_rate_cumulative"`
	Tests_weekly                       int       `json:"tests_weekly"`
	Tests_cumulative                   int       `json:"tests_cumulative"`
	Test_rate_weekly                   float64   `json:"test_rate_weekly"`
	Test_rate_cumulative               float64   `json:"test_rate_cumulative"`
	Percent_tested_positive_weekly     float64   `json:"percent_tested_positive_weekly"`
	Percent_tested_positive_cumulative float64   `json:"percent"`
	Deaths_weekly                      int       `json:"deaths_weekly"`
	Deaths_cumulative                  int       `json:"deaths_cumulative"`
	Death_rate_weekly                  float64   `json:"death_rate_weekly"`
	Death_rate_cumulative              float64   `json:"death_rate_cumulative"`
	Population                         int       `json:"population"`
	Row_id                             string    `json:"row_id"`
	Zip_code_location                  []string  `json:"zip_code_location"`
}

type CovidVIJsonRecords []struct {
	Geography_type                        string   `json:"geography_type"`
	Community_area_or_zip                 string   `json:"community_area_or_zip"`
	Community_area_name                   string   `json:"community_area_name"`
	CCVI_score                            float64  `json:"ccvi_score"`
	CCVI_category                         string   `json:"ccvi_category"`
	Rank_socioeconomic_status             int      `json:"rank_socioeconomic_status"`
	Rank_household_composition            int      `json:"rank_household_composition"`
	Rank_adults_no_pcp                    int      `json:"rank_adults_no_pcp"`
	Rank_cumulative_mobility_ratio        int      `json:"rank_cumulative_mobility_ratio"`
	Rank_frontline_essential_workers      int      `json:"rank_frontline_essential_workers"`
	Rank_age_65_plus                      int      `json:"rank_age_65_plus"`
	Rank_comorbid_conditions              int      `json:"rank_comorbid_conditions"`
	Rank_covid_19_incidence_rate          int      `json:"rank_covid_19_incidence_rate"`
	Rank_covid_19_hospital_admission_rate int      `json:"rank_covid_19_hospital_admission_rate"`
	Rank_covid_19_crude_mortality_rate    int      `json:"rank_covid_19_crude_mortality_rate"`
	Location                              []string `json:"location"`
}

type BuildingPermitsJsonRecords []struct {
	Id                     string    `json:"id"`
	Permit_Code            string    `json:"permit_"`
	Permit_type            string    `json:"permit_type"`
	Review_type            string    `json:"review_type"`
	Application_start_date time.Time `json:"application_start_date"`
	Issue_date             time.Time `json:"issue_date"`
	Processing_time        string    `json:"processing_time"`
	Street_number          string    `json:"street_number"`
	Street_direction       string    `json:"street_direction"`
	Street_name            string    `json:"street_name"`
	Suffix                 string    `json:"suffix"`
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
	Pin1                   string    `json:"pin1"`
	Pin2                   string    `json:"pin2"`
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

func cleanTaxiTrips(data map[string]interface{}) (TaxiTripsJsonRecords, error) {
	log.Printf("Applying cleaning rules for Taxi Trips: %+v", data)
	var records TaxiTripsJsonRecords
	var droppedRecords int

	for row := 0; row < len(data["data"].([]interface{})); row++ {
		record := data["data"].([]interface{})[row]
		recMap := record.(map[string]interface{})

		// Clean each field individually
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
	// TODO: Implement Covid Cases cleaning rules
	return records, nil
}

func cleanCovidVI(data map[string]interface{}) (CovidVIJsonRecords, error) {
	log.Printf("Applying cleaning rules for Covid Vulnerability Index: %+v", data)
	var records CovidVIJsonRecords
	// TODO: Implement Covid VI cleaning rules
	return records, nil
}

func cleanBuildingPermits(data map[string]interface{}) (BuildingPermitsJsonRecords, error) {
	log.Printf("Applying cleaning rules for Building Permits: %+v", data)
	var records BuildingPermitsJsonRecords
	// TODO: Implement Building Permits cleaning rules
	return records, nil
}

func cleanCensusData(data map[string]interface{}) (CensusDataJsonRecords, error) {
	log.Printf("Applying cleaning rules for Census Data: %+v", data)
	var records CensusDataJsonRecords
	// TODO: Implement Census Data cleaning rules
	return records, nil
}

func cleanTransportationTrips(data map[string]interface{}) (TransportationTripsJsonRecords, error) {
	log.Printf("Applying cleaning rules for Transportation Trips: %+v", data)
	var records TransportationTripsJsonRecords
	// TODO: Implement Transportation Trips cleaning rules
	return records, nil
}

func cleanPHS(data map[string]interface{}) (PHSJsonRecords, error) {
	log.Printf("Applying cleaning rules for Public Health Statistics: %+v", data)
	var records PHSJsonRecords
	// TODO: Implement PHS cleaning rules
	return records, nil
}
