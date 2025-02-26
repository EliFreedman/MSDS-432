package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/lib/pq"
)

var db *sql.DB

// Connect to Postgres database
func Connect() error {
	var err error

	// Create a connection string
	connStr := fmt.Sprintf("host=postgres port=5432 user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("POSTGRES_USER"), os.Getenv("POSTGRES_PASSWORD"), os.Getenv("POSTGRES_DB"))

	// Open a connection to the database
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error opening database: %q", err)
		return fmt.Errorf("error opening to database: %v", err)
	}

	// Check if the connection is successful
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error connecting to the database: %q", err)
		return fmt.Errorf("error connecting to database: %v", err)
	}

	fmt.Println("Successfully connected to the database")
	return nil
}

// Close the database connection
func Close() {
	err := db.Close()
	if err != nil {
		log.Fatalf("Error closing database: %q", err)
	}

	fmt.Println("Database connection closed")
}

// CreateTable creates a new table in the database based on the JSON schema
func CreateTable(tableName string, jsonSchema string) error {
	// Parse the JSON schema
	var schema map[string]string
	err := json.Unmarshal([]byte(jsonSchema), &schema)
	if err != nil {
		log.Fatalf("Error parsing JSON schema: %q", err)
		return fmt.Errorf("error parsing JSON schema: %v", err)
	}

	// Generate the SQL CREATE TABLE statement
	var columns []string
	for columnName, columnType := range schema {
		columns = append(columns, fmt.Sprintf("%s %s", columnName, columnType))
	}
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableName, strings.Join(columns, ", "))

	// Execute the SQL statement
	_, err = db.Exec(query)
	if err != nil {
		log.Fatalf("Error creating table: %q", err)
		return fmt.Errorf("error creating table: %v", err)
	}

	fmt.Println("Table created successfully or already exists")
	return nil
}

// AddRecords adds multiple records to the database
func AddRecords(tableName string, records []map[string]interface{}) error {
	if len(records) == 0 {
		return fmt.Errorf("no records to add")
	}

	// Generate the SQL INSERT INTO statement
	var columns []string
	var placeholders []string
	var values []interface{}
	i := 1

	// Assuming all records have the same keys
	for column := range records[0] {
		columns = append(columns, column)
	}

	for _, record := range records {
		var recordPlaceholders []string
		for _, column := range columns {
			values = append(values, record[column])
			recordPlaceholders = append(recordPlaceholders, fmt.Sprintf("$%d", i))
			i++
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(recordPlaceholders, ", ")))
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", tableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	// Execute the SQL statement
	_, err := db.Exec(query, values...)
	if err != nil {
		log.Fatalf("Error adding records: %q", err)
		return fmt.Errorf("error adding records: %v", err)
	}

	fmt.Println("Records added successfully")
	return nil
}
