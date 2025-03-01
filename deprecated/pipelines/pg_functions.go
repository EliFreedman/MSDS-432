package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"io"

	_ "github.com/lib/pq"
	"github.com/joho/godotenv"
)

func init() {
	// Load environment variables from .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	// Configure logging to file and console
	logDir := "logs"
	logFile := logDir + "/go_processes.log"

	// Ensure log directory exists
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		os.Mkdir(logDir, 0755)
	}

	// Open log file
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	// Configure multi-writer to log to both file and console
	multiWriter := io.MultiWriter(os.Stdout, file)
	log.SetOutput(multiWriter)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile) // Adds timestamp and file location
	log.Println("Logging initialized")
}

func connectToPostgres() (*sql.DB, error) {
	dbname := os.Getenv("POSTGRES_DB")
	if dbname == "" {
		dbname = "mydatabase"
	}

	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "user"
	}

	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = "password"
	}

	host := os.Getenv("POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbname)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Printf("Error: Unable to connect to the database. Details: %v", err)
		return nil, err
	}

	log.Printf("Successfully connected to the database: %s", dbname)
	return db, nil
}

func executeQuery(db *sql.DB, query string) ([]map[string]interface{}, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := []map[string]interface{}{}
	for rows.Next() {
		scans := make([]interface{}, len(columns))
		for i := range scans {
			scans[i] = new(interface{})
		}

		if err := rows.Scan(scans...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]interface{})
		for i, col := range columns {
			rowMap[col] = *(scans[i].(*interface{}))
		}
		results = append(results, rowMap)
	}
	return results, nil
}

func createTableIfNotExists(db *sql.DB, tableName string, columns []string) error {
	columnDefs := ""
	for i, col := range columns {
		if i > 0 {
			columnDefs += ", "
		}
		columnDefs += fmt.Sprintf(`"%s" TEXT`, col) // Ensuring column names are SQL-safe
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s);`, tableName, columnDefs)
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	log.Printf("Table '%s' is ready.", tableName)
	return nil
}


func insertData(db *sql.DB, tableName string, columns []string, data [][]interface{}) error {
	// Construct the column names and placeholders dynamically
	columnsStr := `"` + strings.Join(columns, `", "`) + `"`
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`, tableName, columnsStr, strings.Join(placeholders, ", "))

	// Insert each row
	for _, row := range data {
		if len(row) != len(columns) {
			log.Printf("Skipping row due to column mismatch: expected %d, got %d", len(columns), len(row))
			continue
		}
		_, err := db.Exec(query, row...)
		if err != nil {
			log.Printf("Error inserting row into '%s': %v", tableName, err)
		}
	}
	return nil
}
