package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	apiBaseURL   = "https://api.data.gov.in/resource/cef25fe2-9231-4128-8aec-2c948fedd43f"
	apiKeyEnvVar = "DATA_GOV_API_KEY" // get key from https://www.data.gov.in/resource/kisan-call-centre-kcc-transcripts-farmers-queries-answers
)

// APIResponse and KCCRecord structs remain the same
type APIResponse struct {
	Records []KCCRecord `json:"records"`
}

type KCCRecord struct {
	StateName    string `json:"StateName"`
	DistrictName string `json:"DistrictName"`
	BlockName    string `json:"BlockName"`
	Season       string `json:"Season"`
	Sector       string `json:"Sector"`
	Category     string `json:"Category"`
	Crop         string `json:"Crop"`
	QueryType    string `json:"QueryType"`
	QueryText    string `json:"QueryText"`
	KccAnswer    string `json:"KccAns"`
	CreatedDate  string `json:"CreatedDate"` // The date is a string like "2024-01-15"
	Year         int    `json:"year"`        // The year is a number like 2024
}

/*
func main() {
	// 1. Define and parse command-line flags
	// The flag functions return pointers, so we'll need to dereference them later with *
	yearPtr := flag.Int("year", 2025, "The year to filter records for (default 2025).")
	offsetPtr := flag.Int("offset", 0, "The starting offset for fetching records.")
	batchSizePtr := flag.Int("batch", 1000, "The number of records to fetch per API call (the 'limit').")
	dbFile := flag.String("db", "kcc_data.db", "optional sqlite 3 database file name")

	flag.Parse()

	// 2. Validate required flags
	if *yearPtr == 0 {
		log.Println("Error: The --year flag is required.")
		fmt.Println("Usage:")
		flag.PrintDefaults() // Prints the help message for all flags
		os.Exit(1)
	}

	log.Printf("Starting data fetch with parameters: year=%d, initial-offset=%d, batch-size=%d", *yearPtr, *offsetPtr, *batchSizePtr)

	// Create a context that is canceled on an interrupt signal (Ctrl+C) or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop() // Releases resources associated with the context

	// 3. Get API key from environment variable
	apiKey := os.Getenv(apiKeyEnvVar)
	if apiKey == "" {
		log.Fatalf("Error: API Key not found. Please set the %s environment variable.", apiKeyEnvVar)
	}
	log.Println("Successfully retrieved API key from environment.")

	// 4. Initialize the database
	db, err := initDB(*dbFile)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()
	log.Println("Database initialized successfully.")

	// 5. Fetch all data in batches using the flag values
	err = fetchAllAndSaveData(ctx, db, apiKey, *yearPtr, *offsetPtr, *batchSizePtr)
	if err != nil {
		// Check if the error was due to context cancellation (e.g., Ctrl+C)
		if err == context.Canceled || err == context.DeadlineExceeded {
			log.Println("Operation cancelled by user. Shutting down gracefully.")
		} else {
			log.Fatalf("An error occurred during the fetch and save process: %v", err)
		}
	}

	log.Println("Process completed successfully!")
}
*/
// fetchAllAndSaveData orchestrates the batch fetching and saving process.
// It now accepts a context for graceful shutdown.
func fetchAllAndSaveData(ctx context.Context, db *sql.DB, apiKey string, year, startOffset, batchSize, retries, backoff int) error {
	// Initialize the loop offset with the value from the flag.
	offset := startOffset
	totalNewRecordsSaved := 0

	for {
		log.Printf("Fetching batch: year=%d, offset=%d, limit=%d", year, offset, batchSize)

		// Check for cancellation before starting a new batch
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Continue if context is not cancelled
		}

		records, err := fetchBatch(ctx, apiKey, year, offset, batchSize, retries, backoff)
		if err != nil {
			return fmt.Errorf("could not fetch batch at offset %d: %w", offset, err)
		}

		if len(records) == 0 {
			log.Println("Received an empty batch. Assuming all data has been fetched for the given parameters.")
			break
		}

		log.Printf("Fetched %d records. Saving to database...", len(records))

		newlyInserted, err := saveRecordsToDB(ctx, db, records)
		if err != nil {
			return fmt.Errorf("could not save batch from offset %d to DB: %w", offset, err)
		}
		totalNewRecordsSaved += newlyInserted

		// If the number of records returned is less than the batch size,
		// it's the last page, so we can stop.
		if len(records) < batchSize {
			log.Println("Received fewer records than batch size. This is the last page.")
			break
		}

		offset += batchSize
		// Use a select with a timer to make the sleep interruptible by the context.
		select {
		case <-time.After(500 * time.Millisecond):
			// Wait completed, continue to next iteration
		case <-ctx.Done():
			return ctx.Err() // Context was cancelled during sleep
		}
	}

	log.Printf("--------------------------------------------------")
	log.Printf("Summary: Finished fetching for year %d.", year)
	log.Printf("Total new records saved in this run: %d", totalNewRecordsSaved)
	log.Printf("--------------------------------------------------")
	return nil
}

// fetchBatch, initDB, and saveRecordsToDB functions remain exactly the same as the previous version.
// ... (They are included here for completeness)

func fetchBatch(ctx context.Context, apiKey string, year, offset, limit, retries, backoff int) ([]KCCRecord, error) {
	u, err := url.Parse(apiBaseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	q := u.Query()
	q.Set("api-key", apiKey)
	q.Set("format", "json")
	q.Set("offset", strconv.Itoa(offset))
	q.Set("limit", strconv.Itoa(limit))
	q.Set("filters[year]", strconv.Itoa(year))
	u.RawQuery = q.Encode()

	var lastErr error
	delay := time.Duration(backoff) * time.Millisecond

	for attempt := 0; attempt <= retries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("error creating HTTP request: %w", err)
		}

		client := &http.Client{Timeout: 20 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			// If context was cancelled, return immediately
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil, err
			}
			lastErr = err
			if attempt < retries {
				log.Printf("HTTP request failed (attempt %d/%d): %v. Retrying in %v...", attempt+1, retries+1, err, delay)
				select {
				case <-time.After(delay):
					delay *= 2 // Exponential backoff
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			break
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			lastErr = fmt.Errorf("API returned non-200 status code: %d. Response: %s", resp.StatusCode, string(body))
			if attempt < retries {
				log.Printf("API error (attempt %d/%d): %v. Retrying in %v...", attempt+1, retries+1, lastErr, delay)
				select {
				case <-time.After(delay):
					delay *= 2
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			break
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			lastErr = fmt.Errorf("error reading response body: %w", err)
			if attempt < retries {
				log.Printf("Read error (attempt %d/%d): %v. Retrying in %v...", attempt+1, retries+1, lastErr, delay)
				select {
				case <-time.After(delay):
					delay *= 2
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			break
		}

		var apiResponse APIResponse
		if err := json.Unmarshal(body, &apiResponse); err != nil {
			lastErr = fmt.Errorf("error unmarshaling JSON: %w", err)
			if attempt < retries {
				log.Printf("JSON error (attempt %d/%d): %v. Retrying in %v...", attempt+1, retries+1, lastErr, delay)
				select {
				case <-time.After(delay):
					delay *= 2
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			break
		}

		return apiResponse.Records, nil
	}
	return nil, fmt.Errorf("error making HTTP request after %d attempts: %w", retries+1, lastErr)
}

func initDB(filepath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", filepath)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS kcc_transcripts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		state_name TEXT,
		district_name TEXT,
		block_name TEXT,
		season TEXT,
		sector TEXT,
		category TEXT,
		crop TEXT,
		query_type TEXT,
		query_text TEXT,
		kcc_answer TEXT,
		created_date TEXT,
		year TEXT,
		hashed TEXT NOT NULL UNIQUE
	);`
	_, err = db.Exec(createTableSQL)
	return db, err
}

func saveRecordsToDB(ctx context.Context, db *sql.DB, records []KCCRecord) (int, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("error starting transaction: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT OR IGNORE INTO kcc_transcripts (
			state_name, district_name, block_name, season, sector, category, crop, query_type, query_text, kcc_answer, created_date, year, hashed
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("error preparing insert statement: %w", err)
	}
	defer stmt.Close()

	insertedCount := 0
	for _, record := range records {
		// Create a consistent string representation of the record for hashing.
		// Using a separator helps prevent collisions, e.g., "ab" + "c" vs "a" + "bc".
		hashInput := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",
			record.StateName, record.DistrictName, record.BlockName,
			record.Season, record.Sector, record.Category, record.Crop,
			record.QueryType, record.QueryText, record.KccAnswer,
		)

		// Calculate SHA256 hash
		hashBytes := sha256.Sum256([]byte(hashInput))
		hashed := hex.EncodeToString(hashBytes[:])

		res, err := stmt.Exec(record.StateName, record.DistrictName, record.BlockName, record.Season, record.Sector, record.Category, record.Crop, record.QueryType, record.QueryText, record.KccAnswer, record.CreatedDate, record.Year, hashed)
		if err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("error executing insert: %w", err)
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("error getting rows affected: %w", err)
		}
		if rowsAffected > 0 {
			insertedCount++
		}
	}
	log.Printf("Attempted to insert %d records. %d new records were added.", len(records), insertedCount)

	return insertedCount, tx.Commit()
}
