package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
)

// fetchCmd implements the Command interface for the 'fetch' command.
type fetchCmd struct{}

func (c *fetchCmd) Execute(ctx context.Context) error {
	fetchFlagSet := flag.NewFlagSet("fetch", flag.ExitOnError)
	yearPtr := fetchFlagSet.Int("year", 0, "The year to filter records for (required).")
	offsetPtr := fetchFlagSet.Int("offset", 0, "The starting offset for fetching records.")
	batchSizePtr := fetchFlagSet.Int("batch", 1000, "The number of records to fetch per API call (the 'limit').")
	dbFile := fetchFlagSet.String("db", "kcc_data.db", "optional sqlite 3 database file name")
	retriesPtr := fetchFlagSet.Int("retries", 3, "Number of retries for failed HTTP requests (default 3).")
	backoffPtr := fetchFlagSet.Int("backoff", 500, "Initial backoff in milliseconds for retries (default 500ms).")

	fetchFlagSet.Usage = func() {
		fmt.Println(c.Help())
		fmt.Println("\nFlags:")
		fetchFlagSet.PrintDefaults()
	}

	fetchFlagSet.Parse(os.Args[2:])

	if *yearPtr == 0 {
		fmt.Println("Error: The --year flag is required.")
		fetchFlagSet.Usage()
		os.Exit(1)
	}

	apiKey := os.Getenv(apiKeyEnvVar)
	if apiKey == "" {
		log.Fatalf("Error: API Key not found. Please set the %s environment variable.", apiKeyEnvVar)
	}

	db, err := initDB(*dbFile)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	err = fetchAllAndSaveData(ctx, db, apiKey, *yearPtr, *offsetPtr, *batchSizePtr, *retriesPtr, *backoffPtr)
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			log.Println("Operation cancelled by user.")
			return err
		} else {
			log.Fatalf("An error occurred: %v", err)
			return err
		}
	}
	return nil
}

func (c *fetchCmd) Help() string {
	return "Fetches KCC transcripts from the API and saves them to a SQLite database.\nUsage: kcc fetch --year <year> [--offset <offset>] [--batch <size>] [--db <dbfile>] [--retries <count>] [--backoff <ms>]"
}
