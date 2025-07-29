package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os/exec"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// Add context.Context as the first argument
func ExportSqliteToDolt(ctx context.Context, sqlitePath, doltPath, table string, where string) error {
	if sqlitePath == "" || doltPath == "" || table == "" {
		return fmt.Errorf("usage: sqlite2dolt --sqlite <sqlite.db> --dolt <dolt_dir> --table <table_name>")
	}

	// Open SQLite DB
	db, err := sql.Open("sqlite3", sqlitePath)
	if err != nil {
		return fmt.Errorf("failed to open SQLite DB: %v", err)
	}
	defer db.Close()

	// Recreate the table in Dolt from the SQLite schema
	if err := recreateDoltTable(db, doltPath, sqlitePath, table); err != nil {
		return fmt.Errorf("failed to prepare Dolt table: %v", err)
	}

	whereClause := ""

	if where != "" {
		whereClause = " WHERE " + where
	}
	// Query all rows from the table
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s %s", table, whereClause))
	if err != nil {
		return fmt.Errorf("failed to query table: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %v", err)
	}

	// Prepare Dolt insert statement
	insertStmt := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES ", table,
		join(cols, ","))

	var recordsSaved int

	// For each row, insert into DoltDB using dolt sql concurrently
	for rows.Next() {
		// Check for cancellation before processing each row
		select {
		case <-ctx.Done():
			log.Println("Export cancelled by user.")
			return ctx.Err()
		default:
		}

		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		// The `rows.Scan` function populates the `values` slice with the data from the database.
		// We need to pass the `values` slice, which contains the actual data, to `insertRowToDolt`.
		// The `valuePtrs` slice contains pointers to the elements of `values`, which is what `Scan` needs,
		// but not what `insertRowToDolt` needs.
		// We also handle `[]byte` which the driver might return for TEXT columns.
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				values[i] = string(b)
			}
		}

		if err := insertRowToDolt(doltPath, insertStmt, values); err != nil {
			return fmt.Errorf("failed to insert row: %v", err)
		}
		recordsSaved++
		if recordsSaved%1000 == 0 {
			log.Printf("Saved %d records...", recordsSaved)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error during row iteration: %v", err)
	}

	log.Printf("Summary: Total records saved: %d", recordsSaved)
	fmt.Println("Export complete.")
	return nil
}

func insertRowToDolt(doltPath string, insertStmt string, values []interface{}) error {
	valueStrings := make([]string, 0, len(values))
	for _, v := range values {
		if v == nil {
			valueStrings = append(valueStrings, "NULL")
			continue
		}

		switch val := v.(type) {
		case string:
			// For SQL, single quotes need to be escaped by doubling them up for insertion.
			// The entire string must also be enclosed in single quotes.
			escaped := strings.ReplaceAll(val, "'", "''")
			valueStrings = append(valueStrings, fmt.Sprintf("'%s'", escaped))
		default:
			valueStrings = append(valueStrings, fmt.Sprintf("%v", v))
		}
	}

	// The `dolt sql -q` command expects the entire query as a single string argument.
	// We construct the full query string here, e.g., "INSERT ... VALUES ('val1', 123, 'val2')"
	fullQuery := insertStmt + "(" + strings.Join(valueStrings, ", ") + ")"

	cmd := exec.Command("dolt", "sql", "-q", fullQuery)
	cmd.Dir = doltPath
	if output, err := cmd.CombinedOutput(); err != nil {
		// Using CombinedOutput provides more context on failure.
		return fmt.Errorf("failed to insert into dolt db: %w\nOutput: %s", err, string(output))
	}
	return nil
}

// recreateDoltTable reads the CREATE TABLE statement from SQLite, converts it to be
// MySQL-compatible, and executes it in Dolt to create a fresh table.
func recreateDoltTable(db *sql.DB, doltPath, sqlitePath, tableName string) error {
	// Get CREATE TABLE statement from SQLite
	var createTableSQL string
	err := db.QueryRow(fmt.Sprintf("SELECT sql FROM sqlite_master WHERE type='table' AND name = '%s'", tableName)).Scan(&createTableSQL)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("table '%s' not found in SQLite database '%s'", tableName, sqlitePath)
		}
		return fmt.Errorf("failed to get CREATE TABLE statement from SQLite: %w", err)
	}

	// Convert SQLite CREATE TABLE to Dolt/MySQL compatible statement.
	// This is a simplistic conversion for the known schema.
	doltCreateTableSQL := strings.Replace(createTableSQL, "IF NOT EXISTS", "", 1)
	doltCreateTableSQL = strings.Replace(doltCreateTableSQL, "AUTOINCREMENT", "AUTO_INCREMENT", -1)
	doltCreateTableSQL = strings.Replace(doltCreateTableSQL, "INTEGER PRIMARY KEY", "BIGINT PRIMARY KEY", 1)

	// To ensure idempotency, we drop the table before creating it.
	log.Printf("Dropping table `%s` in Dolt if it exists...", tableName)
	dropCmd := exec.Command("dolt", "sql", "-q", fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
	dropCmd.Dir = doltPath
	if output, err := dropCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to drop table in DoltDB: %w\nOutput: %s", err, string(output))
	}

	// Execute CREATE TABLE in Dolt
	log.Printf("Creating table `%s` in Dolt...", tableName)
	createCmd := exec.Command("dolt", "sql", "-q", doltCreateTableSQL)
	createCmd.Dir = doltPath
	if output, err := createCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to create table in DoltDB: %w\nOutput: %s", err, string(output))
	}
	log.Printf("Table `%s` created successfully in Dolt.", tableName)

	return nil
}

func join(arr []string, sep string) string {
	result := ""
	for i, s := range arr {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}
