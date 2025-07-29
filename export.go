package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
)

// exportCmd implements the Command interface for the 'export' command.
type exportCmd struct{}

func (c *exportCmd) Execute(ctx context.Context) error {
	exportFlagSet := flag.NewFlagSet("export", flag.ExitOnError)
	sqlitePath := exportFlagSet.String("sqlite", "", "Path to SQLite database file (required)")
	doltPath := exportFlagSet.String("dolt", "", "Path to DoltDB database directory (required)")
	table := exportFlagSet.String("table", "kcc_transcripts", "Table name to export")
	where := exportFlagSet.String("where", "", "Optional WHERE clause to filter records")

	exportFlagSet.Usage = func() {
		fmt.Println(c.Help())
		fmt.Println("\nFlags:")
		exportFlagSet.PrintDefaults()
	}

	exportFlagSet.Parse(os.Args[2:])

	if *sqlitePath == "" || *doltPath == "" || *table == "" {
		fmt.Println("Error: --sqlite and --dolt flags are required.")
		exportFlagSet.Usage()
		os.Exit(1)
	}

	err := ExportSqliteToDolt(ctx, *sqlitePath, *doltPath, *table, *where)
	if err != nil {
		log.Fatalf("Export failed: %v", err)
		return err
	}
	return nil
}

func (c *exportCmd) Help() string {
	return "Exports a SQLite table to DoltDB.\nUsage: kcc export --sqlite <path> --dolt <path> [--table <name> --where <condition>]"
}
