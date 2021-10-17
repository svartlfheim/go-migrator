package gomigratorintegrationtest

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

type MigrationRecordEventExpectation struct {
	Action     string
	Actor      string
	TimeFormat string
	Result     string
}

type MigrationRecordExpectation struct {
	Id     string
	Status string
	Events []MigrationRecordEventExpectation
}

type migrationRecordEvent struct {
	Action      string `json:"action"`
	Actor       string `json:"actor"`
	PerformedAt string `json:"performedat"`
	Result      string `json:"result"`
}

type migrationRecordDb struct {
	Id     string `db:"id"`
	Events string `db:"events"`
	Status string `db:"status"`
}

type migrationRecord struct {
	Id     string                 `db:"id"`
	Events []migrationRecordEvent `db:"events"`
	Status string                 `db:"status"`
}

func DBDriver() string {
	return os.Getenv("DB_DRIVER")
}

func SkipIfIntegrationTestsNotConfigured(expectedEnvVars []string, driverSpecificVars map[string][]string, t *testing.T) {
	if val, found := os.LookupEnv("CI_INTEGRATION_TESTS_ENABLED"); !found || val != "true" {
		t.Skip("Skipping integration test, set CI_INTEGRATION_TESTS_ENABLED=true to run this test")
	}

	missingVars := []string{}
	for _, envVar := range expectedEnvVars {
		_, found := os.LookupEnv(envVar)

		if !found {
			missingVars = append(missingVars, envVar)
		}
	}

	if len(missingVars) > 0 {
		t.Errorf("The following env vars must be set for integration tests: %s", strings.Join(missingVars, ","))
	}

	driver := DBDriver()
	driverEnvVars, found := driverSpecificVars[driver]

	if !found {
		// no specific driver vars
		return
	}

	missingVars = []string{}
	for _, envVar := range driverEnvVars {
		_, found := os.LookupEnv(envVar)

		if !found {
			missingVars = append(missingVars, envVar)
		}
	}

	if len(missingVars) > 0 {
		t.Errorf("The following env vars must be set for '%s' integration tests: %s", driver, strings.Join(missingVars, ","))
	}
}

func BuildDBConn(t *testing.T) *sqlx.DB {
	driver := DBDriver()
	database := os.Getenv("DB_DATABASE")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")

	var connString string

	switch driver {
	case "postgres":
		schema := os.Getenv("DB_SCHEMA")
		connString = fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s search_path=%s sslmode=disable",
			user,
			password,
			database,
			host,
			port,
			schema,
		)
	default:
		t.Errorf("Unsupported db driver: '%s'", driver)
		return nil
	}

	var dbConn *sqlx.DB
	var connError error
	for i := 0; i < 5; i++ {
		dbConn, connError = sqlx.Connect(driver, connString)

		if connError == nil {
			break
		}

		log.Printf("Error connecting (%s), retrying in 10 seconds...", connError.Error())
		time.Sleep(10 * time.Second)
	}

	if connError != nil {
		t.Errorf("Failed to connect to db for driver '%s', with data source: '%s' (%s)", driver, connString, connError.Error())
	}

	return dbConn
}

func doesTableExistPostgres(table string, conn *sqlx.DB, t *testing.T) bool {
	schema := os.Getenv("DB_SCHEMA")
	checkExistsQuery := `
	SELECT 
		COUNT(1)
	FROM 
		information_schema.tables 
	WHERE 
		table_schema=$1 AND table_name=$2;
	`
	rows, err := conn.Query(checkExistsQuery, schema, table)

	if err != nil {
		t.Errorf("Faied to run query in assertTableExistsPostgres: %s", err.Error())
		t.FailNow()
	}

	rows.Next()
	var count int
	err = rows.Scan(&count)

	if err != nil {
		t.Errorf("Failed to scan result from assertTableExistsPostgres query: %s", err.Error())
		t.FailNow()
		return false
	}

	return count == 1
}

func assertTableExistsPostgres(table string, conn *sqlx.DB, t *testing.T) {
	doesExist := doesTableExistPostgres(table, conn, t)

	assert.True(t, doesExist, "table %s does not exist", table)
}

func AssertTableExists(table string, conn *sqlx.DB, t *testing.T) {
	switch driver := DBDriver(); driver {
	case "postgres":
		assertTableExistsPostgres(table, conn, t)
	default:
		t.Errorf("assertTableExists is not implemented for '%s'", driver)
	}
}

func assertTableDoesNotExistPostgres(table string, conn *sqlx.DB, t *testing.T) {
	doesExist := doesTableExistPostgres(table, conn, t)

	assert.False(t, doesExist, "table %s exists and was not expected to", table)
}

func AssertTableDoesNotExist(table string, conn *sqlx.DB, t *testing.T) {
	switch driver := DBDriver(); driver {
	case "postgres":
		assertTableDoesNotExistPostgres(table, conn, t)
	default:
		t.Errorf("assertTableDoesNotExist is not implemented for '%s'", driver)
	}
}

func fetchMigrationRecordsPostgres(conn *sqlx.DB, t *testing.T) []migrationRecord {
	// Maybe this should specify schema...seems fine right now
	// Could be a problem in future, but not likely
	q := `
	SELECT 
		id,
		status,
		events
	FROM 
		migrations
	ORDER BY id ASC;`
	rows, err := conn.Queryx(q)

	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	migs := []migrationRecord{}

	for rows.Next() {
		var migRecordDb migrationRecordDb
		err := rows.StructScan(&migRecordDb)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		events := []migrationRecordEvent{}

		err = json.Unmarshal([]byte(migRecordDb.Events), &events)

		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		migRecord := migrationRecord{
			Id:     migRecordDb.Id,
			Status: migRecordDb.Status,
			Events: events,
		}

		migs = append(migs, migRecord)
	}

	return migs
}

func AssertMigrationRecordsTable(conn *sqlx.DB, t *testing.T, mRs []MigrationRecordExpectation) {
	var records []migrationRecord
	switch driver := DBDriver(); driver {
	case "postgres":
		records = fetchMigrationRecordsPostgres(conn, t)
	default:
		t.Errorf("AssertMigrationRecordsExist not implemented for driver '%s'", driver)
	}

	// The fetched records should be sorted by id ASC also
	sort.Slice(mRs, func(i, j int) bool {
		return mRs[i].Id < mRs[j].Id
	})

	if len(mRs) != len(records) {
		t.Errorf("expected %d migration records found %d", len(mRs), len(records))
		t.FailNow()
	}

	for i, expected := range mRs {
		got := records[i]
		assert.Equal(t, expected.Id, got.Id, "id mismatch for expected migration record %d", i)
		assert.Equal(t, expected.Status, got.Status, "status mismatch for expected migration record %d", i)
		assert.Equal(t, len(expected.Events), len(got.Events), "events count mismatch for expected migration record %d", i)

		for j, expectedEvent := range expected.Events {
			gotEvent := got.Events[j]

			assert.Equal(t, expectedEvent.Action, gotEvent.Action, "action mismatch for event %d on on migration record %d", j, i)
			assert.Equal(t, expectedEvent.Actor, gotEvent.Actor, "actor mismatch for event %d on on migration record %d", j, i)
			assert.Equal(t, expectedEvent.Result, gotEvent.Result, "result mismatch for event %d on on migration record %d", j, i)

			_, err := time.Parse(expectedEvent.TimeFormat, gotEvent.PerformedAt)

			assert.Nil(t, err, "peformedat for event %d on migration record %d could not be parsed with format %s", j, i, expectedEvent.TimeFormat)
		}
	}
}
