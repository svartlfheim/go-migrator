package gomigrator_test

import (
	"bytes"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/svartlfheim/gomigrator"
	gomigratorintegrationtest "github.com/svartlfheim/gomigrator/test/integration"
	gomigratorteststubs "github.com/svartlfheim/gomigrator/test/stubs"
)

var expectedEnvVars = []string{
	"DB_DRIVER",
	"DB_DATABASE",
	"DB_HOST",
	"DB_PORT",
	"DB_USER",
	"DB_PASSWORD",
}

var driverSpecificVars = map[string][]string{
	"postgres": {
		"DB_SCHEMA",
	},
}

var migrations = map[string][]gomigrator.Migration{
	"postgres": {
		{
			Id:   "my-first-mig",
			Name: "Create my_first_table",
			Execute: func(tx *sqlx.Tx) (sql.Result, error) {
				return tx.Exec(`CREATE TABLE my_first_table (id int, field1 TEXT);`)
			},
			Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
				return tx.Exec(`DROP TABLE my_first_table;`)
			},
		},
		{
			Id:   "my-second-mig",
			Name: "Create my_second_table",
			Execute: func(tx *sqlx.Tx) (sql.Result, error) {
				return tx.Exec(`CREATE TABLE my_second_table (id int, field1 TEXT);`)
			},
			Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
				return tx.Exec(`DROP TABLE my_second_table;`)
			},
		},
		{
			Id:   "my-third-mig",
			Name: "Create my_third",
			Execute: func(tx *sqlx.Tx) (sql.Result, error) {
				return tx.Exec(`CREATE TABLE my_third_table (id int, field1 TEXT);`)
			},
			Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
				return tx.Exec(`DROP TABLE my_third_table;`)
			},
		},
	},
}

func migratorOpts() gomigrator.Opts {
	opts := gomigrator.Opts{
		Applyer: "gotest",
	}

	switch gomigratorintegrationtest.DBDriver() {
	case "postgres":
		opts.Schema = os.Getenv("DB_SCHEMA")
	}

	return opts
}

func cleanDB(conn *sqlx.DB, t *testing.T) {
	sql := `
	DROP TABLE IF EXISTS my_first_table;
	DROP TABLE IF EXISTS my_second_table;
	DROP TABLE IF EXISTS my_third_table;
	DROP TABLE IF EXISTS migrations;
`

	_, err := conn.Exec(sql)

	if err != nil {
		t.Errorf("Error cleaning up database: %s", err.Error())
	}
}

func TestIntegration_Migrator_Up_AllApplied(t *testing.T) {
	gomigratorintegrationtest.SkipIfIntegrationTestsNotConfigured(expectedEnvVars, driverSpecificVars, t)
	conn := gomigratorintegrationtest.BuildDBConn(t)
	defer conn.Close()
	defer cleanDB(conn, t)

	b := new(bytes.Buffer)
	l := gomigratorteststubs.BuildZerologLogger(b)

	ml := gomigrator.NewMigrationList(migrations[gomigratorintegrationtest.DBDriver()])
	m, err := gomigrator.NewMigrator(conn, ml, migratorOpts(), l)

	assert.Nil(t, err)

	err = m.Up(gomigrator.MigrateToLatest)

	assert.Nil(t, err)
	gomigratorintegrationtest.AssertTableExists("my_first_table", conn, t)
	gomigratorintegrationtest.AssertTableExists("my_second_table", conn, t)
	gomigratorintegrationtest.AssertTableExists("my_third_table", conn, t)
	gomigratorintegrationtest.AssertTableExists("migrations", conn, t)

	if t.Failed() {
		// Stop execution here to aid with debugging
		// All of the below tests will fail with a lot of unneeded output
		return
	}

	gomigratorintegrationtest.AssertMigrationRecordsTable(conn, t, []gomigratorintegrationtest.MigrationRecordExpectation{
		{
			Id:     "my-first-mig",
			Status: "applied",
			Events: []gomigratorintegrationtest.MigrationRecordEventExpectation{
				{
					Action:     "apply",
					Actor:      "gotest",
					Result:     "applied",
					TimeFormat: time.RFC3339,
				},
			},
		},
		{
			Id:     "my-second-mig",
			Status: "applied",
			Events: []gomigratorintegrationtest.MigrationRecordEventExpectation{
				{
					Action:     "apply",
					Actor:      "gotest",
					Result:     "applied",
					TimeFormat: time.RFC3339,
				},
			},
		},
		{
			Id:     "my-third-mig",
			Status: "applied",
			Events: []gomigratorintegrationtest.MigrationRecordEventExpectation{
				{
					Action:     "apply",
					Actor:      "gotest",
					Result:     "applied",
					TimeFormat: time.RFC3339,
				},
			},
		},
	})
	assert.Equal(t, []map[string]interface{}{
		{
			"level":   "info",
			"message": "Creating migrations table as it was not present.",
		},
		{
			"id":      "my-first-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-first-mig",
			"level":   "debug",
			"message": "no existing entry for migration",
		},
		{
			"id":      "my-second-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-second-mig",
			"level":   "debug",
			"message": "no existing entry for migration",
		},
		{
			"id":      "my-third-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-third-mig",
			"level":   "debug",
			"message": "no existing entry for migration",
		},
	}, gomigratorteststubs.ExtractLogsToMap(b))
}

func TestIntegration_Migrator_Up_FirstApplied(t *testing.T) {
	gomigratorintegrationtest.SkipIfIntegrationTestsNotConfigured(expectedEnvVars, driverSpecificVars, t)
	conn := gomigratorintegrationtest.BuildDBConn(t)
	defer conn.Close()
	defer cleanDB(conn, t)

	b := new(bytes.Buffer)
	l := gomigratorteststubs.BuildZerologLogger(b)

	ml := gomigrator.NewMigrationList(migrations[gomigratorintegrationtest.DBDriver()])
	m, err := gomigrator.NewMigrator(conn, ml, migratorOpts(), l)

	assert.Nil(t, err)

	err = m.Up("my-first-mig")

	assert.Nil(t, err)
	gomigratorintegrationtest.AssertTableExists("my_first_table", conn, t)
	gomigratorintegrationtest.AssertTableDoesNotExist("my_second_table", conn, t)
	gomigratorintegrationtest.AssertTableDoesNotExist("my_third_table", conn, t)
	gomigratorintegrationtest.AssertTableExists("migrations", conn, t)

	if t.Failed() {
		// Stop execution here to aid with debugging
		// All of the below tests will fail with a lot of unneeded output
		return
	}

	gomigratorintegrationtest.AssertMigrationRecordsTable(conn, t, []gomigratorintegrationtest.MigrationRecordExpectation{
		{
			Id:     "my-first-mig",
			Status: "applied",
			Events: []gomigratorintegrationtest.MigrationRecordEventExpectation{
				{
					Action:     "apply",
					Actor:      "gotest",
					Result:     "applied",
					TimeFormat: time.RFC3339,
				},
			},
		},
	})
	assert.Equal(t, []map[string]interface{}{
		{
			"level":   "info",
			"message": "Creating migrations table as it was not present.",
		},
		{
			"id":      "my-first-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-first-mig",
			"level":   "debug",
			"message": "no existing entry for migration",
		},
	}, gomigratorteststubs.ExtractLogsToMap(b))
}

func TestIntegration_Migrator_Down_FullRollback(t *testing.T) {
	gomigratorintegrationtest.SkipIfIntegrationTestsNotConfigured(expectedEnvVars, driverSpecificVars, t)
	conn := gomigratorintegrationtest.BuildDBConn(t)
	defer conn.Close()
	defer cleanDB(conn, t)

	b := new(bytes.Buffer)
	l := gomigratorteststubs.BuildZerologLogger(b)

	ml := gomigrator.NewMigrationList(migrations[gomigratorintegrationtest.DBDriver()])
	m, err := gomigrator.NewMigrator(conn, ml, migratorOpts(), l)

	assert.Nil(t, err)

	err = m.Up(gomigrator.MigrateToLatest)

	if err != nil {
		t.Error("Failed to apply migrations to rollback")
		t.FailNow()
	}

	err = m.Down(gomigrator.MigrateToNothing)

	assert.Nil(t, err)
	gomigratorintegrationtest.AssertTableDoesNotExist("my_first_table", conn, t)
	gomigratorintegrationtest.AssertTableDoesNotExist("my_second_table", conn, t)
	gomigratorintegrationtest.AssertTableDoesNotExist("my_third_table", conn, t)
	gomigratorintegrationtest.AssertTableExists("migrations", conn, t)

	if t.Failed() {
		// Stop execution here to aid with debugging
		// All of the below tests will fail with a lot of unneeded output
		return
	}

	gomigratorintegrationtest.AssertMigrationRecordsTable(conn, t, []gomigratorintegrationtest.MigrationRecordExpectation{
		{
			Id:     "my-first-mig",
			Status: "rolled_back",
			Events: []gomigratorintegrationtest.MigrationRecordEventExpectation{
				{
					Action:     "apply",
					Actor:      "gotest",
					Result:     "applied",
					TimeFormat: time.RFC3339,
				},
				{
					Action:     "rollback",
					Actor:      "gotest",
					Result:     "rolled_back",
					TimeFormat: time.RFC3339,
				},
			},
		},
		{
			Id:     "my-second-mig",
			Status: "rolled_back",
			Events: []gomigratorintegrationtest.MigrationRecordEventExpectation{
				{
					Action:     "apply",
					Actor:      "gotest",
					Result:     "applied",
					TimeFormat: time.RFC3339,
				},
				{
					Action:     "rollback",
					Actor:      "gotest",
					Result:     "rolled_back",
					TimeFormat: time.RFC3339,
				},
			},
		},
		{
			Id:     "my-third-mig",
			Status: "rolled_back",
			Events: []gomigratorintegrationtest.MigrationRecordEventExpectation{
				{
					Action:     "apply",
					Actor:      "gotest",
					Result:     "applied",
					TimeFormat: time.RFC3339,
				},
				{
					Action:     "rollback",
					Actor:      "gotest",
					Result:     "rolled_back",
					TimeFormat: time.RFC3339,
				},
			},
		},
	})
	assert.Equal(t, []map[string]interface{}{
		{
			"level":   "info",
			"message": "Creating migrations table as it was not present.",
		},
		{
			"id":      "my-first-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-first-mig",
			"level":   "debug",
			"message": "no existing entry for migration",
		},
		{
			"id":      "my-second-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-second-mig",
			"level":   "debug",
			"message": "no existing entry for migration",
		},
		{
			"id":      "my-third-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-third-mig",
			"level":   "debug",
			"message": "no existing entry for migration",
		},
		{
			"level":   "debug",
			"message": "skipping create migrations table as it already exists",
		},
		{
			"id":      "my-third-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-third-mig",
			"level":   "debug",
			"message": "found existing entry for migration",
		},
		{
			"id":      "my-second-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-second-mig",
			"level":   "debug",
			"message": "found existing entry for migration",
		},
		{
			"id":      "my-first-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-first-mig",
			"level":   "debug",
			"message": "found existing entry for migration",
		},
	}, gomigratorteststubs.ExtractLogsToMap(b))
}

func TestIntegration_Migrator_Down_RollbackToTarget(t *testing.T) {
	gomigratorintegrationtest.SkipIfIntegrationTestsNotConfigured(expectedEnvVars, driverSpecificVars, t)
	conn := gomigratorintegrationtest.BuildDBConn(t)
	defer conn.Close()
	defer cleanDB(conn, t)

	b := new(bytes.Buffer)
	l := gomigratorteststubs.BuildZerologLogger(b)

	ml := gomigrator.NewMigrationList(migrations[gomigratorintegrationtest.DBDriver()])
	m, err := gomigrator.NewMigrator(conn, ml, migratorOpts(), l)

	assert.Nil(t, err)

	err = m.Up(gomigrator.MigrateToLatest)

	if err != nil {
		t.Error("Failed to apply migrations to rollback")
		t.FailNow()
	}

	err = m.Down("my-second-mig")

	assert.Nil(t, err)
	gomigratorintegrationtest.AssertTableExists("my_first_table", conn, t)
	gomigratorintegrationtest.AssertTableDoesNotExist("my_second_table", conn, t)
	gomigratorintegrationtest.AssertTableDoesNotExist("my_third_table", conn, t)
	gomigratorintegrationtest.AssertTableExists("migrations", conn, t)

	if t.Failed() {
		// Stop execution here to aid with debugging
		// All of the below tests will fail with a lot of unneeded output
		return
	}

	gomigratorintegrationtest.AssertMigrationRecordsTable(conn, t, []gomigratorintegrationtest.MigrationRecordExpectation{
		{
			Id:     "my-first-mig",
			Status: "applied",
			Events: []gomigratorintegrationtest.MigrationRecordEventExpectation{
				{
					Action:     "apply",
					Actor:      "gotest",
					Result:     "applied",
					TimeFormat: time.RFC3339,
				},
			},
		},
		{
			Id:     "my-second-mig",
			Status: "rolled_back",
			Events: []gomigratorintegrationtest.MigrationRecordEventExpectation{
				{
					Action:     "apply",
					Actor:      "gotest",
					Result:     "applied",
					TimeFormat: time.RFC3339,
				},
				{
					Action:     "rollback",
					Actor:      "gotest",
					Result:     "rolled_back",
					TimeFormat: time.RFC3339,
				},
			},
		},
		{
			Id:     "my-third-mig",
			Status: "rolled_back",
			Events: []gomigratorintegrationtest.MigrationRecordEventExpectation{
				{
					Action:     "apply",
					Actor:      "gotest",
					Result:     "applied",
					TimeFormat: time.RFC3339,
				},
				{
					Action:     "rollback",
					Actor:      "gotest",
					Result:     "rolled_back",
					TimeFormat: time.RFC3339,
				},
			},
		},
	})
	assert.Equal(t, []map[string]interface{}{
		{
			"level":   "info",
			"message": "Creating migrations table as it was not present.",
		},
		{
			"id":      "my-first-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-first-mig",
			"level":   "debug",
			"message": "no existing entry for migration",
		},
		{
			"id":      "my-second-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-second-mig",
			"level":   "debug",
			"message": "no existing entry for migration",
		},
		{
			"id":      "my-third-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-third-mig",
			"level":   "debug",
			"message": "no existing entry for migration",
		},
		{
			"level":   "debug",
			"message": "skipping create migrations table as it already exists",
		},
		{
			"id":      "my-third-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-third-mig",
			"level":   "debug",
			"message": "found existing entry for migration",
		},
		{
			"id":      "my-second-mig",
			"level":   "debug",
			"message": "recording state",
		},
		{
			"id":      "my-second-mig",
			"level":   "debug",
			"message": "found existing entry for migration",
		},
	}, gomigratorteststubs.ExtractLogsToMap(b))
}
