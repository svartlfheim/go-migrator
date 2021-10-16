package gomigrator

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	gomigratorteststubs "github.com/svartlfheim/gomigrator/test/stubs"
)

func Test_postgresHandler_DoesTableExist(t *testing.T) {
	tests := []struct {
		name               string
		tableName          string
		opts               Opts
		shouldExist        bool
		expectedErr        error
		expectedErrType    interface{}
		expectedErrMatcher string
		configureDB        func(sqlmock.Sqlmock)
	}{
		{
			name:            "schema opt not set",
			opts:            Opts{},
			shouldExist:     false,
			expectedErrType: ErrInvalidOpts{},
		},
		{
			name:      "query error",
			tableName: "mytable",
			opts: Opts{
				Schema: "myschema",
			},
			configureDB: func(m sqlmock.Sqlmock) {
				q := regexp.QuoteMeta(`
SELECT 
	count(1)
FROM 
	information_schema.tables 
WHERE 
	table_schema = $1 AND 
	table_name = $2;
`)
				m.ExpectQuery(q).WithArgs(
					"myschema",
					"mytable",
				).WillReturnError(
					errors.New("some query error"),
				)
			},
			shouldExist: false,
			expectedErr: errors.New("some query error"),
		},
		{
			name:      "table does exist",
			tableName: "mytable",
			opts: Opts{
				Schema: "myschema",
			},
			configureDB: func(m sqlmock.Sqlmock) {
				q := regexp.QuoteMeta(`
SELECT 
	count(1)
FROM 
	information_schema.tables 
WHERE 
	table_schema = $1 AND 
	table_name = $2;
`)
				m.ExpectQuery(q).WithArgs(
					"myschema",
					"mytable",
				).WillReturnRows(sqlmock.NewRows([]string{"count(1)"}).AddRow("1"))
			},
			shouldExist: true,
			expectedErr: nil,
		},
		{
			name:      "table does not exist",
			tableName: "mytable",
			opts: Opts{
				Schema: "myschema",
			},
			configureDB: func(m sqlmock.Sqlmock) {
				q := regexp.QuoteMeta(`
SELECT 
	count(1)
FROM 
	information_schema.tables 
WHERE 
	table_schema = $1 AND 
	table_name = $2;
`)
				m.ExpectQuery(q).WithArgs(
					"myschema",
					"mytable",
				).WillReturnRows(sqlmock.NewRows([]string{"count(1)"}).AddRow("0"))
			},
			shouldExist: false,
			expectedErr: nil,
		},
		{
			name:      "value does not scan to int",
			tableName: "mytable",
			opts: Opts{
				Schema: "myschema",
			},
			configureDB: func(m sqlmock.Sqlmock) {
				q := regexp.QuoteMeta(`
SELECT 
	count(1)
FROM 
	information_schema.tables 
WHERE 
	table_schema = $1 AND 
	table_name = $2;
`)
				m.ExpectQuery(q).WithArgs(
					"myschema",
					"mytable",
				).WillReturnRows(sqlmock.NewRows([]string{"count(1)"}).AddRow("notanint"))
			},
			shouldExist:        false,
			expectedErrMatcher: "^sql: Scan error.*",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			mockDB, mock, err := sqlmock.New()
			//nolint:staticcheck
			defer mockDB.Close()

			if err != nil {
				t.Error("could not create postgres mock")
			}
			sqlxDB := sqlx.NewDb(mockDB, "postgres")

			if test.configureDB != nil {
				test.configureDB(mock)
			}

			b := new(bytes.Buffer)
			l := gomigratorteststubs.BuildZerologLogger(b)

			h := postgresHandler{
				conn: sqlxDB,
				l:    l,
				opts: test.opts,
			}

			res, err := h.DoesTableExist(test.tableName)

			assert.Equal(tt, test.shouldExist, res)

			if test.expectedErr != nil {
				assert.Equal(tt, test.expectedErr, err)
			} else if test.expectedErrType != nil {
				assert.IsType(tt, test.expectedErrType, err)
			} else if test.expectedErrMatcher != "" {
				assert.Regexp(tt, test.expectedErrMatcher, err.Error())
			} else {
				assert.Nil(tt, err)
			}
		})
	}
}

func Test_postgresHandler_CreateMigrationsTable(t *testing.T) {
	tests := []struct {
		name            string
		tableName       string
		opts            Opts
		expectedErr     error
		expectedErrType interface{}
		expectedLogs    []map[string]interface{}
		configureDB     func(sqlmock.Sqlmock)
	}{
		{
			name:            "error is returned from DoesTableExist",
			opts:            Opts{},
			expectedErrType: ErrInvalidOpts{},
		},
		{
			name: "nothing happens when table exists",
			configureDB: func(m sqlmock.Sqlmock) {
				q := regexp.QuoteMeta(`
SELECT 
	count(1)
FROM 
	information_schema.tables 
WHERE 
	table_schema = $1 AND 
	table_name = $2;
`)
				m.ExpectQuery(q).WithArgs(
					"myschema",
					"migrations",
				).WillReturnRows(sqlmock.NewRows([]string{"count(1)"}).AddRow("1"))
			},
			expectedLogs: []map[string]interface{}{
				{
					"level":   "debug",
					"message": "skipping create migrations table as it already exists",
				},
			},
			opts: Opts{
				Schema: "myschema",
			},
		},
		{
			name: "begin transaction fails",
			opts: Opts{
				Schema: "myschema",
			},
			configureDB: func(m sqlmock.Sqlmock) {
				q := regexp.QuoteMeta(`
SELECT 
	count(1)
FROM 
	information_schema.tables 
WHERE 
	table_schema = $1 AND 
	table_name = $2;
`)
				m.ExpectQuery(q).WithArgs(
					"myschema",
					"migrations",
				).WillReturnRows(sqlmock.NewRows([]string{"count(1)"}).AddRow("0"))

				m.ExpectBegin().WillReturnError(errors.New("cannee do a transaction"))
			},
			expectedLogs: []map[string]interface{}{
				{
					"level":   "info",
					"message": "Creating migrations table as it was not present.",
				},
			},
			expectedErr: ErrCouldNotCreateMigrationsTable{
				Wrapped: errors.New("cannee do a transaction"),
			},
		},
		{
			name: "execute create table fails and is rolled back",
			opts: Opts{
				Schema: "myschema",
			},
			configureDB: func(m sqlmock.Sqlmock) {
				q := regexp.QuoteMeta(`
SELECT 
	count(1)
FROM 
	information_schema.tables 
WHERE 
	table_schema = $1 AND 
	table_name = $2;
`)
				m.ExpectQuery(q).WithArgs(
					"myschema",
					"migrations",
				).WillReturnRows(sqlmock.NewRows([]string{"count(1)"}).AddRow("0"))

				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE migrations(
					id TEXT,
					status TEXT,
					events JSON DEFAULT '[]'::json,
					PRIMARY KEY(id)
				);`)).WillReturnError(errors.New("cannee create that table"))
				m.ExpectRollback()
			},
			expectedLogs: []map[string]interface{}{
				{
					"level":   "info",
					"message": "Creating migrations table as it was not present.",
				},
			},
			expectedErr: ErrCouldNotCreateMigrationsTable{
				Wrapped:       errors.New("cannee create that table"),
				RollbackError: nil,
			},
		},
		{
			name: "execute create table fails and roll back fails",
			opts: Opts{
				Schema: "myschema",
			},
			configureDB: func(m sqlmock.Sqlmock) {
				q := regexp.QuoteMeta(`
SELECT 
	count(1)
FROM 
	information_schema.tables 
WHERE 
	table_schema = $1 AND 
	table_name = $2;
`)
				m.ExpectQuery(q).WithArgs(
					"myschema",
					"migrations",
				).WillReturnRows(sqlmock.NewRows([]string{"count(1)"}).AddRow("0"))

				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE migrations(
					id TEXT,
					status TEXT,
					events JSON DEFAULT '[]'::json,
					PRIMARY KEY(id)
				);`)).WillReturnError(errors.New("cannee create that table"))
				m.ExpectRollback().WillReturnError(errors.New("cannee rollback"))
			},
			expectedLogs: []map[string]interface{}{
				{
					"level":   "info",
					"message": "Creating migrations table as it was not present.",
				},
			},
			expectedErr: ErrCouldNotCreateMigrationsTable{
				Wrapped:       errors.New("cannee create that table"),
				RollbackError: errors.New("cannee rollback"),
			},
		},
		{
			name: "create table commit fails",
			opts: Opts{
				Schema: "myschema",
			},
			configureDB: func(m sqlmock.Sqlmock) {
				q := regexp.QuoteMeta(`
SELECT 
	count(1)
FROM 
	information_schema.tables 
WHERE 
	table_schema = $1 AND 
	table_name = $2;
`)
				m.ExpectQuery(q).WithArgs(
					"myschema",
					"migrations",
				).WillReturnRows(sqlmock.NewRows([]string{"count(1)"}).AddRow("0"))

				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE migrations(
					id TEXT,
					status TEXT,
					events JSON DEFAULT '[]'::json,
					PRIMARY KEY(id)
				);`)).WillReturnResult(sqlmock.NewResult(1, 1))

				m.ExpectCommit().WillReturnError(errors.New("commitment issues are a real thing guys"))
			},
			expectedLogs: []map[string]interface{}{
				{
					"level":   "info",
					"message": "Creating migrations table as it was not present.",
				},
			},
			expectedErr: ErrCouldNotCreateMigrationsTable{
				Wrapped:     errors.New("commitment issues are a real thing guys"),
				CommitError: errors.New("commitment issues are a real thing guys"),
			},
		},
		{
			name: "successfully creates table",
			opts: Opts{
				Schema: "myschema",
			},
			configureDB: func(m sqlmock.Sqlmock) {
				q := regexp.QuoteMeta(`
SELECT 
	count(1)
FROM 
	information_schema.tables 
WHERE 
	table_schema = $1 AND 
	table_name = $2;
`)
				m.ExpectQuery(q).WithArgs(
					"myschema",
					"migrations",
				).WillReturnRows(sqlmock.NewRows([]string{"count(1)"}).AddRow("0"))

				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE migrations(
					id TEXT,
					status TEXT,
					events JSON DEFAULT '[]'::json,
					PRIMARY KEY(id)
				);`)).WillReturnResult(sqlmock.NewResult(1, 1))

				m.ExpectCommit()
			},
			expectedLogs: []map[string]interface{}{
				{
					"level":   "info",
					"message": "Creating migrations table as it was not present.",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			mockDB, mock, err := sqlmock.New()
			//nolint:staticcheck
			defer mockDB.Close()

			if err != nil {
				t.Error("could not create postgres mock")
			}
			sqlxDB := sqlx.NewDb(mockDB, "postgres")

			if test.configureDB != nil {
				test.configureDB(mock)
			}

			b := new(bytes.Buffer)
			l := gomigratorteststubs.BuildZerologLogger(b)

			h := postgresHandler{
				conn: sqlxDB,
				l:    l,
				opts: test.opts,
			}

			err = h.CreateMigrationsTable()

			if test.expectedErr != nil {
				assert.Equal(tt, test.expectedErr, err)
			} else if test.expectedErrType != nil {
				assert.IsType(tt, test.expectedErrType, err)
			} else {
				assert.Nil(tt, err)
			}

			assert.Equal(t, test.expectedLogs, gomigratorteststubs.ExtractLogsToMap(b))
		})
	}
}

func Test_postgresHandler_FetchMigrationFromDb(t *testing.T) {

	tests := []struct {
		name           string
		id             string
		configureDB    func(sqlmock.Sqlmock)
		expectedResult *MigrationRecord
		expectedErr    error
	}{
		{
			name: "error from get",
			id:   "my-migration",
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(
					regexp.QuoteMeta("SELECT * FROM migrations WHERE id=$1"),
				).WithArgs(
					"my-migration",
				).WillReturnError(errors.New("query failed"))
			},
			expectedResult: nil,
			expectedErr:    errors.New("query failed"),
		},

		{
			name: "no rows error from get",
			id:   "my-migration",
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(
					regexp.QuoteMeta("SELECT * FROM migrations WHERE id=$1"),
				).WithArgs(
					"my-migration",
				).WillReturnError(sql.ErrNoRows)
			},
			expectedResult: nil,
			expectedErr:    nil,
		},

		{
			name: "successfully retrieved",
			id:   "my-migration",
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(
					regexp.QuoteMeta("SELECT * FROM migrations WHERE id=$1"),
				).WithArgs(
					"my-migration",
				).WillReturnRows(
					sqlmock.NewRows(
						[]string{"id", "status", "events"},
					).AddRow(
						"my-migration", "applied", "[]",
					))
			},
			expectedResult: &MigrationRecord{
				Id:     "my-migration",
				Status: "applied",
				Events: []MigrationRecordEvent{},
			},
			expectedErr: nil,
		},

		{
			name: "successfully retrieved with some events",
			id:   "my-migration",
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(
					regexp.QuoteMeta("SELECT * FROM migrations WHERE id=$1"),
				).WithArgs(
					"my-migration",
				).WillReturnRows(
					sqlmock.NewRows(
						[]string{"id", "status", "events"},
					).AddRow(
						"my-migration", "applied", "[{\"action\":\"apply\", \"actor\":\"joe\", \"performedat\":\"2021-10-11T09:00:00Z\", \"result\":\"applied\"}]",
					))
			},
			expectedResult: &MigrationRecord{
				Id:     "my-migration",
				Status: "applied",
				Events: []MigrationRecordEvent{
					{
						Action:      "apply",
						Actor:       "joe",
						PerformedAt: "2021-10-11T09:00:00Z",
						Result:      "applied",
					},
				},
			},
			expectedErr: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			mockDB, mock, err := sqlmock.New()
			//nolint:staticcheck
			defer mockDB.Close()

			if err != nil {
				t.Error("could not create postgres mock")
			}
			sqlxDB := sqlx.NewDb(mockDB, "postgres")

			if test.configureDB != nil {
				test.configureDB(mock)
			}

			b := new(bytes.Buffer)
			l := gomigratorteststubs.BuildZerologLogger(b)

			h := postgresHandler{
				conn: sqlxDB,
				l:    l,
			}

			res, err := h.FetchMigrationFromDb(test.id)

			assert.Equal(tt, test.expectedResult, res)
			if test.expectedErr != nil {
				assert.Equal(tt, test.expectedErr, err)
			}
		})
	}
}

type eventJsonSpec struct {
	action      string
	actor       string
	result      string
	timePattern string
}
type eventJsonArg struct {
	specs []eventJsonSpec
}

func (a eventJsonArg) Match(v driver.Value) bool {
	val, ok := v.(string)

	if !ok {
		return false
	}

	mList := []MigrationRecordEvent{}
	err := json.Unmarshal([]byte(val), &mList)

	if err != nil {
		return false
	}

	if len(mList) != len(a.specs) {
		return false
	}

	for i, s := range a.specs {

		m := mList[i]
		_, err = time.Parse(s.timePattern, m.PerformedAt)

		if err != nil || m.Action != s.action || m.Actor != s.actor || m.Result != s.result {
			return false
		}
	}

	return true
}

func Test_postgresHandler_RecordMigrationInDb(t *testing.T) {
	tests := []struct {
		name         string
		id           string
		state        MigrationState
		action       string
		opts         Opts
		configureDB  func(sqlmock.Sqlmock)
		expectedLogs []map[string]interface{}
		expectedErr  error
	}{
		{
			name:   "error fetching migration",
			id:     "my-migration",
			state:  MigrationApplied,
			action: "apply",
			opts: Opts{
				Applyer: "joe",
			},
			expectedLogs: []map[string]interface{}{
				{
					"level":   "debug",
					"message": "recording state",
					"id":      "my-migration",
				},
				{
					"level":   "error",
					"error":   "query failed",
					"message": "error recording migration state",
					"id":      "my-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(
					regexp.QuoteMeta("SELECT * FROM migrations WHERE id=$1"),
				).WithArgs(
					"my-migration",
				).WillReturnError(errors.New("query failed"))
			},
			expectedErr: errors.New("query failed"),
		},

		{
			name:   "inserting: exec fails",
			id:     "my-migration",
			state:  MigrationApplied,
			action: "apply",
			opts: Opts{
				Applyer: "joe",
			},
			expectedLogs: []map[string]interface{}{
				{
					"level":   "debug",
					"message": "recording state",
					"id":      "my-migration",
				},
				{
					"level":   "debug",
					"message": "no existing entry for migration",
					"id":      "my-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {

				m.ExpectQuery(
					regexp.QuoteMeta("SELECT * FROM migrations WHERE id=$1"),
				).WithArgs(
					"my-migration",
				).WillReturnError(sql.ErrNoRows)

				m.ExpectExec(
					regexp.QuoteMeta("INSERT INTO migrations(id, status, events) VALUES($1, $2, $3);"),
				).WithArgs(
					"my-migration",
					string(MigrationApplied),
					eventJsonArg{
						specs: []eventJsonSpec{
							{
								action:      "apply",
								actor:       "joe",
								result:      string(MigrationApplied),
								timePattern: time.RFC3339,
							},
						},
					},
				).WillReturnError(errors.New("insert failed"))
			},
			expectedErr: errors.New("insert failed"),
		},

		{
			name:   "inserting: successful",
			id:     "my-migration",
			state:  MigrationApplied,
			action: "apply",
			opts: Opts{
				Applyer: "joe",
			},
			expectedLogs: []map[string]interface{}{
				{
					"level":   "debug",
					"message": "recording state",
					"id":      "my-migration",
				},
				{
					"level":   "debug",
					"message": "no existing entry for migration",
					"id":      "my-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {

				m.ExpectQuery(
					regexp.QuoteMeta("SELECT * FROM migrations WHERE id=$1"),
				).WithArgs(
					"my-migration",
				).WillReturnError(sql.ErrNoRows)

				m.ExpectExec(
					regexp.QuoteMeta("INSERT INTO migrations(id, status, events) VALUES($1, $2, $3);"),
				).WithArgs(
					"my-migration",
					string(MigrationApplied),
					eventJsonArg{
						specs: []eventJsonSpec{
							{
								action:      "apply",
								actor:       "joe",
								result:      string(MigrationApplied),
								timePattern: time.RFC3339,
							},
						},
					},
				).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectedErr: nil,
		},

		{
			name:   "updating: exec fails",
			id:     "my-migration",
			state:  MigrationApplied,
			action: "apply",
			opts: Opts{
				Applyer: "joe",
			},
			expectedLogs: []map[string]interface{}{
				{
					"level":   "debug",
					"message": "recording state",
					"id":      "my-migration",
				},
				{
					"level":   "debug",
					"message": "found existing entry for migration",
					"id":      "my-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {

				m.ExpectQuery(
					regexp.QuoteMeta("SELECT * FROM migrations WHERE id=$1"),
				).WithArgs(
					"my-migration",
				).WillReturnRows(
					sqlmock.NewRows(
						[]string{"id", "status", "events"},
					).AddRow(
						"my-migration", "applied", "[{\"action\":\"apply\", \"actor\":\"bob\", \"performedat\":\"2021-10-11T09:00:00Z\", \"result\":\"applied\"}]",
					))

				m.ExpectExec(
					regexp.QuoteMeta("UPDATE migrations SET status=$1, events=$2 WHERE id=$3"),
				).WithArgs(
					string(MigrationApplied),
					eventJsonArg{
						specs: []eventJsonSpec{
							{
								action:      "apply",
								actor:       "bob",
								result:      string(MigrationApplied),
								timePattern: time.RFC3339,
							},
							{
								action:      "apply",
								actor:       "joe",
								result:      string(MigrationApplied),
								timePattern: time.RFC3339,
							},
						},
					},
					"my-migration",
				).WillReturnError(errors.New("update failed"))
			},
			expectedErr: errors.New("update failed"),
		},

		{
			name:   "updating: successful",
			id:     "my-migration",
			state:  MigrationApplied,
			action: "apply",
			opts: Opts{
				Applyer: "joe",
			},
			expectedLogs: []map[string]interface{}{
				{
					"level":   "debug",
					"message": "recording state",
					"id":      "my-migration",
				},
				{
					"level":   "debug",
					"message": "found existing entry for migration",
					"id":      "my-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {

				m.ExpectQuery(
					regexp.QuoteMeta("SELECT * FROM migrations WHERE id=$1"),
				).WithArgs(
					"my-migration",
				).WillReturnRows(
					sqlmock.NewRows(
						[]string{"id", "status", "events"},
					).AddRow(
						"my-migration", "applied", "[{\"action\":\"apply\", \"actor\":\"bob\", \"performedat\":\"2021-10-11T09:00:00Z\", \"result\":\"applied\"}]",
					))

				m.ExpectExec(
					regexp.QuoteMeta("UPDATE migrations SET status=$1, events=$2 WHERE id=$3"),
				).WithArgs(
					string(MigrationApplied),
					eventJsonArg{
						specs: []eventJsonSpec{
							{
								action:      "apply",
								actor:       "bob",
								result:      string(MigrationApplied),
								timePattern: time.RFC3339,
							},
							{
								action:      "apply",
								actor:       "joe",
								result:      string(MigrationApplied),
								timePattern: time.RFC3339,
							},
						},
					},
					"my-migration",
				).WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectedErr: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			mockDB, mock, err := sqlmock.New()
			//nolint:staticcheck
			defer mockDB.Close()

			if err != nil {
				t.Error("could not create postgres mock")
			}
			sqlxDB := sqlx.NewDb(mockDB, "postgres")

			if test.configureDB != nil {
				test.configureDB(mock)
			}

			b := new(bytes.Buffer)
			l := gomigratorteststubs.BuildZerologLogger(b)

			h := postgresHandler{
				conn: sqlxDB,
				l:    l,
				opts: test.opts,
			}

			err = h.RecordMigrationInDb(test.id, test.state, test.action)

			assert.Equal(tt, test.expectedErr, err)

			assert.Equal(t, test.expectedLogs, gomigratorteststubs.ExtractLogsToMap(b))
		})
	}
}
