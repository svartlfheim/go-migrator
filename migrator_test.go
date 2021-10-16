package gomigrator

import (
	"bytes"
	"database/sql"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	gomigratorteststubs "github.com/svartlfheim/gomigrator/test/stubs"
)

func Test_NewMigrator_Postgres(t *testing.T) {
	mockDB, _, err := sqlmock.New()
	defer mockDB.Close()
	sqlxDB := sqlx.NewDb(mockDB, "postgres")

	b := new(bytes.Buffer)
	l := gomigratorteststubs.BuildZerologLogger(b)

	migrator, err := NewMigrator(sqlxDB, MigrationList{}, Opts{}, l)

	assert.Nil(t, err)
	assert.IsType(t, &postgresHandler{}, migrator.handler)
}

func Test_NewMigrator_UnknownDriver(t *testing.T) {
	mockDB, _, err := sqlmock.New()
	defer mockDB.Close()
	sqlxDB := sqlx.NewDb(mockDB, "unknown-driver")

	b := new(bytes.Buffer)
	l := gomigratorteststubs.BuildZerologLogger(b)

	migrator, err := NewMigrator(sqlxDB, MigrationList{}, Opts{}, l)

	assert.IsType(t, ErrMigrationsNotImplementedForDriver{}, err)
	assert.Nil(t, migrator)
}

func Test_Migrator_ListMigrations(t *testing.T) {
	tests := []struct{
		name string
		h *handlerStub
		migs MigrationList
		expectedErr error
		expectedMigs []*MigrationRecord
	}{
		{
			name: "Fails to create migrations table",
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					errors.New("not today"),
				},
			},
			expectedErr: errors.New("not today"),
			expectedMigs: []*MigrationRecord{},
		},

		{
			name: "Lists empty set",
			migs: MigrationList{},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},
			},
			expectedErr: nil,
			expectedMigs: []*MigrationRecord{},
		},

		{
			name: "Returns error when fetching from db",
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						Err: errors.New("could not connect to db i guess"),
						MigrationRecord: nil,
					},
				},
			},
			expectedErr: errors.New("could not connect to db i guess"),
			expectedMigs: []*MigrationRecord{},
		},

		{
			name: "Returns migrations when 1 is in db",
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						Err: nil,
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
					},
				},
			},
			expectedErr: nil,
			expectedMigs: []*MigrationRecord{
				{
					Id: "my-first-migration",
					Status: string(MigrationApplied),
				},
			},
		},

		{
			name: "Returns migrations when all are in db",
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
					{
						Id: "my-second-migration",
					},
					{
						Id: "my-third-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						Err: nil,
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
					},
					{
						ExpectedId: "my-second-migration",
						Err: nil,
						MigrationRecord: &MigrationRecord{
							Id: "my-second-migration",
							Status: string(MigrationApplied),
						},
					},
					{
						ExpectedId: "my-third-migration",
						Err: nil,
						MigrationRecord: &MigrationRecord{
							Id: "my-third-migration",
							Status: string(MigrationApplied),
						},
					},
				},
			},
			expectedErr: nil,
			expectedMigs: []*MigrationRecord{
				{
					Id: "my-first-migration",
					Status: string(MigrationApplied),
				},
				{
					Id: "my-second-migration",
					Status: string(MigrationApplied),
				},
				{
					Id: "my-third-migration",
					Status: string(MigrationApplied),
				},
			},
		},

		{
			name: "Error is returned when fetching second migration",
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
					{
						Id: "my-second-migration",
					},
					{
						Id: "my-third-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						Err: nil,
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
					},
					{
						ExpectedId: "my-second-migration",
						Err: errors.New("could not get second migraion"),
						MigrationRecord: nil,
					},
				},
			},
			expectedErr: errors.New("could not get second migraion"),
			expectedMigs: []*MigrationRecord{
				{
					Id: "my-first-migration",
					Status: string(MigrationApplied),
				},
			},
		},

		{
			name: "Latest migration is not in DB",
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
					{
						Id: "my-second-migration",
					},
					{
						Id: "my-third-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						Err: nil,
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
					},
					{
						ExpectedId: "my-second-migration",
						Err: nil,
						MigrationRecord: &MigrationRecord{
							Id: "my-second-migration",
							Status: string(MigrationApplied),
						},
					},
					{
						ExpectedId: "my-third-migration",
						Err: nil,
						MigrationRecord: nil,
					},
				},
			},
			expectedErr: nil,
			expectedMigs: []*MigrationRecord{
				{
					Id: "my-first-migration",
					Status: string(MigrationApplied),
				},
				{
					Id: "my-second-migration",
					Status: string(MigrationApplied),
				},
				{
					Id: "my-third-migration",
					Status: string(MigrationPending),
				},
			},
		},
	}

	for _, test := range(tests) {
		t.Run(test.name, func(tt *testing.T) {

			sqlDB, _, err := sqlmock.New()
			dbConn := sqlx.NewDb(sqlDB, "irrelevant")

			defer sqlDB.Close()

			if err != nil {
				tt.Error("failed to build db connection")
				return
			}
		
			m := Migrator{
				handler: test.h,
				conn: dbConn,
				opts: Opts{},
				migs: test.migs,
			}
		
			res, err := m.ListMigrations()
		
			assert.Equal(t, test.expectedMigs, res)
			assert.Equal(t, test.expectedErr, err)
		})
	}
}



func Test_Migrator_Up(t *testing.T) {
	tests := []struct{
		name string
		buildDB func() (*sqlx.DB, *sql.DB, sqlmock.Sqlmock, error)
		configureDB func(sqlmock.Sqlmock)
		h *handlerStub
		migs MigrationList
		target string
		expectedLogs []map[string]interface{}
		expectedErr error
		expectedErrType interface{}
	}{
		{
			name: "Fails to create migrations table",
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					errors.New("not today"),
				},
			},
			expectedErr: errors.New("not today"),
		},

		{
			name: "Empty target",
			migs: MigrationList{},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},
			},
			expectedErr: ErrInvalidMigrationInstruction{
				Message: "migration target must be set",
			},
		},

		{
			name: "To latest with empty migration list",
			migs: MigrationList{},
			target: MigrateToLatest,
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},
			},
			expectedErr: ErrNoMigrationsFound{},
		},

		{
			name: "To specific target that can't be found",
			target: "my-missing-migration",
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},
			},
			expectedErrType: ErrInvalidMigrationInstruction{},
		},

		{
			name: "error fetching first from db",
			target: MigrateToLatest,
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: errors.New("could not connect for first migration"),
					},
				},
			},
			expectedErr: errors.New("could not connect for first migration"),
		},

		{
			name: "create transaction fails",
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "applying",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin().WillReturnError(errors.New("could not begin transaction"))
			},
			target: MigrateToLatest,
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: nil,
					},
				},
			},
			expectedErr: errors.New("could not begin transaction"),
		},

		{
			name: "migration fails, and is rolled back",
			target: MigrateToLatest,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "applying",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE first_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				).WillReturnError(
					errors.New("failed to run migration"),
				)

				m.ExpectRollback()
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE first_migration (
								id uuid NOT NULL
							)`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: nil,
					},
				},
			},
			expectedErr: ErrCouldNotExecuteMigration{
				Name: "my-first-migration",
				Wrapped: errors.New("failed to run migration"),
				RollbackError: nil,
			},
		},

		{
			name: "migration fails and rollback fails",
			target: MigrateToLatest,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "applying",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE first_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				).WillReturnError(
					errors.New("failed to run migration"),
				)

				m.ExpectRollback().WillReturnError(errors.New("cannee rollback"))
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE first_migration (
								id uuid NOT NULL
							)`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: nil,
					},
				},
			},
			expectedErr: ErrCouldNotExecuteMigration{
				Name: "my-first-migration",
				Wrapped: errors.New("failed to run migration"),
				RollbackError: errors.New("cannee rollback"),
			},
		},

		{
			name: "migration succeeds and is recorded",
			target: MigrateToLatest,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "applying",
					"id": "my-first-migration",
				},
				{
					"level": "info",
					"message": "successfully applied",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE first_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)

				m.ExpectCommit()
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE first_migration (
								id uuid NOT NULL
							)`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationApplied,
						ExpectedAction: "apply",
						Err: nil,
					},
				},
			},
			expectedErr: nil,
		},

		{
			name: "migration succeeds but recording fails",
			target: MigrateToLatest,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "applying",
					"id": "my-first-migration",
				},
				{
					"level": "info",
					"message": "successfully applied",
					"id": "my-first-migration",
				},
				{
					"level": "error",
					"error": "failed to record",
					"message": "failed to record migration state for successful migration",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE first_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)

				m.ExpectCommit()
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE first_migration (
								id uuid NOT NULL
							)`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationApplied,
						ExpectedAction: "apply",
						Err: errors.New("failed to record"),
					},
				},
			},
			expectedErr: nil,
		},

		{
			name: "migration commit fails and is recorded correctly",
			target: MigrateToLatest,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "applying",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE first_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)

				m.ExpectCommit().WillReturnError(errors.New("could not commit"))
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE first_migration (
								id uuid NOT NULL
							)`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationFailed,
						ExpectedAction: "apply",
						Err: nil,
					},
				},
			},
			expectedErr: ErrCouldNotExecuteMigration{
				Name: "my-first-migration",
				CommitError: errors.New("could not commit"),
			},
		},

		{
			name: "migration commit fails and recording fails",
			target: MigrateToLatest,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "applying",
					"id": "my-first-migration",
				},
				{
					"level": "error",
					"error": "failed to record",
					"message": "failed to record migration state for failed apply",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE first_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)

				m.ExpectCommit().WillReturnError(errors.New("could not commit"))
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE first_migration (
								id uuid NOT NULL
							)`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationFailed,
						ExpectedAction: "apply",
						Err: errors.New("failed to record"),
					},
				},
			},
			expectedErr: ErrCouldNotExecuteMigration{
				Name: "my-first-migration",
				CommitError: errors.New("could not commit"),
			},
		},

		{
			name: "applied migration is skipped",
			target: MigrateToLatest,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "already applied",
					"id": "my-first-migration",
				},
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE first_migration (
								id uuid NOT NULL
							)`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},
			},
			expectedErr: nil,
		},

		// TODO...
		{
			name: "multiple migrations applied successfully",
			target: MigrateToLatest,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "applying",
					"id": "my-first-migration",
				},
				{
					"level": "info",
					"message": "successfully applied",
					"id": "my-first-migration",
				},
				{
					"level": "info",
					"message": "applying",
					"id": "my-second-migration",
				},
				{
					"level": "info",
					"message": "successfully applied",
					"id": "my-second-migration",
				},
				{
					"level": "info",
					"message": "applying",
					"id": "my-third-migration",
				},
				{
					"level": "info",
					"message": "successfully applied",
					"id": "my-third-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE first_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit()

				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE second_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit()

				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE third_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit()
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE first_migration (
								id uuid NOT NULL
							)`)
						},
					},
					{
						Id: "my-second-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE second_migration (
								id uuid NOT NULL
							)`)
						},
					},
					{
						Id: "my-third-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE third_migration (
								id uuid NOT NULL
							)`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: nil,
					},
					{
						ExpectedId: "my-second-migration",
						MigrationRecord: nil,
						Err: nil,
					},
					{
						ExpectedId: "my-third-migration",
						MigrationRecord: nil,
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationApplied,
						ExpectedAction: "apply",
						Err: nil,
					},
					{
						ExpectedId: "my-second-migration",
						ExpectedState: MigrationApplied,
						ExpectedAction: "apply",
						Err: nil,
					},
					{
						ExpectedId: "my-third-migration",
						ExpectedState: MigrationApplied,
						ExpectedAction: "apply",
						Err: nil,
					},
				},
			},
			expectedErr: nil,
		},

		{
			name: "first of multiple migrations fails",
			target: MigrateToLatest,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "applying",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE first_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)

				m.ExpectCommit().WillReturnError(errors.New("could not commit first"))
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE first_migration (
								id uuid NOT NULL
							)`)
						},
					},
					{
						Id: "my-second-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE second_migration (
								id uuid NOT NULL
							)`)
						},
					},
					{
						Id: "my-third-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE third_migration (
								id uuid NOT NULL
							)`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationFailed,
						ExpectedAction: "apply",
						Err: nil,
					},
				},
			},
			expectedErr: ErrCouldNotExecuteMigration{
				Name: "my-first-migration",
				CommitError: errors.New("could not commit first"),
			},
		},

		{
			name: "secondary migration fails after initial success",
			target: MigrateToLatest,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "applying",
					"id": "my-first-migration",
				},
				{
					"level": "info",
					"message": "successfully applied",
					"id": "my-first-migration",
				},
				{
					"level": "info",
					"message": "applying",
					"id": "my-second-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE first_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit()

				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE second_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit().WillReturnError(errors.New("could not commit second"))
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE first_migration (
								id uuid NOT NULL
							)`)
						},
					},
					{
						Id: "my-second-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE second_migration (
								id uuid NOT NULL
							)`)
						},
					},
					{
						Id: "my-third-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE third_migration (
								id uuid NOT NULL
							)`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: nil,
					},
					{
						ExpectedId: "my-second-migration",
						MigrationRecord: nil,
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationApplied,
						ExpectedAction: "apply",
						Err: nil,
					},
					{
						ExpectedId: "my-second-migration",
						ExpectedState: MigrationFailed,
						ExpectedAction: "apply",
						Err: nil,
					},
				},
			},
			expectedErr: ErrCouldNotExecuteMigration{
				Name: "my-second-migration",
				CommitError: errors.New("could not commit second"),
			},
		},

		{
			name: "first migration is skipped",
			target: MigrateToLatest,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "already applied",
					"id": "my-first-migration",
				},
				{
					"level": "info",
					"message": "applying",
					"id": "my-second-migration",
				},
				{
					"level": "info",
					"message": "successfully applied",
					"id": "my-second-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`CREATE TABLE second_migration (
					id uuid NOT NULL
				)`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit()
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE first_migration (
								id uuid NOT NULL
							)`)
						},
					},
					{
						Id: "my-second-migration",
						Execute: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`CREATE TABLE second_migration (
								id uuid NOT NULL
							)`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
					{
						ExpectedId: "my-second-migration",
						MigrationRecord: nil,
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-second-migration",
						ExpectedState: MigrationApplied,
						ExpectedAction: "apply",
						Err: nil,
					},
				},
			},
			expectedErr: nil,
		},
	}

	for _, test := range(tests) {
		t.Run(test.name, func(tt *testing.T) {
		
			sqlMock, mock, err := sqlmock.New()
			dbConn := sqlx.NewDb(sqlMock, "irrelevant")

			defer sqlMock.Close()

			if err != nil {
				tt.Error("failed to build database mock")
				return
			}

			if test.configureDB != nil {
				test.configureDB(mock)
			}

			b := new(bytes.Buffer)
			logger := gomigratorteststubs.BuildZerologLogger(b)

			m := Migrator{
				handler: test.h,
				conn: dbConn,
				opts: Opts{},
				migs: test.migs,
				logger: logger,
			}
		
			err = m.Up(test.target)
		
			if test.expectedErr == nil && test.expectedErrType != nil {
				assert.IsType(t, test.expectedErrType, err)
			} else {
				assert.Equal(t, test.expectedErr, err)
			}

			assert.Equal(t, test.expectedLogs, gomigratorteststubs.ExtractLogsToMap(b))
		})
	}
}

func Test_Migrator_Down(t *testing.T) {
	tests := []struct{
		name string
		buildDB func() (*sqlx.DB, *sql.DB, sqlmock.Sqlmock, error)
		configureDB func(sqlmock.Sqlmock)
		h *handlerStub
		migs MigrationList
		target string
		expectedLogs []map[string]interface{}
		expectedErr error
		expectedErrType interface{}
	}{
		{
			name: "Fails to create migrations table",
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					errors.New("not today"),
				},
			},
			expectedErr: errors.New("not today"),
		},

		{
			name: "Empty target",
			migs: MigrationList{},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},
			},
			expectedErr: ErrInvalidMigrationInstruction{
				Message: "migration target must be set",
			},
		},

		{
			name: "To first with empty migration list",
			migs: MigrationList{},
			target: MigrateToNothing,
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},
			},
			expectedErr: ErrNoMigrationsFound{},
		},

		{
			name: "To specific target that can't be found",
			target: "my-missing-migration",
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},
			},
			expectedErrType: ErrInvalidMigrationInstruction{},
		},

		{
			name: "error fetching first from db",
			target: MigrateToNothing,
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: nil,
						Err: errors.New("could not connect for first migration"),
					},
				},
			},
			expectedErr: errors.New("could not connect for first migration"),
		},

		{
			name: "create transaction fails",
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin().WillReturnError(errors.New("could not begin transaction"))
			},
			target: MigrateToNothing,
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},
			},
			expectedErr: errors.New("could not begin transaction"),
		},

		{
			name: "skipped if rolled back",
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "skipping rollback; not applied",
					"id": "my-first-migration",
				},
			},
			target: MigrateToNothing,
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationRolledBack),
						},
						Err: nil,
					},
				},
			},
			expectedErr: nil,
		},

		{
			name: "skipped if pending",
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "skipping rollback; not applied",
					"id": "my-first-migration",
				},
			},
			target: MigrateToNothing,
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationPending),
						},
						Err: nil,
					},
				},
			},
			expectedErr: nil,
		},

		{
			name: "migration fails, and is rolled back",
			target: MigrateToNothing,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE first_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				).WillReturnError(
					errors.New("failed to run rollback"),
				)

				m.ExpectRollback()
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE first_migration`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},
			},
			expectedErr: ErrCouldNotRollbackMigration{
				Name: "my-first-migration",
				Wrapped: errors.New("failed to run rollback"),
				RollbackError: nil,
			},
		},

		{
			name: "migration fails and rollback fails",
			target: MigrateToNothing,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE first_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				).WillReturnError(
					errors.New("failed to run migration"),
				)

				m.ExpectRollback().WillReturnError(errors.New("cannee rollback"))
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE first_migration`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-miration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},
			},
			expectedErr: ErrCouldNotRollbackMigration{
				Name: "my-first-migration",
				Wrapped: errors.New("failed to run migration"),
				RollbackError: errors.New("cannee rollback"),
			},
		},

		{
			name: "migration succeeds and is recorded",
			target: MigrateToNothing,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-first-migration",
				},
				{
					"level": "info",
					"message": "successfully rolled back",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE first_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)

				m.ExpectCommit()
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE first_migration`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationRolledBack,
						ExpectedAction: "rollback",
						Err: nil,
					},
				},
			},
			expectedErr: nil,
		},

		{
			name: "migration succeeds but recording fails",
			target: MigrateToNothing,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-first-migration",
				},
				{
					"level": "info",
					"message": "successfully rolled back",
					"id": "my-first-migration",
				},
				{
					"level": "error",
					"error": "failed to record",
					"message": "failed to record migration state for successful rollback",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE first_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)

				m.ExpectCommit()
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE first_migration`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationRolledBack,
						ExpectedAction: "rollback",
						Err: errors.New("failed to record"),
					},
				},
			},
			expectedErr: nil,
		},

		{
			name: "migration commit fails and is recorded correctly",
			target: MigrateToNothing,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE first_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)

				m.ExpectCommit().WillReturnError(errors.New("could not commit"))
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE first_migration`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationRollbackFailed,
						ExpectedAction: "rollback",
						Err: nil,
					},
				},
			},
			expectedErr: ErrCouldNotRollbackMigration{
				Name: "my-first-migration",
				CommitError: errors.New("could not commit"),
			},
		},

		{
			name: "migration commit fails and recording fails",
			target: MigrateToNothing,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-first-migration",
				},
				{
					"level": "error",
					"error": "failed to record",
					"message": "failed to record migration state for failed roll back",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE first_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)

				m.ExpectCommit().WillReturnError(errors.New("could not commit"))
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE first_migration`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationRollbackFailed,
						ExpectedAction: "rollback",
						Err: errors.New("failed to record"),
					},
				},
			},
			expectedErr: ErrCouldNotRollbackMigration{
				Name: "my-first-migration",
				CommitError: errors.New("could not commit"),
			},
		},

		{
			name: "multiple migrations rolled back successfully",
			target: MigrateToNothing,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-third-migration",
				},
				{
					"level": "info",
					"message": "successfully rolled back",
					"id": "my-third-migration",
				},
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-second-migration",
				},
				{
					"level": "info",
					"message": "successfully rolled back",
					"id": "my-second-migration",
				},
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-first-migration",
				},
				{
					"level": "info",
					"message": "successfully rolled back",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE third_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit()

				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE second_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit()

				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE first_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit()
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE first_migration`)
						},
					},
					{
						Id: "my-second-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE second_migration`)
						},
					},
					{
						Id: "my-third-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE third_migration`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-third-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-third-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
					{
						ExpectedId: "my-second-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-second-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-third-migration",
						ExpectedState: MigrationRolledBack,
						ExpectedAction: "rollback",
						Err: nil,
					},
					{
						ExpectedId: "my-second-migration",
						ExpectedState: MigrationRolledBack,
						ExpectedAction: "rollback",
						Err: nil,
					},
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationRolledBack,
						ExpectedAction: "rollback",
						Err: nil,
					},
				},
			},
			expectedErr: nil,
		},

		{
			name: "first of multiple migrations fails",
			target: MigrateToNothing,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-third-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()

				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE third_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)

				m.ExpectCommit().WillReturnError(errors.New("could not commit third"))
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE first_migration`)
						},
					},
					{
						Id: "my-second-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE second_migration`)
						},
					},
					{
						Id: "my-third-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE third_migration`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-third-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-third-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-third-migration",
						ExpectedState: MigrationRollbackFailed,
						ExpectedAction: "rollback",
						Err: nil,
					},
				},
			},
			expectedErr: ErrCouldNotRollbackMigration{
				Name: "my-third-migration",
				CommitError: errors.New("could not commit third"),
			},
		},

		{
			name: "secondary migration fails after initial success",
			target: MigrateToNothing,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-third-migration",
				},
				{
					"level": "info",
					"message": "successfully rolled back",
					"id": "my-third-migration",
				},
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-second-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE third_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit()

				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE second_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit().WillReturnError(errors.New("could not commit second"))
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE first_migration`)
						},
					},
					{
						Id: "my-second-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE second_migration`)
						},
					},
					{
						Id: "my-third-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE third_migration`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-third-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-third-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
					{
						ExpectedId: "my-second-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-second-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-third-migration",
						ExpectedState: MigrationRolledBack,
						ExpectedAction: "rollback",
						Err: nil,
					},
					{
						ExpectedId: "my-second-migration",
						ExpectedState: MigrationRollbackFailed,
						ExpectedAction: "rollback",
						Err: nil,
					},
				},
			},
			expectedErr: ErrCouldNotRollbackMigration{
				Name: "my-second-migration",
				CommitError: errors.New("could not commit second"),
			},
		},

		{
			name: "non-applied migration is skipped",
			target: MigrateToNothing,
			expectedLogs: []map[string]interface{}{
				{
					"level": "info",
					"message": "skipping rollback; not applied",
					"id": "my-second-migration",
				},
				{
					"level": "info",
					"message": "rolling back",
					"id": "my-first-migration",
				},
				{
					"level": "info",
					"message": "successfully rolled back",
					"id": "my-first-migration",
				},
			},
			configureDB: func(m sqlmock.Sqlmock) {
				m.ExpectBegin()
				m.ExpectExec(regexp.QuoteMeta(`DROP TABLE first_migration`)).WillReturnResult(
					sqlmock.NewResult(1, 1),
				)
				m.ExpectCommit()
			},
			migs: MigrationList{
				migrations: []Migration{
					{
						Id: "my-first-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE first_migration`)
						},
					},
					{
						Id: "my-second-migration",
						Rollback: func(tx *sqlx.Tx) (sql.Result, error) {
							return tx.Exec(`DROP TABLE second_migration`)
						},
					},
				},
			},
			h: &handlerStub{
				T: t,

				CreateMigrationsTableCalls: []error{
					nil,
				},

				FetchMigrationFromDbCalls: []handlerFetchMigrationFromDbCall{
					{
						ExpectedId: "my-second-migration",
						MigrationRecord: nil,
						Err: nil,
					},
					{
						ExpectedId: "my-first-migration",
						MigrationRecord: &MigrationRecord{
							Id: "my-first-migration",
							Status: string(MigrationApplied),
						},
						Err: nil,
					},
				},

				RecordMigrationInDbCalls: []handlerRecordMigrationInDbCall{
					{
						ExpectedId: "my-first-migration",
						ExpectedState: MigrationRolledBack,
						ExpectedAction: "rollback",
						Err: nil,
					},
				},
			},
			expectedErr: nil,
		},
	}

	for _, test := range(tests) {
		t.Run(test.name, func(tt *testing.T) {
		
			sqlMock, mock, err := sqlmock.New()
			dbConn := sqlx.NewDb(sqlMock, "irrelevant")

			defer sqlMock.Close()

			if err != nil {
				tt.Error("failed to build database mock")
				return
			}

			if test.configureDB != nil {
				test.configureDB(mock)
			}

			b := new(bytes.Buffer)
			logger := gomigratorteststubs.BuildZerologLogger(b)

			m := Migrator{
				handler: test.h,
				conn: dbConn,
				opts: Opts{},
				migs: test.migs,
				logger: logger,
			}
		
			err = m.Down(test.target)
		
			if test.expectedErr == nil && test.expectedErrType != nil {
				assert.IsType(t, test.expectedErrType, err)
			} else {
				assert.Equal(t, test.expectedErr, err)
			}

			assert.Equal(t, test.expectedLogs, gomigratorteststubs.ExtractLogsToMap(b))
		})
	}
}