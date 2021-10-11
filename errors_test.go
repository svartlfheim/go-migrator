package gomigrator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ErrCouldNotInitialiseMigrationsTable_RollbackError(t *testing.T) {
	err := ErrCouldNotCreateMigrationsTable{
		RollbackError: errors.New("rollback failed"),
		Wrapped:       errors.New("the root cause"),
	}

	assert.Equal(t, "error rolling back after failed migrations initialisation, rollback error: rollback failed (the root cause)", err.Error())
}

func Test_ErrCouldNotInitialiseMigrationsTable_CommitError(t *testing.T) {
	err := ErrCouldNotCreateMigrationsTable{
		CommitError: errors.New("commit failed"),
		Wrapped:     errors.New("the root cause"),
	}

	assert.Equal(t, "error commiting during migrations initialisation, commit error: commit failed (the root cause)", err.Error())
}

func Test_ErrCouldNotInitialiseMigrationsTable_GenericError(t *testing.T) {
	err := ErrCouldNotCreateMigrationsTable{
		Wrapped: errors.New("the root cause"),
	}

	assert.Equal(t, "could not initialise migrations tables: the root cause", err.Error())
}

func Test_ErrCouldNotExecuteMigration_RollbackError(t *testing.T) {
	err := ErrCouldNotExecuteMigration{
		Name:          "some-migration",
		RollbackError: errors.New("rollback failed"),
		Wrapped:       errors.New("the root cause"),
	}

	assert.Equal(t, "error rolling back after failed migration application (some-migration), rollback error: rollback failed (the root cause)", err.Error())
}

func Test_ErrCouldNotExecuteMigration_CommitError(t *testing.T) {
	err := ErrCouldNotExecuteMigration{
		Name:        "some-migration",
		CommitError: errors.New("commit failed"),
	}

	assert.Equal(t, "error applying migration (some-migration), commit error: commit failed", err.Error())
}

func Test_ErrCouldNotExecuteMigration_GenericError(t *testing.T) {
	err := ErrCouldNotExecuteMigration{
		Name:    "some-migration",
		Wrapped: errors.New("the root cause"),
	}

	assert.Equal(t, "error applying migration (some-migration): the root cause", err.Error())
}

func Test_ErrCouldNotRollbackMigration_RollbackError(t *testing.T) {
	err := ErrCouldNotRollbackMigration{
		Name:          "some-migration",
		RollbackError: errors.New("rollback failed"),
		Wrapped:       errors.New("the root cause"),
	}

	assert.Equal(t, "error rolling back after failed migration rollback (some-migration), rollback error: rollback failed (the root cause)", err.Error())
}

func Test_ErrCouldNotRollbackMigration_CommitError(t *testing.T) {
	err := ErrCouldNotRollbackMigration{
		Name:        "some-migration",
		CommitError: errors.New("commit failed"),
	}

	assert.Equal(t, "error rolling back migration (some-migration), commit error: commit failed", err.Error())
}

func Test_ErrCouldNotRollbackMigration_GenericError(t *testing.T) {
	err := ErrCouldNotRollbackMigration{
		Name:    "some-migration",
		Wrapped: errors.New("the root cause"),
	}

	assert.Equal(t, "error rolling back migration (some-migration): the root cause", err.Error())
}

func Test_ErrInvalidMigrationInstruction(t *testing.T) {
	err := ErrInvalidMigrationInstruction{
		Message: "some problem",
	}

	assert.Equal(t, "invalid migration instruction: some problem", err.Error())
}

func Test_ErrMigrationNotFound(t *testing.T) {
	err := ErrMigrationNotFound{
		Id: "some-migration",
	}

	assert.Equal(t, "no migration exists with name: 'some-migration'", err.Error())
}

func Test_ErrMigrationsNotImplementedForDriver(t *testing.T) {
	err := ErrMigrationsNotImplementedForDriver{
		Driver: "fake-driver",
	}

	assert.Equal(t, "migrations are not implemented for driver: fake-driver", err.Error())
}

func TestErrNoMigrationsFound(t *testing.T) {
	err := ErrNoMigrationsFound{}

	assert.Equal(t, "no migrations were configured", err.Error())
}


func TestErrInvalidOpts(t *testing.T) {
	err := ErrInvalidOpts{
		Message: "some message",
	}

	assert.Equal(t, "some message", err.Error())
}
