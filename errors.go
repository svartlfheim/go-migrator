package gomigrator

import "fmt"

type ErrCouldNotCreateMigrationsTable struct {
	Wrapped       error
	RollbackError error
	CommitError   error
}

func (e ErrCouldNotCreateMigrationsTable) Error() string {
	if e.RollbackError != nil {
		return fmt.Sprintf("error rolling back after failed migrations initialisation, rollback error: %s (%s)", e.RollbackError.Error(), e.Wrapped.Error())
	}

	if e.CommitError != nil {
		return fmt.Sprintf("error commiting during migrations initialisation, commit error: %s (%s)", e.CommitError.Error(), e.Wrapped.Error())
	}

	return fmt.Sprintf("could not initialise migrations tables: %s", e.Wrapped.Error())
}

type ErrCouldNotExecuteMigration struct {
	Name          string
	Wrapped       error
	RollbackError error
	CommitError   error
}

func (e ErrCouldNotExecuteMigration) Error() string {
	if e.RollbackError != nil {
		return fmt.Sprintf("error rolling back after failed migration application (%s), rollback error: %s (%s)", e.Name, e.RollbackError.Error(), e.Wrapped.Error())
	}

	if e.CommitError != nil {
		return fmt.Sprintf("error applying migration (%s), commit error: %s", e.Name, e.CommitError.Error())
	}

	return fmt.Sprintf("error applying migration (%s): %s", e.Name, e.Wrapped.Error())
}

type ErrCouldNotRollbackMigration struct {
	Name          string
	Wrapped       error
	RollbackError error
	CommitError   error
}

func (e ErrCouldNotRollbackMigration) Error() string {
	if e.RollbackError != nil {
		return fmt.Sprintf("error rolling back after failed migration rollback (%s), rollback error: %s (%s)", e.Name, e.RollbackError.Error(), e.Wrapped.Error())
	}

	if e.CommitError != nil {
		return fmt.Sprintf("error rolling back migration (%s), commit error: %s", e.Name, e.CommitError.Error())
	}

	return fmt.Sprintf("error rolling back migration (%s): %s", e.Name, e.Wrapped.Error())
}

type ErrInvalidMigrationInstruction struct {
	Message string
}

func (e ErrInvalidMigrationInstruction) Error() string {
	return fmt.Sprintf("invalid migration instruction: %s", e.Message)
}

type ErrMigrationNotFound struct {
	Id string
}

func (e ErrMigrationNotFound) Error() string {
	return fmt.Sprintf("no migration exists with name: '%s'", e.Id)
}

type ErrMigrationsNotImplementedForDriver struct {
	Driver string
}

func (e ErrMigrationsNotImplementedForDriver) Error() string {
	return fmt.Sprintf("migrations are not implemented for driver: %s", e.Driver)
}

type ErrNoMigrationsFound struct{}

func (e ErrNoMigrationsFound) Error() string {
	return "no migrations were configured"
}

type ErrInvalidOpts struct {
	Message string
}

func (e ErrInvalidOpts) Error() string {
	return e.Message
}
