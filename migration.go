package gomigrator

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type MigrationState string

const MigrationPending MigrationState = "pending"
const MigrationApplied MigrationState = "applied"
const MigrationFailed MigrationState = "failed"
const MigrationRolledBack MigrationState = "rolled_back"
const MigrationRollbackFailed MigrationState = "rollback_failed"

const MigrateToLatest = "MIGRATE_TO_LATEST"
const MigrateToNothing = "MIGRATE_TO_NOTHING"

type MigrationDirection string

const MigrationUp MigrationDirection = "up"
const MigrationDown MigrationDirection = "down"

type MigrationList struct {
	migrations []Migration
}

func (ml MigrationList) All() []Migration {
	return ml.migrations
}

func (ml MigrationList) GetLatestMigration() (*Migration, bool) {
	if len(ml.migrations) == 0 {
		return nil, false
	}

	return &ml.migrations[len(ml.migrations)-1], true
}

func (ml MigrationList) GetFirstMigration() (*Migration, bool) {
	if len(ml.migrations) == 0 {
		return nil, false
	}

	return &ml.migrations[0], true
}

func (ml MigrationList) findById(id string) (*Migration, int) {
	for i, m := range ml.migrations {
		if m.Id == id {
			return &m, i
		}
	}

	return nil, -1
}

func (ml MigrationList) MigrationsUpToId(id string) ([]Migration, error) {
	_, i := ml.findById(id)

	if i == -1 {
		return []Migration{}, ErrMigrationNotFound{
			Id: id,
		}
	}

	return ml.migrations[:(i + 1)], nil
}

func (ml MigrationList) MigrationsFromId(id string) ([]Migration, error) {
	_, i := ml.findById(id)

	if i == -1 {
		return []Migration{}, ErrMigrationNotFound{
			Id: id,
		}
	}

	return ml.migrations[i:], nil
}

type MigrationOpts struct {
	Schema  string
	Applyer string
}

type MigrationInstruction struct {
	Direction MigrationDirection
	Target    string
}

type MigrationExecutor func(tx *sqlx.Tx) (sql.Result, error)

type Migration struct {
	Id       string
	Name     string
	Execute  MigrationExecutor
	Rollback MigrationExecutor
}

type MigrationRecordEvent struct {
	Action      string
	Actor       string
	PerformedAt string
	Result      string
}

type MigrationRecordDb struct {
	Id     string
	Status string
	Events string
}

type MigrationRecord struct {
	Id     string
	Status string
	Events []MigrationRecordEvent
}
