package gomigrator

import (
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

type migrationHandler interface {
	CreateMigrationsTable() error
	// Later...
	//CreateMigrationsLockTable(*sqlx.Tx) error
	DoesTableExist(table string) (bool, error)

	FetchMigrationFromDb(id string) (*MigrationRecord, error)
	RecordMigrationInDb(id string, state MigrationState, action string) error
}

type Opts struct {
	Schema  string
	Applyer string
}

type Migrator struct {
	conn    *sqlx.DB
	handler migrationHandler
	migs    MigrationList
	opts    Opts
	logger  zerolog.Logger
}

func (m *Migrator) getMigrationState(id string) (MigrationState, error) {
	migRecord, err := m.handler.FetchMigrationFromDb(id)

	if err != nil {
		return MigrationState(""), err
	}

	if migRecord == nil {
		return MigrationPending, nil
	}

	return MigrationState(migRecord.Status), err
}

func (m *Migrator) Up(t string) error {
	if err := m.handler.CreateMigrationsTable(); err != nil {
		return err
	}

	if t == "" {
		return ErrInvalidMigrationInstruction{
			Message: "migration target must be set",
		}
	}

	if t == MigrateToLatest {
		latestMig, found := m.migs.GetLatestMigration()

		if !found {
			return ErrNoMigrationsFound{}
		}

		t = latestMig.Id
	}

	requiredMigrations, err := m.migs.MigrationsUpToId(t)

	if err != nil {
		return ErrInvalidMigrationInstruction{
			Message: err.Error(),
		}
	}

	for _, toRun := range requiredMigrations {
		err := m.apply(toRun)

		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) apply(mig Migration) error {
	state, err := m.getMigrationState(mig.Id)

	if err != nil {
		return err
	}

	if state == MigrationApplied {
		// Skip it
		m.logger.Info().Str("id", mig.Id).Msg("already applied")
		return nil
	}

	m.logger.Info().Str("id", mig.Id).Msg("applying")

	tx, err := m.conn.Beginx()

	if err != nil {
		return err
	}

	_, err = mig.Execute(tx)

	if err != nil {
		rollbackErr := tx.Rollback()

		return ErrCouldNotExecuteMigration{
			Name:          mig.Id,
			Wrapped:       err,
			RollbackError: rollbackErr,
		}
	}

	err = tx.Commit()

	if err != nil {
		if stateErr := m.handler.RecordMigrationInDb(mig.Id, MigrationFailed, "apply"); stateErr != nil {
			m.logger.Error().Err(stateErr).Str("id", mig.Id).Msg("failed to record migration state for failed apply")
		}

		return ErrCouldNotExecuteMigration{
			Name:        mig.Id,
			CommitError: err,
		}
	}

	m.logger.Info().Str("id", mig.Id).Msg("successfully applied")

	if stateErr := m.handler.RecordMigrationInDb(mig.Id, MigrationApplied, "apply"); stateErr != nil {
		m.logger.Error().Err(stateErr).Str("id", mig.Id).Msg("failed to record migration state for successful migration")
	}

	return nil
}

func (m *Migrator) Down(t string) error {
	if err := m.handler.CreateMigrationsTable(); err != nil {
		return err
	}

	if t == "" {
		return ErrInvalidMigrationInstruction{
			Message: "migration target must be set",
		}
	}

	if t == MigrateToNothing {
		firstMig, found := m.migs.GetFirstMigration()

		if !found {
			return ErrNoMigrationsFound{}
		}

		t = firstMig.Id
	}

	requiredMigrations, err := m.migs.MigrationsFromId(t)

	if err != nil {
		return ErrInvalidMigrationInstruction{
			Message: err.Error(),
		}
	}

	for i := len(requiredMigrations) - 1; i >= 0; i-- {
		err := m.rollback(requiredMigrations[i])

		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) rollback(mig Migration) error {
	state, err := m.getMigrationState(mig.Id)

	if err != nil {
		return err
	}

	if state == MigrationRolledBack || state == MigrationPending {
		// Skip it
		m.logger.Info().Str("id", mig.Id).Msg("skipping rollback; not applied")
		return nil
	}

	m.logger.Info().Str("id", mig.Id).Msg("rolling back")

	tx, err := m.conn.Beginx()

	if err != nil {
		return err
	}

	_, err = mig.Rollback(tx)

	if err != nil {
		rollbackErr := tx.Rollback()

		return ErrCouldNotRollbackMigration{
			Name:          mig.Id,
			Wrapped:       err,
			RollbackError: rollbackErr,
		}
	}

	err = tx.Commit()

	if err != nil {
		if stateErr := m.handler.RecordMigrationInDb(mig.Id, MigrationRollbackFailed, "rollback"); stateErr != nil {
			m.logger.Error().Err(stateErr).Str("id", mig.Id).Msg("failed to record migration state for failed roll back")
		}

		return ErrCouldNotRollbackMigration{
			Name:        mig.Id,
			CommitError: err,
		}
	}

	m.logger.Info().Str("id", mig.Id).Msg("successfully rolled back")

	if stateErr := m.handler.RecordMigrationInDb(mig.Id, MigrationRolledBack, "rollback"); stateErr != nil {
		m.logger.Error().Err(stateErr).Str("id", mig.Id).Msg("failed to record migration state for successful rollback")
	}

	return nil
}

func (m *Migrator) ListMigrations() ([]*MigrationRecord, error) {
	if err := m.handler.CreateMigrationsTable(); err != nil {
		return []*MigrationRecord{}, err
	}

	migs := []*MigrationRecord{}

	for _, mig := range m.migs.All() {
		migRecord, err := m.handler.FetchMigrationFromDb(mig.Id)

		if err != nil {
			return migs, err
		}

		if migRecord == nil {
			migRecord = &MigrationRecord{
				Id:     mig.Id,
				Status: string(MigrationPending),
			}
		}

		migs = append(migs, migRecord)
	}

	return migs, nil
}

func NewMigrator(dbConn *sqlx.DB, migs MigrationList, opts Opts, logger zerolog.Logger) (*Migrator, error) {
	driver := dbConn.DriverName()

	var handler migrationHandler
	switch driver {
	case "postgres":
		handler = &postgresHandler{
			conn: dbConn,
			opts: opts,
			l:    logger,
		}
	default:
		return nil, ErrMigrationsNotImplementedForDriver{
			Driver: driver,
		}
	}

	return &Migrator{
		conn:    dbConn,
		handler: handler,
		opts:    opts,
		migs:    migs,
	}, nil
}
