package gomigrator

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

type postgresHandler struct {
	conn *sqlx.DB
	opts Opts
	l zerolog.Logger
}

func (h *postgresHandler) DoesTableExist(table string) (bool, error) {
	if h.opts.Schema == "" {
		return false, ErrInvalidOpts{
			Message: "schema opt must be set for postgres handler",
		}
	}

	checkExistsQuery := `
SELECT 
	count(1)
FROM 
	information_schema.tables 
WHERE 
	table_schema = $1 AND 
	table_name = $2;
`

	res, err := h.conn.Queryx(checkExistsQuery, h.opts.Schema, table)

	if err != nil {
		return false, err
	}

	res.Next()
	var count int

	if err := res.Scan(&count); err != nil {
		return false, err
	}

	return count > 0, nil
}

func (h *postgresHandler) CreateMigrationsTable() error {
	tableExists, err := h.DoesTableExist("migrations")

	if err != nil {
		return err
	}

	if tableExists {
		h.l.Debug().Msg("skipping create migrations table as it already exists")
		return nil
	}

	h.l.Info().Msg("Creating migrations table as it was not present.")

	createMigrationsTable := `CREATE TABLE migrations(
		id TEXT,
		status TEXT,
		events JSON DEFAULT '[]'::json,
		PRIMARY KEY(id)
	);`

	tx, err := h.conn.Beginx()
	
	if err != nil {
		return ErrCouldNotCreateMigrationsTable{
			Wrapped: err,
		}
	}

	if _, err = tx.Exec(createMigrationsTable); err != nil {
		rollbackErr := tx.Rollback()
		return ErrCouldNotCreateMigrationsTable{
			Wrapped:       err,
			RollbackError: rollbackErr,
		}
	}

	err = tx.Commit()

	if err != nil {
		return ErrCouldNotCreateMigrationsTable{
			Wrapped:     err,
			CommitError: err,
		}
	}
	return nil
}

func (h *postgresHandler) FetchMigrationFromDb(id string) (*MigrationRecord, error) {
	migRecord := &MigrationRecordDb{}
	err := h.conn.Get(migRecord, `SELECT * FROM migrations WHERE id=$1`, id)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	var events []MigrationRecordEvent

	json.Unmarshal([]byte(migRecord.Events), &events)

	return &MigrationRecord{
		Id:     migRecord.Id,
		Status: migRecord.Status,
		Events: events,
	}, nil
}

func (h *postgresHandler) insertMigration(id string, now time.Time, state MigrationState, action string) error {
	insert := "INSERT INTO migrations(id, status, events) VALUES($1, $2, $3);"

	events := []MigrationRecordEvent{
		{
			Action:      action,
			Actor:       h.opts.Applyer,
			PerformedAt: now.Format(time.RFC3339),
			Result:      string(state),
		},
	}
	json, err := json.Marshal(&events)

	if err != nil {
		return err
	}

	_, err = h.conn.Exec(insert,
		id,
		string(state),
		string(json),
	)

	return err
}

func (h *postgresHandler) updateMigration(current *MigrationRecord, id string, now time.Time, state MigrationState, action string) error {
	update := "UPDATE migrations SET status=$1, events=$2 WHERE id=$3;"

	events := append(current.Events, MigrationRecordEvent{
		Action:      action,
		Actor:       h.opts.Applyer,
		PerformedAt: now.Format(time.RFC3339),
		Result:      string(state),
	})

	json, err := json.Marshal(&events)

	if err != nil {
		return err
	}

	_, err = h.conn.Exec(update,
		string(state),
		string(json),
		string(id),
	)

	return err
}

func (h *postgresHandler) RecordMigrationInDb(id string, state MigrationState, action string) error {
	h.l.Debug().Str("id", id).Msg("recording state")
	now := time.Now().UTC()
	current, err := h.FetchMigrationFromDb(id)

	if err != nil {
		h.l.Error().Str("id", id).Err(err).Msg("error recording migration state")
		return err
	}

	if current == nil {
		h.l.Debug().Str("id", id).Msg("no existing entry for migration")
		return h.insertMigration(id, now, state, action)
	}

	h.l.Debug().Str("id", id).Msg("found existing entry for migration")
	return h.updateMigration(current, id, now, state, action)
}