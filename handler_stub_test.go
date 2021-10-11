package gomigrator

import (
	"fmt"
	"testing"
)

type handlerDoesTableExistCall struct {
	ExpectedTable string
	DoesExist bool
	Err error
}

type handlerFetchMigrationFromDbCall struct {
	ExpectedId string
	MigrationRecord *MigrationRecord
	Err error
}

type handlerRecordMigrationInDbCall struct {
	ExpectedId string
	ExpectedState MigrationState
	ExpectedAction string
	Err error
}

type handlerStub struct {
	T *testing.T

	CreateMigrationsTableCalls []error
	createMigrationsTableCallCount int

	DoesTableExistCalls  []handlerDoesTableExistCall
	doesTableExistCallCount int

	FetchMigrationFromDbCalls []handlerFetchMigrationFromDbCall
	fetchMigrationFromDbCallCount int

	RecordMigrationInDbCalls []handlerRecordMigrationInDbCall
	recordMigrationInDbCallCount int
}

func (h *handlerStub) CreateMigrationsTable() (err error) {
	if len(h.CreateMigrationsTableCalls) == 0 {
		h.T.Error("CreateMigrationsTable was not expected to be called")
		return
	}

	if h.createMigrationsTableCallCount > (len(h.CreateMigrationsTableCalls) -1) {
		h.T.Error("Too many calls to CreateMigrationsTable")
		return
	}

	call := h.CreateMigrationsTableCalls[h.createMigrationsTableCallCount]

	h.createMigrationsTableCallCount++

	return call
}

func (h *handlerStub) DoesTableExist(table string) (doesExist bool, err error) {
	if len(h.DoesTableExistCalls) == 0 {
		h.T.Error("DoesTableExist was not expected to be called")
		return
	}

	if h.doesTableExistCallCount > (len(h.DoesTableExistCalls) -1) {
		h.T.Error("Too many calls to DoesTableExist")
		return
	}

	call := h.DoesTableExistCalls[h.doesTableExistCallCount]

	h.doesTableExistCallCount++

	if call.ExpectedTable != table {
		h.T.Errorf("Unexpected argument 'table' to DoesTableExist; got '%s', expected '%s'", table, call.ExpectedTable)
		return
	}

	return call.DoesExist, call.Err
}

func (h *handlerStub) FetchMigrationFromDb(id string) (migRecord *MigrationRecord, err error) {
	if len(h.FetchMigrationFromDbCalls) == 0 {
		h.T.Error("FetchMigrationFromDb was not expected to be called")
		return
	}

	if h.fetchMigrationFromDbCallCount > (len(h.FetchMigrationFromDbCalls) -1) {
		h.T.Error("Too many calls to FetchMigrationFromDb")
		return
	}

	call := h.FetchMigrationFromDbCalls[h.fetchMigrationFromDbCallCount]

	h.fetchMigrationFromDbCallCount++

	if call.ExpectedId != id {
		h.T.Errorf("Unexpected argument 'id' to FetchMigrationFromDb; got '%s', expected '%s'", id, call.ExpectedId)
		return
	}

	return call.MigrationRecord, call.Err
}

func (h *handlerStub) RecordMigrationInDb(id string, state MigrationState, action string) (err error) {
	if len(h.RecordMigrationInDbCalls) == 0 {
		h.T.Error("RecordMigrationInDb was not expected to be called")
		return
	}

	if h.recordMigrationInDbCallCount > (len(h.RecordMigrationInDbCalls) -1) {
		h.T.Error("Too many calls to RecordMigrationInDb")
		return
	}

	call := h.RecordMigrationInDbCalls[h.recordMigrationInDbCallCount]

	h.recordMigrationInDbCallCount++

	errors := []string{}

	if call.ExpectedId != id {
		errors = append(
			errors, 
			fmt.Sprintf(
				"Unexpected argument 'id' to RecordMigrationInDb; got '%s', expected '%s'",
				id,
				call.ExpectedId,
			),
		)
	}

	if call.ExpectedState != state {
		errors = append(
			errors, 
			fmt.Sprintf(
				"Unexpected argument 'state' to RecordMigrationInDb; got '%s', expected '%s'",
				string(state),
				string(call.ExpectedState),
			),
		)
	}

	if call.ExpectedAction != action {
		errors = append(
			errors, 
			fmt.Sprintf("Unexpected argument 'action' to RecordMigrationInDb; got '%s', expected '%s'", 
				action, 
				call.ExpectedAction,
			),
		)
	}

	if len(errors) > 0 {
		for _, msg := range(errors) {
			h.T.Error(msg)
		}

		return
	}

	return call.Err
}