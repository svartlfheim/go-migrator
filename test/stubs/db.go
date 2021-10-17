package gomigratorteststubs

import (
	"database/sql"
	"testing"

	"github.com/jmoiron/sqlx"
)

type DBBeginxCall struct {
	Tx  *sqlx.Tx
	Err error
}

type DBQueryxCall struct {
	Rows *sqlx.Rows
	Err  error
}

type DBExecCall struct {
	Result sql.Result
	Err    error
}

type DB struct {
	T      *testing.T
	Driver string

	BeginxCalls     []DBBeginxCall
	beginxCallCount int

	QueryxCalls     []DBQueryxCall
	queryxCallCount int

	ExecCalls     []DBExecCall
	execCallCount int

	GetCalls     []error
	getCallCount int
}

func (db *DB) DriverName() string {
	return db.Driver
}

func (db *DB) Beginx() (tx *sqlx.Tx, err error) {
	if len(db.BeginxCalls) == 0 {
		db.T.Error("Beginx was not expected to be called")
		return
	}

	if db.beginxCallCount > (len(db.BeginxCalls) - 1) {
		db.T.Error("Too many calls to Beginx")
		return
	}

	call := db.BeginxCalls[db.beginxCallCount]

	db.beginxCallCount++

	return call.Tx, call.Err
}

func (db *DB) Queryx(query string, args ...interface{}) (rows *sqlx.Rows, err error) {
	if len(db.QueryxCalls) == 0 {
		db.T.Error("Queryx was not expected to be called")
		return
	}

	if db.queryxCallCount > (len(db.QueryxCalls) - 1) {
		db.T.Error("Too many calls to Queryx")
		return
	}

	call := db.QueryxCalls[db.queryxCallCount]

	db.queryxCallCount++

	return call.Rows, call.Err
}

func (db *DB) Get(dest interface{}, query string, args ...interface{}) (err error) {
	if len(db.GetCalls) == 0 {
		db.T.Error("Get was not expected to be called")
		return
	}

	if db.getCallCount > (len(db.GetCalls) - 1) {
		db.T.Error("Too many calls to Get")
		return
	}

	call := db.GetCalls[db.getCallCount]

	db.getCallCount++

	return call
}

func (db *DB) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	if len(db.ExecCalls) == 0 {
		db.T.Error("Exec was not expected to be called")
		return
	}

	if db.execCallCount > (len(db.ExecCalls) - 1) {
		db.T.Error("Too many calls to Exec")
		return
	}

	call := db.ExecCalls[db.execCallCount]

	db.execCallCount++

	return call.Result, call.Err
}
