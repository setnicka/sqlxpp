package sqlxpp

import (
	"context"
	"database/sql"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// New creates new DB object around sqlx.DB
func New(db *sqlx.DB) *DB {
	return &DB{
		queryRunner{db: db},
		*db,
	}
}

// QueryRunner is generic interface which is used to share added functions for both DB and Tx.
type queryRunner struct {
	db dbSPI
}

// DB represents database connection. It is wrapper around generic DB connection.
type DB struct {
	queryRunner
	sqlx.DB
}

// Tx represents database transaction. It is wrapper around generic transaction.
type Tx struct {
	queryRunner
	sqlx.Tx
}

// SPI used inside query runner
type dbSPI interface {
	Get(dest interface{}, query string, args ...interface{}) error
	Select(dest interface{}, query string, args ...interface{}) error
	Exec(query string, args ...interface{}) (sql.Result, error)
	NamedExec(query string, arg interface{}) (sql.Result, error)
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
}

// Begin creates new DB transaction.
func (db *DB) Begin() (*Tx, error) {
	tx, err := db.Beginx()
	if err != nil {
		return nil, err
	}
	return &Tx{
		queryRunner{db: tx},
		*tx,
	}, nil
}

// BeginCtx creates new DB transaction which is automatically rollbacked with context.
func (db *DB) BeginCtx(ctx context.Context) (*Tx, error) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Tx{
		queryRunner{db: tx},
		*tx,
	}, nil
}

// GetE is wrapper around sqlx.Get with errors.WithStack error
func (r *queryRunner) GetE(dest interface{}, query string, args ...interface{}) error {
	return errors.WithStack(r.db.Get(dest, query, args...))
}

// SelectE is wrapper around sqlx.Select with errors.WithStack error
func (r *queryRunner) SelectE(dest interface{}, query string, args ...interface{}) error {
	return errors.WithStack(r.db.Select(dest, query, args...))
}

// Insert given struct into given table. In `exclude_fields` there could be
// specified keys that should not be inserted (used for primary keys and etc)
//
// Warning: Only fields with "db" tag are inserted!
// Inspired by https://github.com/jmoiron/sqlx/issues/255#issuecomment-475843681
func (r *queryRunner) Insert(table string, arg interface{}, excludeFields []string) error {
	fields := dbFields(arg, excludeFields)              // e.g. []string{"id", "name", "description"}
	csv := "\"" + strings.Join(fields, "\", \"") + "\"" // e.g. "id", "name", "description"
	csvc := ":" + strings.Join(fields, ", :")           // e.g. :id, :name, :description

	sql := "INSERT INTO " + table + " (" + csv + ") VALUES (" + csvc + ")"
	_, err := r.db.NamedExec(sql, arg)
	return errors.Wrapf(err, "Cannot insert into table '%s'", table)
}

func (r *queryRunner) InsertAndGetID(table string, arg interface{}, excludeFields []string, idField string, idVar *uint) error {
	fields := dbFields(arg, excludeFields)              // e.g. []string{"id", "name", "description"}
	csv := "\"" + strings.Join(fields, "\", \"") + "\"" // e.g. "id", "name", "description"
	csvc := ":" + strings.Join(fields, ", :")           // e.g. :id, :name, :description

	stmt, err := r.db.PrepareNamed("INSERT INTO " + table + " (" + csv + ") VALUES (" + csvc + ") RETURNING " + idField) // check error
	if err != nil {
		return errors.Wrapf(err, "Cannot prepare statement for insert into table '%s'", table)
	}
	return errors.Wrapf(stmt.Get(idVar, arg), "Cannot insert into table '%s'", table)
}

// Update updates given struct in given table. In `exclude_fields` there could be
// specified keys that should not be inserted (used for primary keys and etc)
//
// Warning: Only fields with "db" tag are updated!
// Inspired by https://github.com/jmoiron/sqlx/issues/255#issuecomment-475843681
func (r *queryRunner) Update(table string, arg interface{}, whereSQL string, excludeFields []string) error {
	fields := dbFields(arg, excludeFields) // e.g. []string{"id", "name", "description"}
	return r.UpdateFields(table, arg, whereSQL, fields)
}

// UpdateFields updates given fields in given table according to the given struct.
//
// Warning: Only fields with "db" tag are updated!
func (r *queryRunner) UpdateFields(table string, arg interface{}, whereSQL string, fields []string) error {
	sql := "UPDATE " + table + " SET " + genUpdateString(fields) + " " + whereSQL
	_, err := r.db.NamedExec(sql, arg)
	return errors.Wrapf(err, "Cannot update table '%s'", table)
}
