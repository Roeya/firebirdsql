// +build go1.8

/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2016 Hajime Nakagami

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*******************************************************************************/

package firebirdsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sort"
)

func (stmt *firebirdsqlStmt) ExecContext(ctx context.Context, namedargs []driver.NamedValue) (result driver.Result, err error) {
	if stmt.tx.fc.isBad {
		return nil, driver.ErrBadConn
	}
	sort.SliceStable(namedargs, func(i, j int) bool {
		return namedargs[i].Ordinal < namedargs[j].Ordinal
	})
	args := make([]driver.Value, len(namedargs))
	for i, nv := range namedargs {
		args[i] = nv.Value
	}

	result, err = stmt.exec(ctx, args)
	err = stmt.tx.fc.checkError(err)
	return
}

func (stmt *firebirdsqlStmt) QueryContext(ctx context.Context, namedargs []driver.NamedValue) (rows driver.Rows, err error) {
	if stmt.tx.fc.isBad {
		return nil, driver.ErrBadConn
	}
	args := make([]driver.Value, len(namedargs))
	for i, nv := range namedargs {
		args[i] = nv.Value
	}

	rows, err = stmt.query(ctx, args)
	err = stmt.tx.fc.checkError(err)
	return
}

func (fc *firebirdsqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	if fc.isBad {
		return nil, driver.ErrBadConn
	}
	if opts.ReadOnly {
		return fc.begin(ISOLATION_LEVEL_READ_COMMITED_RO)
	}

	switch (sql.IsolationLevel)(opts.Isolation) {
	case sql.LevelDefault:
		tx, err = fc.begin(ISOLATION_LEVEL_READ_COMMITED)
	case sql.LevelReadCommitted:
		tx, err = fc.begin(ISOLATION_LEVEL_READ_COMMITED)
	case sql.LevelRepeatableRead:
		tx, err = fc.begin(ISOLATION_LEVEL_REPEATABLE_READ)
	case sql.LevelSerializable:
		tx, err = fc.begin(ISOLATION_LEVEL_SERIALIZABLE)
	default:
		err = errors.New("This isolation level is not supported.")
	}
	err = fc.checkError(err)
	return
}

func (fc *firebirdsqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if fc.isBad {
		return nil, driver.ErrBadConn
	}
	stmt, err := fc.prepare(ctx, query)
	err = fc.checkError(err)
	return stmt, err
}

func (fc *firebirdsqlConn) ExecContext(ctx context.Context, query string, namedargs []driver.NamedValue) (result driver.Result, err error) {
	if fc.isBad {
		return nil, driver.ErrBadConn
	}
	args := make([]driver.Value, len(namedargs))
	for i, nv := range namedargs {
		args[i] = nv.Value
	}
	result, err = fc.exec(ctx, query, args)
	err = fc.checkError(err)
	return
}

func (fc *firebirdsqlConn) Ping(ctx context.Context) error {
	if fc.isBad {
		return driver.ErrBadConn
	}
	if fc == nil {
		return errors.New("Connection was closed")
	}
	return nil
}

func (fc *firebirdsqlConn) QueryContext(ctx context.Context, query string, namedargs []driver.NamedValue) (rows driver.Rows, err error) {
	if fc.isBad {
		return nil, driver.ErrBadConn
	}
	args := make([]driver.Value, len(namedargs))
	for i, nv := range namedargs {
		args[i] = nv.Value
	}
	rows, err = fc.query(ctx, query, args)
	err = fc.checkError(err)
	return
}
