/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013-2016 Hajime Nakagami

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
	"database/sql/driver"
	"math/big"
	"net"

	"context"
)

type firebirdsqlConn struct {
	wp *wireProtocol
	//tx           *firebirdsqlTx
	txTmp    *firebirdsqlTx
	addr     string
	dbName   string
	user     string
	password string
	isBad    bool
	//isAutocommit bool
	clientPublic *big.Int
	clientSecret *big.Int
}

func (fc *firebirdsqlConn) begin(isolationLevel int) (driver.Tx, error) {
	if fc.isBad {
		return nil, driver.ErrBadConn
	}
	tx, err := newFirebirdsqlTx(fc, isolationLevel)
	if err != nil {
		debugPrintf(fc.wp, "newFirebirdsqlTx error %s\n", err)
		err = fc.checkError(err)
		return nil, err
	}

	fc.txTmp = tx
	return driver.Tx(tx), err
}

func (fc *firebirdsqlConn) Begin() (tx driver.Tx, err error) {
	if fc.isBad {
		return nil, driver.ErrBadConn
	}
	tx, err = fc.begin(ISOLATION_LEVEL_READ_COMMITED)
	err = fc.checkError(err)
	return
}

func (fc *firebirdsqlConn) Close() (err error) {
	if fc.isBad {
		return driver.ErrBadConn
	}
	err = fc.wp.opDetach()
	if err != nil {
		return err
	}
	_, _, _, err = fc.wp.opResponse()
	fc.wp.conn.Close()
	return
}

func (fc *firebirdsqlConn) prepare(ctx context.Context, query string) (stmt driver.Stmt, err error) {
	/*tx, err := newFirebirdsqlTx(fc, ISOLATION_LEVEL_READ_COMMITED)
	if err != nil {
		return nil, err
	}*/

	///tx := ctx.Value("Transaction").(*firebirdsqlTx)
	if fc.isBad {
		return nil, driver.ErrBadConn
	}

	stmt, err = newFirebirdsqlStmt(fc, fc.txTmp, query)
	err = fc.checkError(err)
	return
}

func (fc *firebirdsqlConn) Prepare(query string) (stmt driver.Stmt, err error) {
	if fc.isBad {
		return nil, driver.ErrBadConn
	}
	stmt, err = fc.prepare(context.Background(), query)
	err = fc.checkError(err)
	return
}

func (fc *firebirdsqlConn) exec(ctx context.Context, query string, args []driver.Value) (result driver.Result, err error) {
	if fc.isBad {
		return nil, driver.ErrBadConn
	}
	stmt, err := fc.prepare(ctx, query)
	if err != nil {
		return
	}
	//TODO: defer stmt.Close() ?
	result, err = stmt.(*firebirdsqlStmt).exec(ctx, args)
	if err != nil {
		return
	}

	/*err = stmt.(*firebirdsqlStmt).tx.Commit()
	if err != nil {
		return
	}
	if fc.isAutocommit && fc.tx.isAutocommit {
		fc.tx.Commit()
	}*/

	//TODO: Close() error processing?
	stmt.Close()
	return
}

func (fc *firebirdsqlConn) Exec(query string, args []driver.Value) (result driver.Result, err error) {
	if fc.isBad {
		return nil, driver.ErrBadConn
	}
	result, err = fc.exec(context.Background(), query, args)
	err = fc.checkError(err)
	return
}

func (fc *firebirdsqlConn) query(ctx context.Context, query string, args []driver.Value) (rows driver.Rows, err error) {
	if fc.isBad {
		return nil, driver.ErrBadConn
	}
	stmt, err := fc.prepare(ctx, query)
	if err != nil {
		return
	}
	rows, err = stmt.(*firebirdsqlStmt).query(ctx, args)
	err = fc.checkError(err)
	return
}

func (fc *firebirdsqlConn) Query(query string, args []driver.Value) (rows driver.Rows, err error) {
	if fc.isBad {
		return nil, driver.ErrBadConn
	}
	rows, err = fc.query(context.Background(), query, args)
	err = fc.checkError(err)
	return
}

func (fc *firebirdsqlConn) checkError(err error) error {
	if err == nil {
		return nil
	}
	debugPrintf(fc.wp, "checkError %s\n", err)
	_, ok := err.(*net.OpError)
	if ok {
		debugPrintf(fc.wp, "checkError: network error, connection gone bad %s\n", err)
		fc.isBad = true
		return driver.ErrBadConn
	}
	return err
}

func newFirebirdsqlConn(dsn string) (fc *firebirdsqlConn, err error) {
	addr, dbName, user, password, role, authPluginName, wireCrypt, err := parseDSN(dsn)
	wp, err := newWireProtocol(addr)
	if err != nil {
		return
	}
	clientPublic, clientSecret := getClientSeed()

	wp.opConnect(dbName, user, password, authPluginName, wireCrypt, clientPublic)
	err = wp.opAccept(user, password, authPluginName, wireCrypt, clientPublic, clientSecret)
	if err != nil {
		return
	}
	wp.opAttach(dbName, user, password, role)
	wp.dbHandle, _, _, err = wp.opResponse()
	if err != nil {
		return
	}

	fc = new(firebirdsqlConn)
	fc.wp = wp
	fc.addr = addr
	fc.dbName = dbName
	fc.user = user
	fc.password = password
	//fc.isAutocommit = true
	//	fc.tx, err = newFirebirdsqlTx(fc, ISOLATION_LEVEL_READ_COMMITED, fc.isAutocommit)
	fc.clientPublic = clientPublic
	fc.clientSecret = clientSecret

	return fc, err
}

func createFirebirdsqlConn(dsn string) (fc *firebirdsqlConn, err error) {
	// Create Database
	addr, dbName, user, password, role, authPluginName, wireCrypt, err := parseDSN(dsn)
	wp, err := newWireProtocol(addr)
	if err != nil {
		return
	}

	clientPublic, clientSecret := getClientSeed()

	wp.opConnect(dbName, user, password, authPluginName, wireCrypt, clientPublic)
	err = wp.opAccept(user, password, authPluginName, wireCrypt, clientPublic, clientSecret)
	if err != nil {
		return
	}
	wp.opCreate(dbName, user, password, role)
	wp.dbHandle, _, _, err = wp.opResponse()
	if err != nil {
		return
	}

	fc = new(firebirdsqlConn)
	fc.wp = wp
	fc.addr = addr
	fc.dbName = dbName
	fc.user = user
	fc.password = password
	//fc.isAutocommit = true
	//fc.tx, err = newFirebirdsqlTx(fc, ISOLATION_LEVEL_READ_COMMITED, fc.isAutocommit)
	fc.clientPublic = clientPublic
	fc.clientSecret = clientSecret

	return fc, err
}
