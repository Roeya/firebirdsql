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
	"fmt"
)

type firebirdsqlTx struct {
	fc             *firebirdsqlConn
	isolationLevel int
	isAutocommit   bool
	transHandle    int32
}

func (tx *firebirdsqlTx) begin() error {
	if tx.fc.isBad {
		return driver.ErrBadConn
	}
	var tpb []byte
	switch tx.isolationLevel {
	case ISOLATION_LEVEL_READ_COMMITED_LEGACY:
		tpb = []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_write),
			byte(isc_tpb_wait),
			byte(isc_tpb_read_committed),
			byte(isc_tpb_no_rec_version),
		}
	case ISOLATION_LEVEL_READ_COMMITED:
		tpb = []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_write),
			byte(isc_tpb_nowait),
			byte(isc_tpb_read_committed),
			byte(isc_tpb_rec_version),
		}
	case ISOLATION_LEVEL_REPEATABLE_READ:
		tpb = []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_write),
			byte(isc_tpb_nowait),
			byte(isc_tpb_concurrency),
		}
	case ISOLATION_LEVEL_SERIALIZABLE:
		tpb = []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_write),
			byte(isc_tpb_nowait),
			byte(isc_tpb_consistency),
		}
	case ISOLATION_LEVEL_READ_COMMITED_RO:
		tpb = []byte{
			byte(isc_tpb_version3),
			byte(isc_tpb_read),
			byte(isc_tpb_nowait),
			byte(isc_tpb_read_committed),
			byte(isc_tpb_rec_version),
		}
	default:
		return fmt.Errorf("Invalid isolation level %d", tx.isolationLevel)
	}
	err := tx.fc.wp.opTransaction(tpb)
	if err != nil {
		errorPrintf(tx.fc.wp, "tx.Begin: opTransaction error %s\n", err)
		return err
	}
	tx.transHandle, _, _, err = tx.fc.wp.opResponse()
	if err != nil {
		errorPrintf(tx.fc.wp, "tx.Begin: opResponse error %s\n", err)
		return err
	}
	return nil
}

func (tx *firebirdsqlTx) Commit() (err error) {
	tx.fc.txTmp = nil
	if tx.fc.isBad {
		return driver.ErrBadConn
	}
	err = tx.fc.wp.opCommit(tx.transHandle)
	if err != nil {
		errorPrintf(tx.fc.wp, "Commit: opCommit error %s", err)
		return err
	}
	_, _, _, err = tx.fc.wp.opResponse()
	if err != nil {
		errorPrintf(tx.fc.wp, "Commit: opResponse error %s", err)
		return err
	}
	//tx.isAutocommit = tx.fc.isAutocommit

	return
}

func (tx *firebirdsqlTx) Rollback() (err error) {
	tx.fc.txTmp = nil
	if tx.fc.isBad {
		return driver.ErrBadConn
	}
	tx.fc.wp.opRollback(tx.transHandle)
	if err != nil {
		errorPrintf(tx.fc.wp, "Rollback: opRollback error %s", err)
		return err
	}
	_, _, _, err = tx.fc.wp.opResponse()
	if err != nil {
		errorPrintf(tx.fc.wp, "Rollback: opResponse error %s", err)
		return err
	}
	//tx.isAutocommit = tx.fc.isAutocommit
	return
}

func newFirebirdsqlTx(fc *firebirdsqlConn, isolationLevel int /*, isAutocommit bool*/) (tx *firebirdsqlTx, err error) {
	tx = new(firebirdsqlTx)
	tx.fc = fc
	tx.isolationLevel = isolationLevel
	//tx.isAutocommit = isAutocommit
	err = tx.begin()
	return
}
