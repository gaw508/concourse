package migrations

import (
	"database/sql"
	"reflect"

	"github.com/concourse/concourse/atc/db/encryption"
)

func NewEncryptedGoMigrationRunner(db *sql.DB, es encryption.Strategy) *migrations {
	return &migrations{db, es}
}

type migrations struct {
	*sql.DB
	encryption.Strategy
}

func (runner *migrations) RunDatabaseMigration(name string) error {

	res := reflect.ValueOf(runner).MethodByName(name).Call(nil)

	ret := res[0].Interface()

	if ret != nil {
		return ret.(error)
	}

	return nil
}
