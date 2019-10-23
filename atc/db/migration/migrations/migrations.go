package migrations

import (
	"database/sql"
	"reflect"

	"github.com/concourse/concourse/atc/db/encryption"
)

func NewEncryptedGoMigrationRunner(es encryption.Strategy) *migrations {
	return &migrations{es}
}

type migrations struct {
	encryption.Strategy
}

func (runner *migrations) Run(db *sql.DB, name string) error {

	res := reflect.ValueOf(runner).MethodByName(name).Call([]reflect.Value{reflect.ValueOf(db)})

	ret := res[0].Interface()

	if ret != nil {
		return ret.(error)
	}

	return nil
}
