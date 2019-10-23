package migration

import (
	"database/sql"
	"errors"
	"strings"

	"code.cloudfoundry.org/lager"
	"github.com/gobuffalo/packr"
	_ "github.com/lib/pq"

	"github.com/concourse/concourse/atc/db/encryption"
	"github.com/concourse/concourse/atc/db/lock"
	"github.com/concourse/concourse/atc/db/migration/migrations"
	"github.com/concourse/voyager"
)

var concourseMigrationDir = "migrations/"

func NewOpenHelper(
	driver,
	name string,
	lockFactory lock.LockFactory,
	strategy encryption.Strategy,
) *OpenHelper {
	source := &packrSource{packr.NewBox("migrations/")}
	goMigrationsRunner := migrations.NewEncryptedGoMigrationRunner(strategy)
	migrator := voyager.NewMigrator(
		lock.NewDatabaseMigrationLockID()[0],
		source,
		goMigrationsRunner,
		&oldSchemaMigrationAdapter{})

	return &OpenHelper{
		driver,
		name,
		lockFactory,
		strategy,
		migrator,
	}
}

type OpenHelper struct {
	driver         string
	dataSourceName string
	lockFactory    lock.LockFactory
	strategy       encryption.Strategy
	migrator       voyager.Migrator
}

func (self *OpenHelper) CurrentVersion(logger lager.Logger) (int, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return -1, err
	}

	defer db.Close()

	return self.migrator.CurrentVersion(logger, db)
}

func (self *OpenHelper) SupportedVersion(logger lager.Logger) (int, error) {
	return self.migrator.SupportedVersion(logger)
}

func (self *OpenHelper) Open(logger lager.Logger) (*sql.DB, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return nil, err
	}

	if err := self.migrator.Up(logger, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func (self *OpenHelper) OpenAtVersion(logger lager.Logger, version int) (*sql.DB, error) {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return nil, err
	}

	if err := self.migrator.Migrate(logger, db, version); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func (self *OpenHelper) MigrateToVersion(logger lager.Logger, version int) error {
	db, err := sql.Open(self.driver, self.dataSourceName)
	if err != nil {
		return err
	}

	defer db.Close()
	return self.migrator.Migrate(logger, db, version)
}

func checkTableExist(db *sql.DB, tableName string) (bool, error) {
	var exists bool
	err := db.QueryRow("SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_name=$1)", tableName).Scan(&exists)
	if err != nil {
		return false, err
	}

	if exists {
		return true, nil
	}

	// SELECT EXISTS doesn't fail if the user doesn't have permission to look
	// at the information_schema, so fall back to checking the table directly
	rows, err := db.Query("SELECT * from " + tableName)
	if rows != nil {
		defer rows.Close()
	}

	if err == nil {
		return true, nil
	}

	if strings.Contains(err.Error(), "does not exist") {
		return false, nil
	} else {
		return false, err
	}
}

type oldSchemaMigrationAdapter struct{}

func (adapter *oldSchemaMigrationAdapter) MigrateFromOldSchema(db *sql.DB, toVersion int) (int, error) {
	oldSchemaExists, err := checkTableExist(db, "schema_migrations")
	if err != nil {
		return 0, err
	}

	newSchemaExists, err := checkTableExist(db, "migrations_history")
	if err != nil {
		return 0, err
	}

	if !oldSchemaExists || newSchemaExists {
		// TODO: this should return the first version of the new schema rather than 0
		return 0, nil
	}

	var isDirty = false
	var existingVersion int
	err = db.QueryRow("SELECT dirty, version FROM schema_migrations LIMIT 1").Scan(&isDirty, &existingVersion)
	if err != nil {
		return 0, err
	}

	if isDirty {
		return 0, errors.New("cannot begin migration. Database is in a dirty state")
	}

	return existingVersion, nil
}

func (adapter *oldSchemaMigrationAdapter) MigrateToOldSchema(db *sql.DB, toVersion int) error {
	newMigrationsHistoryFirstVersion := 1532706545

	if toVersion >= newMigrationsHistoryFirstVersion {
		return nil
	}

	oldSchemaExists, err := checkTableExist(db, "schema_migrations")
	if err != nil {
		return err
	}

	if !oldSchemaExists {
		_, err := db.Exec("CREATE TABLE schema_migrations (version bigint, dirty boolean)")
		if err != nil {
			return err
		}

		_, err = db.Exec("INSERT INTO schema_migrations (version, dirty) VALUES ($1, false)", toVersion)
		if err != nil {
			return err
		}
	} else {
		_, err := db.Exec("UPDATE schema_migrations SET version=$1, dirty=false", toVersion)
		if err != nil {
			return err
		}
	}

	return nil
}

func (adapter *oldSchemaMigrationAdapter) CurrentVersion(db *sql.DB) (int, error) {
	return 0, errors.New("not implemented")
}
