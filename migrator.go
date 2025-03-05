package migrator

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	logger "github.com/IvanSkripnikov/go-logger"
)

const FirstVersion = "00_create_migration_table.sql"
const migrationsDir = "./migrations"
const MigrationTableName = "migration"

type Migration struct {
	Version   string
	ApplyTime int64
}

// HasExistsRow Проверка наличия текущей миграции в таблице
func (model *Migration) HasExistsRow(dbConn *sql.DB) bool {
	sqlQuery := fmt.Sprintf("select count(*) as countRow from %s where `version` = '%s'",
		MigrationTableName, model.Version)
	rows, err := dbConn.Query(sqlQuery)

	if err != nil {
		if strings.Contains(model.Version, FirstVersion) {
			return false
		}

		logger.Errorf("Failed to get migration data for version %v. Error: %v", model.Version, err)
	} else {
		logger.Debugf("Migration data for version %v was successfully received.", model.Version)
	}

	var countRow int

	if rows.Next() {
		err := rows.Scan(&countRow)

		if err != nil {
			logger.Errorf("Failed to get current migration string for version %v. Error: %v", model.Version, err)
		} else {
			logger.Debugf("The current migration string for version %v was successfully retrieved.", model.Version)
		}
	}

	defer rows.Close()
	return countRow > 0
}

// InsertRow Вставка строки для текущей миграции
func (model *Migration) InsertRow(dbConn *sql.DB) {
	sqlQuery := fmt.Sprintf("insert into %s values ('%s', %d);",
		MigrationTableName, model.Version, time.Now().Unix())

	result, err := dbConn.Exec(sqlQuery)
	if err != nil || result == nil {
		logger.Errorf("Failed to insert migration for version %v. Error: %v", model.Version, err)
	} else {
		logger.Debugf("Migration for version %v inserted successfully.", model.Version)
	}
}

// CreateTables Выполнить запросы на создание таблиц
func CreateTables(DB *sql.DB) {
	files, err := os.ReadDir(migrationsDir)
	if err != nil {
		logger.Errorf("Failed to get list of migration files. Error: %v", err)
	} else {
		logger.Debug("List of migration files retrieved successfully.")
	}

	dataFirstMigration, err := os.ReadFile(migrationsDir + "/" + FirstVersion)
	if err != nil {
		logger.Errorf("Failed to migrations dir. Error: %v", err)
	}

	sqlQuery := strings.ReplaceAll(string(dataFirstMigration), "\r\n", "")
	_, err = DB.Exec(sqlQuery)

	if err != nil {
		logger.Errorf("Failed to execute first migration. Error: %v", err)
	} else {
		logger.Debug("The First migration was successfully applied")
	}

	for _, file := range files {
		if !file.IsDir() {
			migration := Migration{
				Version: file.Name(),
			}

			if !migration.HasExistsRow(DB) {
				data, err := os.ReadFile(migrationsDir + "/" + file.Name())

				if err != nil {
					logger.Errorf("Failed to read migration file: %v. Error: %v", file.Name(), err)
				} else {
					logger.Debugf("The migration file was successfully read: %v.", file.Name())
				}

				sqlQuery := strings.ReplaceAll(string(data), "\r\n", "")
				result, err := DB.Exec(sqlQuery)
				migration.InsertRow(DB)

				if err == nil && result != nil {
					logger.Infof("Migration has been applied successfully: %v.", file.Name())
				}
			}
		}
	}
}
