package db

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/pelicanplatform/pelicanobjectstager/config"
	"github.com/pelicanplatform/pelicanobjectstager/logger"
)

type StagingRecord struct {
	ID              uint      `gorm:"primaryKey;autoIncrement"` // Auto-generated primary key
	CreatedAt       time.Time `gorm:"autoCreateTime"`
	UpdatedAt       time.Time `gorm:"autoUpdateTime"`                                    // Automatically set the current timestamp
	PelicanURL      string    `gorm:"type:varchar(255);uniqueIndex:idx_pelican_staging"` // Part of unique combination
	StagingStorage  string    `gorm:"type:varchar(255);uniqueIndex:idx_pelican_staging"` // Part of unique combination
	ObjectSize      int64     `gorm:"type:bigint"`                                       // Object size in bytes as an int64
	JobID           string    `gorm:"type:varchar(255)"`                                 // Job ID as a string
	PelicanExitCode int       `gorm:"type:int"`                                          // Pelican client exit code as an integer
	PelicanStdout   string    `gorm:"type:text"`                                         // Pelican client stdout as a string
	PelicanStderr   string    `gorm:"type:text"`                                         // Pelican client stderr as a string
}

type StagingRecordLite struct {
	ID             uint      `gorm:"primaryKey" json:"id"`
	PelicanURL     string    `gorm:"column:pelican_url" json:"pelican_url"`
	StagingStorage string    `gorm:"column:staging_storage" json:"staging_storage"`
	ObjectSize     int64     `gorm:"column:object_size" json:"object_size"`
	UpdatedAt      time.Time `gorm:"column:updated_at" json:"updated_at"`
}

var (
	DB  *gorm.DB
	log = logger.SlogWith(slog.String("component", "database"))
)

// Initialize sets up the database connection and runs migrations.
func InitializeDB() {
	// Attempt to connect to the database
	var err error
	databaseLocation := config.AppConfig.Database.Location
	DB, err = gorm.Open(sqlite.Open(databaseLocation), &gorm.Config{})
	if err != nil {
		logger.LogFatal(log, "Failed to connect to the database", err)
		return
	}
	log.Info("Database connection established", slog.String("location", databaseLocation))

	// Run migrations
	err = DB.AutoMigrate(&StagingRecord{})
	if err != nil {
		logger.LogFatal(log, "Failed to migrate database", err)
		return
	}
	log.Info("Database migration completed")
}

func InsertOrUpdateStagingRecord(pelicanURL, stagingStorage, jobID string, objectSize int64, exitCode int, stdout, stderr string) error {
	// Check if the record with the given combination already exists
	var existingRecord StagingRecord
	err := DB.Where("pelican_url = ? AND staging_storage = ?", pelicanURL, stagingStorage).First(&existingRecord).Error

	if err == nil {
		// Record exists, update it
		existingRecord.ObjectSize = objectSize
		existingRecord.JobID = jobID
		existingRecord.PelicanExitCode = exitCode
		existingRecord.PelicanStdout = stdout
		existingRecord.PelicanStderr = stderr

		if updateErr := DB.Save(&existingRecord).Error; updateErr != nil {
			return fmt.Errorf("failed to update record: %v", updateErr)
		}
	} else if err == gorm.ErrRecordNotFound {
		// Record does not exist, create a new one
		newRecord := StagingRecord{
			PelicanURL:      pelicanURL,
			StagingStorage:  stagingStorage,
			ObjectSize:      objectSize,
			JobID:           jobID,
			PelicanExitCode: exitCode,
			PelicanStdout:   stdout,
			PelicanStderr:   stderr,
		}

		if createErr := DB.Create(&newRecord).Error; createErr != nil {
			return fmt.Errorf("failed to create new record: %v", createErr)
		}
	} else {
		// Some other error occurred during the query
		return fmt.Errorf("error checking existing record: %v", err)
	}

	return nil
}

func GetStagingRecordLites() ([]StagingRecordLite, error) {
	var records []StagingRecordLite

	// GORM will automatically map fields in StagingRecordLite to database columns
	err := DB.Model(&StagingRecord{}).Find(&records).Error
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve staging record lites: %v", err)
	}

	return records, nil
}

func GetStagingStorageSizeMap() (map[string]int64, error) {
	type Result struct {
		StagingStorage string
		TotalSize      int64
	}
	var results []Result

	err := DB.Model(&StagingRecord{}).
		Select("staging_storage, SUM(object_size) as total_size").
		Group("staging_storage").
		Scan(&results).Error

	if err != nil {
		return nil, fmt.Errorf("failed to calculate storage sizes: %v", err)
	}

	// Convert the results to a map
	storageSizeMap := make(map[string]int64)
	for _, result := range results {
		storageSizeMap[result.StagingStorage] = result.TotalSize
	}

	return storageSizeMap, nil
}

func GetStagingRecordByID(id uint) (*StagingRecord, error) {
	var record StagingRecord

	err := DB.First(&record, id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to retrieve record with ID %d: %v", id, err)
	}

	return &record, nil
}
