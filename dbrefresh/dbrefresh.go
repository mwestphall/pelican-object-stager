package dbrefresh

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/pelicanplatform/pelicanobjectstager/config"
	"github.com/pelicanplatform/pelicanobjectstager/db"
	"github.com/pelicanplatform/pelicanobjectstager/logger"
)

var log = logger.SlogWith(slog.String("component", "db-refresh"))

var insecureHTTPClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Skip TLS verification
		},
	},
	Timeout: 10 * time.Second, // Optional: Set a timeout for requests
}

// refreshStaleRecords identifies stale records and processes them concurrently
func refreshRecords() error {
	jobID := fmt.Sprintf("refresh-records-id-%s", time.Now().Format("20060102-150405"))
	log.Info("Starting refresh stale records job", slog.String("job_id", jobID))

	// Step 1: Fetch stale records
	cutoffTime := time.Now().Add(-config.AppConfig.Database.MaxRecordStaleDuration)
	var staleRecords []db.StagingRecord
	if err := db.DB.Where("updated_at < ?", cutoffTime).Find(&staleRecords).Error; err != nil {
		log.Error("Failed to fetch stale records", slog.String("job_id", jobID), logger.Error(err))
		return err
	}

	if len(staleRecords) == 0 {
		log.Info("No stale records found", slog.String("job_id", jobID))
		return nil
	}

	log.Info("Stale records fetched", slog.Int("count", len(staleRecords)), slog.String("job_id", jobID))

	// Step 2: Set up worker pool
	numWorkers := config.AppConfig.Staging.Workers
	recordChan := make(chan db.StagingRecord, len(staleRecords))
	resultsChan := make(chan error, len(staleRecords))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go refreshRecordWorker(recordChan, resultsChan, &wg, jobID)
	}

	// Send records to the workers
	for _, record := range staleRecords {
		recordChan <- record
	}
	close(recordChan)

	// Wait for all workers to complete
	wg.Wait()
	close(resultsChan)

	// Step 3: Aggregate results
	var hasErrors bool
	for err := range resultsChan {
		if err != nil {
			hasErrors = true
		}
	}

	if hasErrors {
		log.Warn("Refresh completed with errors", slog.String("job_id", jobID))
		return nil // Or return a custom error summarizing failures if needed
	}

	log.Info("Refresh completed successfully", slog.String("job_id", jobID))
	return nil
}

func refreshRecordWorker(recordChan <-chan db.StagingRecord, resultsChan chan<- error, wg *sync.WaitGroup, jobID string) {
	defer wg.Done()

	for record := range recordChan {
		// Extract URL path from PelicanURL and append it to StagingStorage
		parsedURL, err := url.Parse(record.PelicanURL)
		if err != nil {
			log.Error("Failed to parse URL", slog.String("job_id", jobID), slog.Any("recordID", record.ID), logger.Error(err))
			resultsChan <- err
			continue
		}
		objectPath := parsedURL.Path

		stagingURL, err := url.Parse(record.StagingStorage)
		if err != nil {
			log.Error("Failed to parse StagingStorage", slog.String("job_id", jobID), slog.Any("recordID", record.ID), logger.Error(err))
			resultsChan <- err
			continue
		}

		stagingURL.Path = objectPath
		objectURL := stagingURL.String()

		log.Info("Worker processing record",
			slog.String("job_id", jobID),
			slog.Any("recordID", record.ID),
			slog.String("url", objectURL),
		)

		// Make a HEAD request to the URL using the custom insecure HTTP client
		resp, err := insecureHTTPClient.Head(objectURL)
		if err != nil {
			log.Error("Failed to make HEAD request", slog.String("job_id", jobID), slog.Any("recordID", record.ID), logger.Error(err))
			resultsChan <- err
			continue
		}

		// Ensure the response body is closed immediately after processing
		func() {
			defer resp.Body.Close()

			// Handle response status codes
			switch resp.StatusCode {
			case http.StatusOK: // 200 OK
				// Update the `UpdatedAt` timestamp in the database
				if updateErr := db.DB.Model(&record).Update("updated_at", time.Now()).Error; updateErr != nil {
					log.Error("Failed to update record timestamp", slog.String("job_id", jobID), slog.Any("recordID", record.ID), logger.Error(updateErr))
					resultsChan <- updateErr
				} else {
					log.Info("Record timestamp updated", slog.String("job_id", jobID), slog.Any("recordID", record.ID))
					resultsChan <- nil
				}
			case http.StatusNotFound: // 404 Not Found
				// Delete the record from the database
				if deleteErr := db.DB.Delete(&record).Error; deleteErr != nil {
					log.Error("Failed to delete record", slog.String("job_id", jobID), slog.Any("recordID", record.ID), logger.Error(deleteErr))
					resultsChan <- deleteErr
				} else {
					log.Info("Record deleted", slog.String("job_id", jobID), slog.Any("recordID", record.ID))
					resultsChan <- nil
				}
			default:
				log.Warn("Unexpected response status", slog.String("job_id", jobID), slog.Any("recordID", record.ID), slog.Int("status_code", resp.StatusCode))
				resultsChan <- fmt.Errorf("unexpected response status: %d", resp.StatusCode)
			}
		}()
	}
}

// LaunchPeriodicRefreshRecords starts a periodic task to refresh stale records,
// tied to the lifecycle of the Gin server.
func LaunchPeriodicRefreshRecords(ctx context.Context) {
	refreshInterval := config.AppConfig.Database.RefreshInterval

	log.Info("Launching periodic refresh records", slog.Duration("interval", refreshInterval))

	// Start the periodic refresh loop
	go func() {
		ticker := time.NewTicker(refreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				log.Info("Periodic refresh triggered")
				if err := refreshRecords(); err != nil {
					log.Error("Error occurred during refresh", logger.Error(err))
				}
			case <-ctx.Done():
				// Stop the loop when the Gin context is canceled
				log.Info("Stopping periodic refresh records")
				return
			}
		}
	}()
}
