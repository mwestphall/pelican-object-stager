package object

import (
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/pelicanplatform/pelicanobjectstager/config"
	"github.com/pelicanplatform/pelicanobjectstager/db"
	"github.com/pelicanplatform/pelicanobjectstager/logger"
	"github.com/pelicanplatform/pelicanobjectstager/pelican"
)

var log = logger.SlogWith(slog.String("component", "object"))

// StageRequest represents the input structure for the /object/stage endpoint
type StageRequest struct {
	Entries     []RequestEntry `json:"entries" binding:"required"`      // List of entries
	TargetCache string         `json:"target_cache" binding:"required"` // Target cache
}

// RequestEntry represents a single request entry
type RequestEntry struct {
	RequestURL string `json:"request_url" binding:"required"` // Object URL
	Parameters string `json:"parameters,omitempty"`           // Optional flags/options
}

func HandleStage(c *gin.Context) {
	var input StageRequest

	// Extract job_id from the context
	jobID := c.GetString("job_id")
	if jobID == "" {
		log.Error("Missing job_id in context")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing job_id in context"})
		return
	}

	if err := c.ShouldBindJSON(&input); err != nil {
		log.Error("Failed to bind JSON input", slog.String("job_id", jobID), logger.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	numWorkers := config.AppConfig.Staging.Workers
	entryChan := make(chan RequestEntry, len(input.Entries))
	resultsChan := make(chan map[string]interface{}, len(input.Entries))
	var wg sync.WaitGroup

	// Start staging workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go stagingWorker(entryChan, input.TargetCache, resultsChan, &wg, jobID)
	}

	// Send entries to the workers
	for _, entry := range input.Entries {
		entryChan <- entry
	}
	close(entryChan)

	// Wait for all workers to complete
	wg.Wait()
	close(resultsChan)

	results := make(map[string]interface{})
	hasErrors := false

	for result := range resultsChan {
		url := result["request_url"].(string)
		status := result["result"]

		results[url] = status
		if status != "success" {
			hasErrors = true
		}
	}

	// Determine response status
	if hasErrors {
		log.Warn("Staging completed with errors", slog.String("job_id", jobID))
		c.JSON(http.StatusInternalServerError, gin.H{
			"job_id":  jobID,
			"message": "Staging completed with errors",
			"results": results,
		})
	} else {
		log.Info("Staging completed successfully", slog.String("job_id", jobID))
		c.JSON(http.StatusOK, gin.H{
			"job_id":  jobID,
			"message": "Staging completed successfully",
			"results": results,
		})
	}
}

// stagingWorker processes a single entry and sends results to channels
func stagingWorker(entries <-chan RequestEntry, targetCache string, results chan<- map[string]interface{}, wg *sync.WaitGroup, jobID string) {
	defer wg.Done()

	tempObjectName := uuid.New().String()
	tempDestination := config.AppConfig.Staging.TempDestination
	objectDestination := filepath.Join(tempDestination, tempObjectName)

	for entry := range entries {
		args := []string{"object", "get", entry.RequestURL, objectDestination}

		if entry.Parameters != "" {
			parameterArgs := strings.Fields(entry.Parameters) // Split by space
			args = append(args, parameterArgs...)
		}

		args = append(args, "--cache", targetCache)

		log.Debug("Processing entry",
			slog.String("job_id", jobID),
			slog.String("request_url", entry.RequestURL),
			slog.String("parameters", entry.Parameters),
			slog.String("parsed_args", strings.Join(args, ", ")),
			slog.String("temp_destination", tempDestination),
			slog.String("local_object_destination", objectDestination),
		)

		stdout, stderr, exitCode, err := pelican.InvokePelicanBinary(args)

		if err != nil {
			errorMessage := stderr
			// If stderr is empty, use the default error message
			if stderr == "" {
				errorMessage = err.Error()
			}

			log.Error("Failed to process entry",
				slog.String("job_id", jobID),
				slog.String("request_url", entry.RequestURL),
				slog.String("stdout", stdout),
				slog.String("stderr", stderr),
				slog.String("local_object_destination", objectDestination),
				slog.String("error", errorMessage),
				slog.Int("pelican_client_exit_code", exitCode),
			)
			results <- map[string]interface{}{
				"request_url": entry.RequestURL,
				"result":      errorMessage,
			}
		} else {
			objectInfo, err := os.Stat(objectDestination)
			if err != nil {
				log.Error("Failed to process entry",
					slog.String("job_id", jobID),
					slog.String("request_url", entry.RequestURL),
					slog.String("stdout", stdout),
					slog.String("stderr", stderr),
					slog.String("local_object_destination", objectDestination),
					slog.String("error", err.Error()),
					slog.Int("pelican_client_exit_code", exitCode),
				)
				results <- map[string]interface{}{
					"request_url": entry.RequestURL,
					"result":      err.Error(),
				}
			}
			objectSize := objectInfo.Size()

			err = db.InsertOrUpdateStagingRecord(entry.RequestURL, targetCache, jobID, objectSize, exitCode, stdout, stderr)
			if err == nil {
				log.Info("Entry processed successfully",
					slog.String("job_id", jobID),
					slog.String("request_url", entry.RequestURL),
					slog.Int64("object_size_in_bytes", objectSize),
					slog.String("stdout", stdout),
					slog.String("stderr", stderr),
					slog.Int("pelican_client_exit_code", exitCode),
				)
				results <- map[string]interface{}{
					"request_url": entry.RequestURL,
					"result":      "success",
				}
			} else {
				log.Error("Failed to insert staging record",
					slog.String("job_id", jobID),
					slog.String("request_url", entry.RequestURL),
					slog.Int64("object_size_in_bytes", objectSize),
					slog.String("stdout", stdout),
					slog.String("stderr", stderr),
					slog.Int("pelican_client_exit_code", exitCode),
					logger.Error(err),
				)
				results <- map[string]interface{}{
					"request_url": entry.RequestURL,
					"result":      err.Error(),
				}
			}

		}
	}
}
