package server

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/pelicanplatform/pelicanobjectstager/config"
	"github.com/pelicanplatform/pelicanobjectstager/dbrefresh"
	"github.com/pelicanplatform/pelicanobjectstager/logger"
	"github.com/pelicanplatform/pelicanobjectstager/server/object"
)

var log = logger.SlogWith(slog.String("component", "server"))

func StartServer() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := gin.New()

	for _, mw := range middlewares {
		r.Use(mw)
	}
	r.POST("/pelican", handleStartBinary)
	r.GET("/health", handleHealthCheck)
	r.GET("/records/all", handleRecordsAll)
	r.GET("/records/stagingstorages/all", handleStagingStoragesAll)
	r.GET("/records/:id", handleGetRecordByID)

	object.RegisterObjectRoutes(r)

	address := config.AppConfig.Server.Port

	log.Debug("Starting LaunchPeriodicRefreshRecords...")
	go dbrefresh.LaunchPeriodicRefreshRecords(ctx)

	log.Info("Starting server",
		slog.Int("port", address),
	)

	port := strconv.Itoa(address)
	if err := r.Run(":" + port); err != nil {
		logger.LogFatal(log, "Failed to start server", err)
	}
}
