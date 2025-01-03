package logger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"

	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	baseSlogger  *slog.Logger
	initSlogOnce sync.Once
)

type TeeHandler struct {
	handlers []slog.Handler
}

const FatalLevel = slog.Level(12)

var FatalPrintOpts = slog.HandlerOptions{
	// Need to do a custom string representation of fatal
	ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == slog.LevelKey {
			if a.Value.Any().(slog.Level) == FatalLevel {
				a.Value = slog.StringValue("FATAL")
			}
		}
		return a
	},
}

// Preconfigure the desired "child" loggers to CHTC standards
// TODO develop CHTC standards
func NewConsoleFileTeeHandler(console io.Writer, filepath string, consoleOpts *slog.HandlerOptions, fileOpts *slog.HandlerOptions) *TeeHandler {
	consoleLogger := slog.New(slog.NewTextHandler(console, consoleOpts))
	logrotate := &lumberjack.Logger{
		Filename:   filepath,
		MaxSize:    500,
		MaxBackups: 3,
		MaxAge:     28,
		Compress:   true,
	}
	fileLogger := slog.New(slog.NewJSONHandler(logrotate, fileOpts))

	return &TeeHandler{
		handlers: []slog.Handler{
			consoleLogger.Handler(),
			fileLogger.Handler(),
		},
	}
}

// Return whether any child handler is able to handle the message
func (h *TeeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

// Pass the record down to both child loggers for handling
func (h *TeeHandler) Handle(ctx context.Context, r slog.Record) error {
	errs := make([]error, 0)
	for _, handler := range h.handlers {
		errs = append(errs, handler.Handle(ctx, r))
	}
	return errors.Join(errs...)
}

// Return a new struct that contains copies of both handlers
func (h *TeeHandler) WithGroup(name string) slog.Handler {
	newHandlers := make([]slog.Handler, 0)
	for _, handler := range h.handlers {
		newHandlers = append(newHandlers, handler.WithGroup(name))
	}
	return &TeeHandler{handlers: newHandlers}
}

// Return a new struct that contains copies of both handlers
func (h *TeeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newHandlers := make([]slog.Handler, 0)
	for _, handler := range h.handlers {
		newHandlers = append(newHandlers, handler.WithAttrs(attrs))
	}
	return &TeeHandler{handlers: newHandlers}
}

func SlogBase() *slog.Logger {
	initSlogOnce.Do(func() {
		fmt.Println("Base Slogger initializing...")
		baseSlogger = slog.New(NewConsoleFileTeeHandler(os.Stdout, "/tmp/slog-logs.log", &FatalPrintOpts, &FatalPrintOpts))
	})

	if baseSlogger == nil {
		// Return the default logger if unable to initialize
		return slog.Default()
	}

	return baseSlogger
}

func SlogWith(args ...any) *slog.Logger {
	return SlogBase().With(args...)
}

// One big issue, no default fatal level
func LogFatal(log *slog.Logger, msg string, err error, args ...any) {
	args = append([]any{Error(err)}, args...)
	log.Log(context.Background(), FatalLevel, msg, args...)
	os.Exit(1)
}

func Error(err error) slog.Attr {
	return slog.Any("error", err)
}
