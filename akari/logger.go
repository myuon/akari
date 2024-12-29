package akari

import (
	"log/slog"
	"time"
)

type DurationLogger struct {
	*slog.Logger
	PrevTime time.Time
}

func NewDurationLogger(logger *slog.Logger) *DurationLogger {
	return &DurationLogger{
		Logger: logger,
	}
}

func (l *DurationLogger) Debug(message string, args ...any) {
	newArgs := append([]any{"duration(ms)", time.Since(l.PrevTime).Milliseconds()}, args...)

	l.Logger.Debug(message, newArgs...)
	l.PrevTime = time.Now()
}
