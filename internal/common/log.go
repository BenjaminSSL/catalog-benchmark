package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type RoutineBatchLogger struct {
	ExperimentID string
	TheadID      int
	file         *os.File // Single file for each logging struct
	buffer       []LogEntry
	batchSize    int
}

type LogEntry struct {
	Level        string `json:"level"`
	ExperimentID string `json:"experiment_id"`
	ThreadID     int    `json:"thread_id"`
	Timestamp    string `json:"timestamp"`
	Message      string `json:"message"`
	Data         any    `json:"data"`
}

func NewRoutineBatchLogger(logDir string, experimentID string, theadID int, batchSize int) (*RoutineBatchLogger, error) {
	// Creates the log directory, if it already exists it does not create a new directory
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create the log directory: %w", err)
	}

	// Create the file
	filename := filepath.Join(logDir, fmt.Sprintf("%s_%d.jsonl", experimentID, theadID))
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}

	return &RoutineBatchLogger{
		ExperimentID: experimentID,
		TheadID:      theadID,
		file:         file,
		buffer:       make([]LogEntry, 0, batchSize),
		batchSize:    batchSize,
	}, nil

}

func (l *RoutineBatchLogger) Log(level string, message string, data any) error {
	l.buffer = append(l.buffer, LogEntry{
		Level:        level,
		ExperimentID: l.ExperimentID,
		ThreadID:     l.TheadID,
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		Message:      message,
		Data:         data,
	})

	if len(l.buffer) >= l.batchSize {
		return l.Flush()
	}
	return nil
}

func (l *RoutineBatchLogger) Flush() error {
	if len(l.buffer) == 0 {
		return nil
	}

	for _, entry := range l.buffer {

		jsonData, err := json.Marshal(entry)

		if err != nil {
			return fmt.Errorf("failed to marshal log entry: %v", err)
		}
		if _, err := l.file.Write(append(jsonData, '\n')); err != nil {
			return fmt.Errorf("failed to write log entry: %v", err)
		}
	}

	// Reset the buffer
	l.buffer = l.buffer[:0]
	return nil
}

func (l *RoutineBatchLogger) Close() error {
	if err := l.Flush(); err != nil {
		return err
	}
	return nil
}
