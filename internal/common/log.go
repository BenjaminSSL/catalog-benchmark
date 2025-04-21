package common

import (
	"encoding/json"
	"fmt"
	"io"
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
	Type         string `json:"type"`
	StepID       int    `json:"step_id"`
	Timestamp    string `json:"timestamp"`
	StatusCode   int    `json:"status_code"`
	Body         string `json:"body"`
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

func (l *RoutineBatchLogger) Log(level string, requestType string, stepID int, statusCode int, body string) {
	l.buffer = append(l.buffer, LogEntry{
		Level:        level,
		Type:         requestType,
		ExperimentID: l.ExperimentID,
		ThreadID:     l.TheadID,
		StepID:       stepID,
		StatusCode:   statusCode,
		Body:         body,
		Timestamp:    time.Now().UTC().Format(time.RFC3339Nano),
	})

	if len(l.buffer) >= l.batchSize {
		l.Flush()
	}

}

func (l *RoutineBatchLogger) Flush() {
	if len(l.buffer) == 0 {
		return
	}

	for _, entry := range l.buffer {

		jsonData, err := json.Marshal(entry)

		if err != nil {
			panic(fmt.Sprintf("failed to marshal log entry: %v", err))
		}
		if _, err = l.file.Write(append(jsonData, '\n')); err != nil {
			panic(fmt.Sprintf("failed to write log entry: %v", err))
		}
	}

	// Reset the buffer
	l.buffer = l.buffer[:0]
}

func (l *RoutineBatchLogger) Close() {
	l.Flush()
	l.file.Close()

}

func MergeLogs(logDir string, experimentID string) error {
	// Finds all the file of the jsonl type
	files, err := filepath.Glob(filepath.Join(logDir, "*.jsonl"))
	if err != nil {
		return err
	}

	mergedFilename := filepath.Join("./output/logs", fmt.Sprintf("%s.jsonl", experimentID))
	mergedFile, err := os.OpenFile(mergedFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer mergedFile.Close()

	for _, file := range files {

		logFile, err := os.Open(file)
		if err != nil {
			return err
		}

		_, err = io.Copy(mergedFile, logFile)
		if err != nil {
			return err
		}

		err = logFile.Close()
		if err != nil {
			return err
		}

		err = os.Remove(file)
		if err != nil {
			return err
		}
	}

	return nil
}

func DeleteLogs(logDir string) error {
	files, err := filepath.Glob(filepath.Join(logDir, "*.jsonl"))
	if err != nil {
		return err
	}

	for _, file := range files {
		err = os.Remove(file)
		if err != nil {
			return err
		}
	}

	return nil
}
