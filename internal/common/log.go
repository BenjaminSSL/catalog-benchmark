package common

import (
	"encoding/json"
	"github.com/google/uuid"
	"os"
	"sync"
	"time"
)

type Logger struct {
}

type logFile struct {
	mu   sync.Mutex
	file *os.File
}

type LogEntry struct {
	ExperimentID uuid.UUID
	ThreadID     int
	Timestamp    time.Time
	Message      json.RawMessage
}
