package common

import (
	"encoding/json"
	"fmt"
)

func MarshalJSON(body interface{}) ([]byte, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return jsonBody, nil
}
