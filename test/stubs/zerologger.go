package gomigratorteststubs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

type zerologFilterOpt func(map[string]interface{})

func IgnoreFieldFilter(f string) zerologFilterOpt {
	return func(m map[string]interface{}) {
		delete(m, f)
	}
}

func ExtractLogsToMap(b *bytes.Buffer, filters ...zerologFilterOpt) (m []map[string]interface{}) {
	lines := strings.Split(b.String(), "\n")

	// trailing newline needs removing
	lines = lines[:len(lines)-1]

	for _, line := range lines {
		into := map[string]interface{}{}
		err := json.Unmarshal([]byte(line), &into)

		if err != nil {
			panic(fmt.Errorf("Failed to unmarshal json: %w", err))
		}

		// Remove the time field if it exists...
		// It tends to be problematic for tests, and very rarely a useful test criterion
		delete(into, "time")

		for _, f := range filters {
			f(into)
		}

		m = append(m, into)
	}

	return
}

func BuildZerologLogger(buffer *bytes.Buffer) zerolog.Logger {
	return zerolog.New(buffer).With().Timestamp().Logger()
}
