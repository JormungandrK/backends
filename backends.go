package backends

import (
	"fmt"
	"strings"

	"github.com/JormungandrK/microservice-tools/config"
)

// Repository defaines the interface for accessing the data
type Repository interface {
	GetOne(filter map[string]interface{}, result interface{}) error
	GetAll(filter map[string]interface{}, order string, limit int, offset int) (interface{}, error)
	Save(object interface{}, filter map[string]interface{}) (interface{}, error)
	DeleteOne(filter map[string]interface{}) error
	DeleteAll(filter map[string]interface{}) error
}

var (
	// AVAILABLE_BACKENDS contains the supported backend types
	AVAILABLE_BACKENDS = []string{"mongodb", "dynamodb"}

	// PROPS contains the required properties for the selected backend
	PROPS = map[string]map[string]interface{}{
		"mongodb": {
			"dbName":   "string",
			"host":     "string",
			"database": "string",
			"collections": map[string]interface{}{
				"string": map[string]interface{}{
					"indexes":   "string array",
					"enableTTL": "bool",
					"TTL":       "int",
				},
			},
			"user": "string",
			"pass": "string",
		},
		"dynamodb": {
			"dbName":      "string",
			"credentials": "string",
			"awsRegion":   "string",
			"database":    "string",
			"collections": map[string]interface{}{
				"string": map[string]interface{}{
					"indexes":   "string array",
					"enableTTL": "bool",
					"TTL":       "int",
				},
			},
		},
	}

	// KNOWN_BACKENDS store the builder function for every supported backend
	KNOWN_BACKENDS = map[string]BackendBuilder{}
)

// BackendBuilder defines the builder function
type BackendBuilder func(dbInfo *config.DBInfo) (map[string]Repository, error)

// AddBackendType add backend as to KNOWN_BACKENDS
func AddBackendType(dbName string, builder BackendBuilder) {
	KNOWN_BACKENDS[dbName] = builder
}

// ListAvailableBackends returns all supported backends
func ListAvailableBackends() []string {
	return AVAILABLE_BACKENDS
}

// ListRequiredProps return the required properties for the selected backend type
func ListRequiredProps(dbName string) map[string]interface{} {
	return PROPS[dbName]
}

// New returns interface for the selected backend type base on the DB config
func New(dbConfig config.DBConfig) (map[string]Repository, error) {
	var builder BackendBuilder
	dbName := strings.ToLower(dbConfig.DBName)
	dbInfo := dbConfig.DBInfo

	if _, ok := KNOWN_BACKENDS[dbName]; ok {
		builder = KNOWN_BACKENDS[dbName]
	} else {
		return nil, fmt.Errorf("Not supported backend: %s", dbName)
	}

	return builder(&dbInfo)
}
