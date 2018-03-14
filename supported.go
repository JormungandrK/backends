package backends

import "github.com/JormungandrK/microservice-tools/config"

// addSupported adds new backends
func addSupported(manager BackendManager) {
	manager.SupportBackend("mongodb", MongoDBBackendBuilder, map[string]interface{}{
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
	})

	manager.SupportBackend("dynamodb", DynamoDBBackendBuilder, map[string]interface{}{
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
	})
}

// NewBackendSupport registers new backends
func NewBackendSupport(dbConfig map[string]*config.DBInfo) BackendManager {
	manager := NewBackendManager(dbConfig)
	addSupported(manager)
	return manager
}
