package backends

import "github.com/JormungandrK/microservice-tools/config"

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

	// similarily for dynamodb
}

func NewBackendSupport(config map[string]*config.DBInfo) BackendManager {
	manager := NewBackendManager(config)
	addSupported(manager)
	return manager
}
