package backends

import (
	"github.com/Microkubes/microservice-tools/config"
)

// addSupported adds new backends
func addSupported(manager BackendManager) {
	manager.SupportBackend("mongodb", MongoDBBackendBuilder, map[string]interface{}{
		"type": map[string]interface{}{
			"required": true,
			"type":     "string",
		},
		"host": map[string]interface{}{
			"required": true,
			"type":     "string",
		},
		"database": map[string]interface{}{
			"required": true,
			"type":     "string",
		},
		"credentials": map[string]interface{}{
			"type":     "object",
			"required": true,
			"properties": map[string]interface{}{
				"username": map[string]interface{}{
					"type":     "string",
					"required": true,
				},
				"password": map[string]interface{}{
					"type":     "string",
					"required": true,
				},
			},
		},
		"collections": map[string]interface{}{
			"type": "map",
			"key": map[string]interface{}{
				"type": "string",
			},
			"value": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{
						"type":     "string",
						"required": true,
					},
					"indexes": map[string]interface{}{
						"elementType": "object",
						"elementProperties": map[string]interface{}{
							"columns": map[string]interface{}{
								"array":       true,
								"elementType": "string",
								"required":    true,
							},
							"unique": map[string]interface{}{
								"type":     "boolean",
								"required": true,
							},
						},
						"array": true,
						"type":  "array",
					},
					"enableTtl": map[string]interface{}{
						"type": "boolean",
					},
					"ttl": map[string]interface{}{
						"type": "integer",
					},
					"ttlAttribute": map[string]interface{}{
						"type": "string",
					},
				},
			},
		},
	})

	manager.SupportBackend("dynamodb", DynamoDBBackendBuilder, map[string]interface{}{
		"type": map[string]interface{}{
			"required": true,
			"type":     "string",
		},
		"host": map[string]interface{}{
			"required": true,
			"type":     "string",
		},
		"database": map[string]interface{}{
			"required": true,
			"type":     "string",
		},
		"credentials": map[string]interface{}{
			"type":     "object",
			"required": true,
			"properties": map[string]interface{}{
				"username": map[string]interface{}{
					"type":     "string",
					"required": true,
				},
				"password": map[string]interface{}{
					"type":     "string",
					"required": true,
				},
			},
		},
		"collections": map[string]interface{}{
			"type": "map",
			"key": map[string]interface{}{
				"type": "string",
			},
			"value": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{
						"type":     "string",
						"required": true,
					},
					"indexes": map[string]interface{}{
						"elementType": "object",
						"elementProperties": map[string]interface{}{
							"columns": map[string]interface{}{
								"array":       true,
								"elementType": "string",
								"required":    true,
							},
							"unique": map[string]interface{}{
								"type":     "boolean",
								"required": true,
							},
						},
						"array": true,
					},
					"enableTtl": map[string]interface{}{
						"type": "boolean",
					},
					"ttl": map[string]interface{}{
						"type": "integer",
					},
					"ttlAttribute": map[string]interface{}{
						"type": "string",
					},
					"dynamoHashKey": map[string]interface{}{
						"type":     "string",
						"required": true,
					},
					"dynamoRangeKey": map[string]interface{}{
						"type":     "string",
						"required": true,
					},
					"dynamoReadCapacity": map[string]interface{}{
						"type":     "integer",
						"required": true,
					},
					"dynamoWriteCapacity": map[string]interface{}{
						"type":     "integer",
						"required": true,
					},
					"dynamoGSI": map[string]interface{}{
						"type": "string",
					},
				},
			},
		},
		"awsEndpoint": map[string]interface{}{
			"type":     "string",
			"required": true,
		},
		"awsRegion": map[string]interface{}{
			"type":     "string",
			"required": true,
		},
	})
}

// NewBackendSupport registers new backends
func NewBackendSupport(dbConfig map[string]*config.DBInfo) BackendManager {
	manager := NewBackendManager(dbConfig)
	addSupported(manager)
	return manager
}
