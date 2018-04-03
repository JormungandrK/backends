package backends

import (
	"fmt"
	"strings"
	"testing"
)

var backendSchema = map[string]interface{}{
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
							"type":        "array",
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
}

func TestValidateBackendVanilla(t *testing.T) {
	result, err := ValidateBackend(map[string]interface{}{
		"host":     "192.168.1.90:89-9",
		"database": "users",
		"type":     "mongodb",
		"credentials": map[string]interface{}{
			"username": "test",
			"password": "pass",
		},
		"collections": map[string]interface{}{
			"users": map[string]interface{}{
				"name": "users",
				"indexes": []map[string]interface{}{
					map[string]interface{}{
						"columns": []string{"id"},
						"unique":  true,
					},
					map[string]interface{}{
						"columns": []string{"email"},
						"unique":  true,
					},
				},
			},
			"tokens": map[string]interface{}{
				"name": "tokens",
				"indexes": []map[string]interface{}{
					map[string]interface{}{
						"columns": []string{"id"},
						"unique":  true,
					},
					map[string]interface{}{
						"columns": []string{"token"},
						"unique":  true,
					},
				},
				"enableTtl":    true,
				"ttl":          3600000,
				"ttlAttribute": "createdAt",
			},
		},
	}, backendSchema)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid {
		t.Fatal(fmt.Sprintf("Validation errors:\n%s", strings.Join(result.Errors, "\n")))
	}
}
