package backends

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/JormungandrK/microservice-tools/config"
)

// Repository defaines the interface for accessing the data
type Repository interface {
	GetOne(filter map[string]interface{}, result interface{}) error
	GetAll(filter map[string]interface{}, results interface{}, order string, sorting string, limit int, offset int) error
	Save(object interface{}, filter map[string]interface{}) (interface{}, error)
	DeleteOne(filter map[string]interface{}) error
	DeleteAll(filter map[string]interface{}) error
}

type RepositoryDefinition interface {
	GetName() string
	GetIndexes() []interface{}
	EnableTTL() bool
	GetTTL() int64
}

type Backend interface {
	DefineRepository(name string, def RepositoryDefinition) (Repository, error)
	GetRepository(name string) (Repository, error)
	GetConfig() *config.DBInfo
	GetFromContext(key string) interface{}
	SetInContext(key string, value interface{})
	Shutdown()
}

type RepoBuilder func(def RepositoryDefinition, backend Backend) (Repository, error)

type BackendBuilder_2 func(conf *config.DBInfo, manager BackendManager) (Backend, error)

type BackendManager interface {
	GetBackend(backendType string) (Backend, error)
	SupportBackend(backendType string, builder BackendBuilder_2, properties map[string]interface{})
	GetSupportedBackends() []string
	GetRequiredBackendProperties(backendType string) (map[string]interface{}, error)
}

type BackendCleanup func()

type RepositoryDefinitionMap map[string]interface{}

func (m RepositoryDefinitionMap) GetIndexes() []interface{} {
	indexes := []interface{}{}

	if indxsif, ok := m["indexes"]; ok {
		if indexesArr, ok := indxsif.([]interface{}); ok {
			return indexesArr
		}
	}

	return indexes
}

func (m RepositoryDefinitionMap) GetName() string {
	if name, ok := m["name"]; ok {
		return name.(string)
	}
	return ""
}

func (m RepositoryDefinitionMap) EnableTTL() bool {
	if ttlEnabled, ok := m["enableTtl"]; ok {
		return ttlEnabled.(bool)
	}
	return false
}

func (m RepositoryDefinitionMap) GetTTL() int64 {
	if ttl, ok := m["ttl"]; ok {
		return ttl.(int64)
	}
	return 0
}

type RepositoriesBackend struct {
	repositories      map[string]Repository
	repositoryBuilder RepoBuilder
	mutex             *sync.Mutex
	DBInfo            *config.DBInfo
	ctx               context.Context
	cleanupFn         BackendCleanup
}

type BackendContextKey string

func (m *RepositoriesBackend) DefineRepository(name string, def RepositoryDefinition) (Repository, error) {

	if repository, ok := m.repositories[name]; ok {
		return repository, nil
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	if repository, ok := m.repositories[name]; ok {
		return repository, nil
	}

	repository, err := m.repositoryBuilder(def, m)
	if err != nil {
		return nil, err
	}
	m.repositories[name] = repository
	return repository, nil
}

func (m *RepositoriesBackend) GetRepository(name string) (Repository, error) {
	if repo, ok := m.repositories[name]; ok {
		return repo, nil
	}
	return nil, fmt.Errorf("unknown repo")
}

func (m *RepositoriesBackend) GetConfig() *config.DBInfo {
	return m.DBInfo
}

func (m *RepositoriesBackend) GetFromContext(key string) interface{} {
	return m.ctx.Value(key)
}
func (m *RepositoriesBackend) SetInContext(key string, value interface{}) {
	m.mutex.Lock() // FIXME: Maybe use another mutex for the context
	defer m.mutex.Unlock()
	m.ctx = context.WithValue(m.ctx, BackendContextKey(key), value)
}

func (m *RepositoriesBackend) Shutdown() {
	if m.cleanupFn != nil {
		m.cleanupFn()
	}
}

func NewRepositoriesBackend(ctx context.Context, dbInfo *config.DBInfo, repoBuilder RepoBuilder, cleanup BackendCleanup) Backend {
	return &RepositoriesBackend{
		DBInfo:            dbInfo,
		mutex:             &sync.Mutex{},
		repositories:      map[string]Repository{},
		repositoryBuilder: repoBuilder,
		ctx:               ctx,
		cleanupFn:         cleanup,
	}
}

type DefaultBackendManager struct {
	backendBuilders map[string]BackendBuilder_2
	backends        map[string]Backend
	mutex           *sync.Mutex
	backendProps    map[string]interface{}
	dbConfig        map[string]*config.DBInfo
}

func (m *DefaultBackendManager) GetBackend(backendType string) (Backend, error) {
	if backend, ok := m.backends[backendType]; ok {
		return backend, nil
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if backend, ok := m.backends[backendType]; ok {
		return backend, nil
	}

	backend, err := m.buildBackend(backendType)
	if err != nil {
		return nil, err
	}
	return backend, nil
}

func (m *DefaultBackendManager) SupportBackend(backendType string, builder BackendBuilder_2, properties map[string]interface{}) {
	m.backendBuilders[backendType] = builder
	m.backendProps[backendType] = properties
}

func (m *DefaultBackendManager) GetSupportedBackends() []string {
	supported := []string{}

	for backendType, _ := range m.backendBuilders {
		supported = append(supported, backendType)
	}

	return supported
}

func (m *DefaultBackendManager) GetRequiredBackendProperties(backendType string) (map[string]interface{}, error) {
	if props, ok := m.backendProps[backendType]; ok {
		return props.(map[string]interface{}), nil
	}
	return nil, fmt.Errorf("backend not supported")
}

func (m *DefaultBackendManager) buildBackend(backendType string) (Backend, error) {
	if backendBuilder, ok := m.backendBuilders[backendType]; ok {
		dbInfo, ok := m.dbConfig[backendType]
		if !ok || dbInfo == nil {
			return nil, fmt.Errorf("backend not configured")
		}
		backend, err := backendBuilder(dbInfo, m)
		if err != nil {
			return nil, err
		}
		m.backends[backendType] = backend
		return backend, nil
	}
	return nil, fmt.Errorf("backend not supported")
}

func NewBackendManager(dbConfig map[string]*config.DBInfo) BackendManager {
	return &DefaultBackendManager{
		backendBuilders: map[string]BackendBuilder_2{},
		backendProps:    map[string]interface{}{},
		backends:        map[string]Backend{},
		dbConfig:        dbConfig,
		mutex:           &sync.Mutex{},
	}
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
