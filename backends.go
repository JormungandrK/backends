package backends

import (
	"context"
	"fmt"
	"sync"

	"github.com/JormungandrK/microservice-tools/config"
)

// Repository defines the interface for accessing the data
type Repository interface {
	GetOne(filter map[string]interface{}, result interface{}) error
	GetAll(filter map[string]interface{}, results interface{}, order string, sorting string, limit int, offset int) error
	Save(object interface{}, filter map[string]interface{}) (interface{}, error)
	DeleteOne(filter map[string]interface{}) error
	DeleteAll(filter map[string]interface{}) error
}

// RepositoryDefinition defines interface for accessing collection props
type RepositoryDefinition interface {
	GetName() string
	GetIndexes() []string
	EnableTTL() bool
	GetTTL() int
	GetTTLAttribute() string
	GetHashKey() string
	GetRangeKey() string
	GetReadCapacity() int64
	GetWriteCapacity() int64
	GetGSI() map[string]interface{}
}

// Backend defines interface for defining the repository
type Backend interface {
	DefineRepository(name string, def RepositoryDefinition) (Repository, error)
	GetRepository(name string) (Repository, error)
	GetConfig() *config.DBInfo
	GetFromContext(key string) interface{}
	SetInContext(key string, value interface{})
	Shutdown()
}

// BackendManager defines interface for managing the backend
type BackendManager interface {
	GetBackend(backendType string) (Backend, error)
	SupportBackend(backendType string, builder BackendBuilder, properties map[string]interface{})
	GetSupportedBackends() []string
	GetRequiredBackendProperties(backendType string) (map[string]interface{}, error)
}

// BackendBuilder builds the backend
type BackendBuilder func(conf *config.DBInfo, manager BackendManager) (Backend, error)

// RepoBuilder builds the repo (collection or table)
type RepoBuilder func(def RepositoryDefinition, backend Backend) (Repository, error)

// RepositoryDefinitionMap is the configuration map
type RepositoryDefinitionMap map[string]interface{}

// BackendCleanup is the collection/table clean  up func
type BackendCleanup func()

// DefaultBackendManager represents the backend store
type DefaultBackendManager struct {
	backendBuilders map[string]BackendBuilder
	backends        map[string]Backend
	backendProps    map[string]interface{}
	dbConfig        map[string]*config.DBInfo
	mutex           *sync.Mutex
}

// RepositoriesBackend represents the repository store
type RepositoriesBackend struct {
	repositories      map[string]Repository
	repositoryBuilder RepoBuilder
	mutex             *sync.Mutex
	DBInfo            *config.DBInfo
	ctx               context.Context
	cleanupFn         BackendCleanup
}

// GetIndexes returns the indexes for colletion or table
func (m RepositoryDefinitionMap) GetIndexes() []string {
	indexes := []string{}

	if indxsif, ok := m["indexes"]; ok {
		return indxsif.([]string)
	}

	return indexes
}

// GetName returns the collection/table name
func (m RepositoryDefinitionMap) GetName() string {
	if name, ok := m["name"]; ok {
		return name.(string)
	}

	return ""
}

// EnableTTL set the TTL for collection or table
func (m RepositoryDefinitionMap) EnableTTL() bool {
	if ttlEnabled, ok := m["enableTtl"]; ok {
		return ttlEnabled.(bool)
	}

	return false
}

// GetTTL returns the time in seconds for TTL
func (m RepositoryDefinitionMap) GetTTL() int {
	if ttl, ok := m["ttl"]; ok {
		return ttl.(int)
	}

	return 0
}

// GetTTLAttribute returns the TTL attribute
func (m RepositoryDefinitionMap) GetTTLAttribute() string {
	if ttlField, ok := m["ttlAttribute"]; ok {
		return ttlField.(string)
	}

	return ""
}

// GetHashKey return the hashKey for dynamoDB
func (m RepositoryDefinitionMap) GetHashKey() string {
	if hashKey, ok := m["hashKey"]; ok {
		return hashKey.(string)
	}

	return ""
}

// GetRangeKey return the rangeKey for dynamoDB
func (m RepositoryDefinitionMap) GetRangeKey() string {
	if rangeKey, ok := m["rangeKey"]; ok {
		return rangeKey.(string)
	}

	return ""
}

// GetReadCapacity return the read capacity for dynamoDB table
func (m RepositoryDefinitionMap) GetReadCapacity() int64 {
	if readCapacity, ok := m["readCapacity"]; ok {
		return readCapacity.(int64)
	}

	return 0
}

// GetWriteCapacity return the write capacity for dynamoDB table
func (m RepositoryDefinitionMap) GetWriteCapacity() int64 {
	if writeCapacity, ok := m["writeCapacity"]; ok {
		return writeCapacity.(int64)
	}

	return 0
}

// GetGSI returns global secondary indexes
func (m RepositoryDefinitionMap) GetGSI() map[string]interface{} {
	if gsi, ok := m["GSI"]; ok {
		return gsi.(map[string]interface{})
	}

	return nil
}

// DefineRepository defines the repository (collection/table)
func (m *RepositoriesBackend) DefineRepository(name string, def RepositoryDefinition) (Repository, error) {

	if repository, ok := m.repositories[name]; ok {
		return repository, nil
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	repository, err := m.repositoryBuilder(def, m)
	if err != nil {
		return nil, err
	}

	m.repositories[name] = repository
	return repository, nil
}

// GetRepository return the repository (collection/table)
func (m *RepositoriesBackend) GetRepository(name string) (Repository, error) {
	if repo, ok := m.repositories[name]; ok {
		return repo, nil
	}

	return nil, fmt.Errorf("unknown repo")
}

// GetConfig return the config
func (m *RepositoriesBackend) GetConfig() *config.DBInfo {
	return m.DBInfo
}

// GetFromContext returns from config
func (m *RepositoriesBackend) GetFromContext(key string) interface{} {
	return m.ctx.Value(key)
}

// SetInContext sets in context
func (m *RepositoriesBackend) SetInContext(key string, value interface{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.ctx = context.WithValue(m.ctx, key, value)
}

// Shutdown close the session
func (m *RepositoriesBackend) Shutdown() {
	if m.cleanupFn != nil {
		m.cleanupFn()
	}
}

// GetBackend returns the RepositoryBackend
func (m *DefaultBackendManager) GetBackend(backendType string) (Backend, error) {
	if backend, ok := m.backends[backendType]; ok {
		return backend, nil
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()

	backend, err := m.buildBackend(backendType)
	if err != nil {
		return nil, err
	}
	return backend, nil
}

// SupportBackend register the DB builder function and required props for the DB
func (m *DefaultBackendManager) SupportBackend(backendType string, builder BackendBuilder, properties map[string]interface{}) {
	m.backendBuilders[backendType] = builder
	m.backendProps[backendType] = properties
}

// GetSupportedBackends returns the supported backedns
func (m *DefaultBackendManager) GetSupportedBackends() []string {
	supported := []string{}

	for backendType, _ := range m.backendBuilders {
		supported = append(supported, backendType)
	}

	return supported
}

// GetRequiredBackendProperties returns the required props for the selected backend
func (m *DefaultBackendManager) GetRequiredBackendProperties(backendType string) (map[string]interface{}, error) {
	if props, ok := m.backendProps[backendType]; ok {
		return props.(map[string]interface{}), nil
	}
	return nil, fmt.Errorf("backend not supported")
}

// buildBackend builds new backend
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

// NewRepositoriesBackend sets new RepositoriesBackend
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

// NewBackendManager returns new backend manager
func NewBackendManager(dbConfig map[string]*config.DBInfo) BackendManager {
	return &DefaultBackendManager{
		backendBuilders: map[string]BackendBuilder{},
		backendProps:    map[string]interface{}{},
		backends:        map[string]Backend{},
		dbConfig:        dbConfig,
		mutex:           &sync.Mutex{},
	}
}
