package backends

import (
	"context"
	"sync"
	"testing"

	"github.com/Microkubes/microservice-tools/config"
	"github.com/guregu/dynamo"
)

var props = map[string]interface{}{
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
}

var collectionInfo = RepositoryDefinitionMap{
	"name":          "tokens",
	"indexes":       []Index{NewUniqueIndex("token")},
	"hashKey":       "token",
	"readCapacity":  int64(5),
	"writeCapacity": int64(5),
	"GSI": map[string]interface{}{
		"token": map[string]interface{}{
			"readCapacity":  2,
			"writeCapacity": 2,
		},
	},
	"enableTtl":    true,
	"ttlAttribute": "created_at",
	"ttl":          86400,
}

var repoBuilder = &RepositoriesBackend{
	DBInfo:            &config.DBInfo{},
	mutex:             &sync.Mutex{},
	repositories:      map[string]Repository{},
	repositoryBuilder: repoBuilderFn,
	ctx:               context.Background(),
	cleanupFn:         func() {},
}

var backendManager = &DefaultBackendManager{
	backendBuilders: map[string]BackendBuilder{},
	backendProps: map[string]interface{}{
		"key": "value",
	},
	backends: map[string]Backend{},
	dbConfig: map[string]*config.DBInfo{
		"some-db": &config.DBInfo{},
	},
	mutex: &sync.Mutex{},
}

func backendBuilderFn(dbInfo *config.DBInfo, manager BackendManager) (Backend, error) {
	return repoBuilder, nil
}

func repoBuilderFn(repoDef RepositoryDefinition, backend Backend) (Repository, error) {
	repo := DynamoCollection{
		&dynamo.Table{},
		&collectionInfo,
	}

	return &repo, nil
}

func TestGetIndexes(t *testing.T) {
	indexes := collectionInfo.GetIndexes()

	if len(indexes) != 1 {
		t.Errorf("Expected indexes lengtth was 1, got %d", len(indexes))
	}
}

func TestGetName(t *testing.T) {
	name := collectionInfo.GetName()

	if name == "" {
		t.Errorf("Expected name was tokens, got %s", name)
	}
}

func TestEnableTTL(t *testing.T) {
	ttl := collectionInfo.EnableTTL()

	if !ttl {
		t.Errorf("Expected was to enable ttl")
	}
}

func TestGetTTLL(t *testing.T) {
	ttl := collectionInfo.GetTTL()

	if ttl != 86400 {
		t.Errorf("Expected ttl value was 86400, got %d", ttl)
	}
}

func TestGetTTLAttribute(t *testing.T) {
	attr := collectionInfo.GetTTLAttribute()

	if attr != "created_at" {
		t.Errorf("Expected ttl attribute was created_at, got %s", attr)
	}
}

func TestGetHashKey(t *testing.T) {
	hashKey := collectionInfo.GetHashKey()

	if hashKey != "token" {
		t.Errorf("Expected hash key was token, got %s", hashKey)
	}
}

func TestGetRangeKey(t *testing.T) {
	rangeKey := collectionInfo.GetRangeKey()

	if rangeKey != "" {
		t.Errorf("Expected range key to not be set")
	}
}

func TestGetReadCapacity(t *testing.T) {
	readCapacity := collectionInfo.GetReadCapacity()

	if readCapacity != 5 {
		t.Errorf("Expected read capacity was 5, got %d", readCapacity)
	}
}

func TestGetWriteCapacity(t *testing.T) {
	writeCapacity := collectionInfo.GetWriteCapacity()

	if writeCapacity != 5 {
		t.Errorf("Expected read capacity was 5, got %d", writeCapacity)
	}
}

func TestGetGSI(t *testing.T) {
	gsi := collectionInfo.GetGSI()

	if gsi == nil {
		t.Errorf("Nil GSI")
	}
}

func TestDefineRepository(t *testing.T) {
	r, err := repoBuilder.DefineRepository("test-repo", collectionInfo)
	if r == nil {
		t.Errorf("Got empty repositiry")
	}

	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestGetRepository(t *testing.T) {
	r, err := repoBuilder.GetRepository("test-repo")

	if r == nil {
		t.Errorf("Got nil repositiry")
	}

	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestGetConfig(t *testing.T) {
	conf := repoBuilder.GetConfig()

	if conf == nil {
		t.Errorf("Got empty config")
	}
}

func TestSetInContext(t *testing.T) {
	repoBuilder.SetInContext("key", repoBuilder)
}

func TestGetFromContext(t *testing.T) {
	value := repoBuilder.GetFromContext("key")

	if value == nil {
		t.Errorf("Empty context")
	}
}

func TestShutdown(t *testing.T) {
	repoBuilder.Shutdown()
}

func TestSupportBackend(t *testing.T) {
	backendManager.SupportBackend("some-db", backendBuilderFn, props)
}

func TestGetBackend(t *testing.T) {
	backend, err := backendManager.GetBackend("some-db")

	if backend == nil {
		t.Errorf("Got nil backend")
	}

	if err != nil {
		t.Errorf(err.Error())
	}
}

func TestGetSupportedBackends(t *testing.T) {
	backends := backendManager.GetSupportedBackends()

	if len(backends) == 0 {
		t.Errorf("Expected to got supported backends, got %d", len(backends))
	}
}

func TestGetRequiredBackendProperties(t *testing.T) {
	props, err := backendManager.GetRequiredBackendProperties("some-db")

	if props == nil {
		t.Errorf("Got emptu required props")
	}

	if err != nil {
		t.Errorf(err.Error())
	}
}
