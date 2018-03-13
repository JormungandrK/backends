package backends

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/JormungandrK/microservice-tools/config"
	"github.com/goadesign/goa"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// MongoCollection wraps a mgo.Collection to embed methods in models.
type MongoCollection struct {
	*mgo.Collection
}

// mutex is an exclusion lock
var mutexMongo = &sync.Mutex{}

// MongoDBRepoBuilder builds new mongo collection.
// If it does not exist builder will create it
func MongoDBRepoBuilder(repoDef RepositoryDefinition, backend Backend) (Repository, error) {

	sessionObj := backend.GetFromContext("MONGO_SESSION")
	if sessionObj == nil {
		return nil, fmt.Errorf("mongo session not configured")
	}

	session, ok := sessionObj.(*mgo.Session)
	if !ok {
		return nil, fmt.Errorf("unknown session type")
	}

	databaseName := backend.GetConfig().DatabaseName
	if databaseName == "" {
		return nil, fmt.Errorf("database name is missing and required")
	}

	collectionName := repoDef.GetName()
	if collectionName == "" {
		return nil, fmt.Errorf("collection name is missing and required")
	}

	mongoColl, err := PrepareDB(
		session,
		databaseName,
		collectionName,
		repoDef.GetIndexes(),
		repoDef.EnableTTL(),
		repoDef.GetTTL(),
		repoDef.GetTTLAttribute(),
	)

	if err != nil {
		return nil, err
	}

	return &MongoCollection{
		mongoColl,
	}, nil
}

// MongoDBBackendBuilder returns RepositoriesBackend
func MongoDBBackendBuilder(conf *config.DBInfo, manager BackendManager) (Backend, error) {

	session, err := NewSession(conf.Host, conf.Username, conf.Password, conf.DatabaseName)
	if err != nil {
		return nil, err
	}

	ctx := context.WithValue(context.Background(), "MONGO_SESSION", session)
	cleanup := func() {
		session.Close()
	}

	return NewRepositoriesBackend(ctx, conf, MongoDBRepoBuilder, cleanup), nil
}

// NewSession returns a new Mongo Session.
func NewSession(Host string, Username string, Password string, Database string) (*mgo.Session, error) {

	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:    []string{Host},
		Username: Username,
		Password: Password,
		Database: Database,
		Timeout:  30 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	// SetMode - consistency mode for the session.
	session.SetMode(mgo.Monotonic, true)

	return session, nil
}

// PrepareDB ensure presence of persistent and immutable data in the DB. It creates indexes
func PrepareDB(session *mgo.Session, db string, dbCollection string, indexes []string, enableTTL bool, TTL int, TTLField string) (*mgo.Collection, error) {

	mutexMongo.Lock()
	defer mutexMongo.Unlock()

	collection := session.DB(db).C(dbCollection)

	// Define indexes
	for _, elem := range indexes {
		i := []string{elem}
		index := mgo.Index{
			Key:        i,
			Unique:     true,
			DropDups:   true,
			Background: true,
			Sparse:     true,
		}

		// Create indexes
		if err := collection.EnsureIndex(index); err != nil {
			return nil, err
		}
	}

	if enableTTL == true {
		if TTLField == "" {
			return nil, fmt.Errorf("TTL attribute is reqired when TTL is enabled")
		}

		if TTL == 0 {
			return nil, fmt.Errorf("TTL value is missing and must be greater than zero")
		}

		index := mgo.Index{
			Key:         []string{TTLField},
			Unique:      false,
			DropDups:    false,
			Background:  true,
			Sparse:      true,
			ExpireAfter: time.Duration(TTL) * time.Second,
		}
		if err := collection.EnsureIndex(index); err != nil {
			return nil, err
		}

	}

	return collection, nil
}

// GetOne fetches only one record for given filter
func (c *MongoCollection) GetOne(filter map[string]interface{}, result interface{}) error {

	var record map[string]interface{}

	if err := stringToObjectID(filter); err != nil {
		return goa.ErrBadRequest(err)
	}

	err := c.Find(filter).One(&record)
	if err != nil {
		if err == mgo.ErrNotFound {
			return goa.ErrNotFound(err)
		}
		return goa.ErrInternal(err)
	}

	record["id"] = record["_id"].(bson.ObjectId).Hex()
	err = MapToInterface(&record, &result)
	if err != nil {
		return goa.ErrInternal(err)
	}

	return nil
}

// GetAll fetches all matched records for given filter
func (c *MongoCollection) GetAll(filter map[string]interface{}, results interface{}, order string, sorting string, limit int, offset int) error {

	var records []map[string]interface{}

	if err := stringToObjectID(filter); err != nil {
		return goa.ErrBadRequest(err)
	}

	query := c.Find(filter)
	if order != "" {
		if sorting == "desc" {
			order = "-" + order
		}
		query = query.Sort(order)
	}
	if offset != 0 {
		query = query.Skip(offset)
	}
	if limit != 0 {
		query = query.Limit(limit)
	}

	err := query.All(&records)
	if err != nil {
		if err == mgo.ErrNotFound {
			return goa.ErrNotFound(err)
		}
		return goa.ErrInternal(err)
	}

	for index, v := range records {
		v["id"] = v["_id"].(bson.ObjectId).Hex()
		records[index] = v
	}

	err = MapToInterface(&records, &results)
	if err != nil {
		return goa.ErrInternal(err)
	}

	return nil
}

// Save creates new record unless it does not exist, otherwise it updates the record
func (c *MongoCollection) Save(object interface{}, filter map[string]interface{}) (interface{}, error) {

	var result interface{}

	payload, err := InterfaceToMap(object)
	if err != nil {
		return nil, goa.ErrInternal(err)
	}

	if filter == nil {

		id := bson.NewObjectId()
		(*payload)["_id"] = id
		delete(*payload, "id")

		err = c.Insert(payload)
		if err != nil {
			if mgo.IsDup(err) {
				return nil, goa.ErrBadRequest("record already exists!")
			}
			return nil, goa.ErrInternal(err)
		}

		(*payload)["id"] = id.Hex()
		err = MapToInterface(payload, &result)
		if err != nil {
			return nil, goa.ErrInternal(err)
		}

		return result, nil
	}

	if err := stringToObjectID(filter); err != nil {
		return nil, goa.ErrBadRequest(err)
	}

	err = c.Update(filter, bson.M{"$set": payload})
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, goa.ErrNotFound(err)
		}
		if mgo.IsDup(err) {
			return nil, goa.ErrBadRequest("record already exists!")
		}

		return nil, goa.ErrInternal(err)
	}

	err = c.GetOne(filter, &result)
	if err != nil {
		return nil, goa.ErrInternal(err)
	}

	return result, nil
}

// DeleteOne deletes only one record for given filter
func (c *MongoCollection) DeleteOne(filter map[string]interface{}) error {

	if err := stringToObjectID(filter); err != nil {
		return goa.ErrBadRequest(err)
	}

	err := c.Remove(filter)
	if err != nil {
		if err == mgo.ErrNotFound {
			return goa.ErrNotFound(err)
		}
		return goa.ErrInternal(err)
	}

	return nil
}

// DeleteAll deletes all matched records for given filter
func (c *MongoCollection) DeleteAll(filter map[string]interface{}) error {

	if err := stringToObjectID(filter); err != nil {
		return goa.ErrBadRequest(err)
	}

	_, err := c.RemoveAll(filter)
	if err != nil {
		if err == mgo.ErrNotFound {
			return goa.ErrNotFound(err)
		}
		return goa.ErrInternal(err)
	}

	return nil
}
