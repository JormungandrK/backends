package backends

import (
	"context"
	"reflect"
	"time"

	"github.com/Microkubes/microservice-tools/config"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// MONGO_CTX_KEY is mongoDB context key
var MONGO_CTX_KEY = "MONGO_SESSION"

// MongoCollection wraps a mgo.Collection to embed methods in models.
type MongoCollection struct {
	*mgo.Collection
	repoDef RepositoryDefinition
}

// MongoDBRepoBuilder builds new mongo collection.
// If it does not exist builder will create it
func MongoDBRepoBuilder(repoDef RepositoryDefinition, backend Backend) (Repository, error) {

	sessionObj := backend.GetFromContext(MONGO_CTX_KEY)
	if sessionObj == nil {
		return nil, ErrBackendError("mongo session not configured")
	}

	session, ok := sessionObj.(*mgo.Session)
	if !ok {
		return nil, ErrBackendError("unknown session type")
	}

	databaseName := backend.GetConfig().DatabaseName
	if databaseName == "" {
		return nil, ErrBackendError("database name is missing and required")
	}

	collectionName := repoDef.GetName()
	if collectionName == "" {
		return nil, ErrBackendError("collection name is missing and required")
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
		Collection: mongoColl,
		repoDef:    repoDef,
	}, nil
}

// MongoDBBackendBuilder returns RepositoriesBackend
func MongoDBBackendBuilder(conf *config.DBInfo, manager BackendManager) (Backend, error) {

	session, err := NewSession(conf.Host, conf.Username, conf.Password, conf.DatabaseName)
	if err != nil {
		return nil, err
	}

	ctx := context.WithValue(context.Background(), MONGO_CTX_KEY, session)
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
func PrepareDB(session *mgo.Session, db string, dbCollection string, indexes []Index, enableTTL bool, TTL int, TTLField string) (*mgo.Collection, error) {

	collection := session.DB(db).C(dbCollection)

	// Define indexes
	for _, elem := range indexes {
		i := elem.GetFields()
		index := mgo.Index{
			Key:        i,
			Unique:     elem.Unique(),
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
			return nil, ErrBackendError("TTL attribute is reqired when TTL is enabled")
		}

		if TTL == 0 {
			return nil, ErrBackendError("TTL value is missing and must be greater than zero")
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
func (c *MongoCollection) GetOne(filter Filter, result interface{}) (interface{}, error) {

	var record map[string]interface{}

	if !c.repoDef.IsCustomID() {
		if err := stringToObjectID(filter); err != nil {
			return nil, err
		}
	}

	err := c.Find(filter).One(&record)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, err
		}
		return nil, err
	}
	if c.repoDef.IsCustomID() {
		record["_id"] = record["_id"].(bson.ObjectId).Hex()
	} else {
		record["id"] = record["_id"].(bson.ObjectId).Hex()
	}

	err = MapToInterface(&record, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetAll fetches all matched records for given filter
func (c *MongoCollection) GetAll(filter Filter, resultsTypeHint interface{}, order string, sorting string, limit int, offset int) (interface{}, error) {
	resultsTypeHint = AsPtr(resultsTypeHint)
	results := NewSliceOfType(resultsTypeHint)

	// Create a pointer to a slice value and set it to the slice
	slicePointer := reflect.New(results.Type())
	slicePointer.Elem().Set(results)

	if !c.repoDef.IsCustomID() {
		if err := stringToObjectID(filter); err != nil {
			return nil, ErrInvalidInput(err)
		}
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

	err := query.All(slicePointer.Interface())
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, ErrNotFound(err)
		}
		return nil, err
	}

	// results is always a Slice
	err = IterateOverSlice(slicePointer.Interface(), func(i int, item interface{}) error {
		if item == nil {
			return nil // ignore
		}

		itemValue := reflect.ValueOf(item)
		itemType := reflect.TypeOf(item)
		if itemType.Kind() == reflect.Ptr {
			// item is pointer to something
			itemType = itemType.Elem()
			itemValue = reflect.Indirect(itemValue)
		}

		if itemType.Kind() == reflect.Map {
			// we have a map[string]<some-type>
			idValue := itemValue.MapIndex(reflect.ValueOf("_id"))
			if idValue.IsValid() {
				// ok,there is such value
				if bsonID, ok := idValue.Interface().(bson.ObjectId); ok {
					idStr := bsonID.Hex()
					if c.repoDef.IsCustomID() {
						// we have a custom handling on property "id", so we'll map _id => HEX(_id)
						itemValue.SetMapIndex(reflect.ValueOf("_id"), reflect.ValueOf(idStr))
					} else {
						// no custom mapping set, so the default behaviour is to map id => HEX(_id)
						itemValue.SetMapIndex(reflect.ValueOf("id"), reflect.ValueOf(idStr))
						itemValue.SetMapIndex(reflect.ValueOf("_id"), reflect.Value{})
					}

				}
			}
		}

		return nil
	})

	return slicePointer.Interface(), nil
}

// Save creates new record unless it does not exist, otherwise it updates the record
func (c *MongoCollection) Save(object interface{}, filter Filter) (interface{}, error) {

	var result interface{}

	payload, err := InterfaceToMap(object)
	if err != nil {
		return nil, err
	}

	if filter == nil {

		id := bson.NewObjectId()
		(*payload)["_id"] = id
		if !c.repoDef.IsCustomID() {
			delete(*payload, "id")
		}

		err = c.Insert(payload)
		if err != nil {
			if mgo.IsDup(err) {
				return nil, ErrAlreadyExists("record already exists!")
			}
			return nil, err
		}

		if !c.repoDef.IsCustomID() {
			(*payload)["id"] = id.Hex()
		}
		err = MapToInterface(payload, &object)
		if err != nil {
			return nil, err
		}

		return object, nil
	}

	if !c.repoDef.IsCustomID() {
		if err := stringToObjectID(filter); err != nil {
			return nil, ErrInvalidInput(err)
		}
	}

	err = c.Update(filter, bson.M{"$set": payload})
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, ErrNotFound(err)
		}
		if mgo.IsDup(err) {
			return nil, ErrAlreadyExists("record already exists!")
		}

		return nil, err
	}

	_, err = c.GetOne(filter, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// DeleteOne deletes only one record for given filter
func (c *MongoCollection) DeleteOne(filter Filter) error {

	if !c.repoDef.IsCustomID() {
		if err := stringToObjectID(filter); err != nil {
			return ErrInvalidInput(err)
		}
	}

	err := c.Remove(filter)
	if err != nil {
		if err == mgo.ErrNotFound {
			return ErrNotFound(err)
		}
		return err
	}

	return nil
}

// DeleteAll deletes all matched records for given filter
func (c *MongoCollection) DeleteAll(filter Filter) error {

	if !c.repoDef.IsCustomID() {
		if err := stringToObjectID(filter); err != nil {
			return ErrInvalidInput(err)
		}
	}

	_, err := c.RemoveAll(filter)
	if err != nil {
		if err == mgo.ErrNotFound {
			return ErrNotFound(err)
		}
		return err
	}

	return nil
}
