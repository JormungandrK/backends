package backends

import (
	"time"

	"github.com/JormungandrK/microservice-tools/config"
	"github.com/goadesign/goa"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// init creates mongoDB backend builder and add it as knows backend type
func init() {
	builder := func(dbInfo *config.DBInfo) (map[string]Repository, error) {
		mongoSession, err := NewSession(dbInfo.Host, dbInfo.Username, dbInfo.Password, dbInfo.DatabaseName)
		if err != nil {
			return nil, err
		}

		collections := map[string]Repository{}
		for collection, collectionInfo := range dbInfo.Collections {
			indexes := []string{}
			for _, index := range collectionInfo["indexes"].([]interface{}) {
				indexes = append(indexes, index.(string))
			}

			collectionDB, err := PrepareDB(
				mongoSession,
				dbInfo.DatabaseName,
				collection,
				indexes,
				collectionInfo["enableTTL"].(bool),
				collectionInfo["TTL"].(float64),
			)
			if err != nil {
				return nil, err
			}

			collections[collection] = &MongoCollection{collectionDB}
		}

		return collections, nil
	}

	AddBackendType("mongodb", builder)
}

// MongoCollection wraps a mgo.Collection to embed methods in models.
type MongoCollection struct {
	*mgo.Collection
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

// PrepareDB ensure presence of persistent and immutable data in the DB.
func PrepareDB(session *mgo.Session, db string, dbCollection string, indexes []string, enableTTL bool, TTL float64) (*mgo.Collection, error) {
	// Create collection
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
		index := mgo.Index{
			Key:         []string{"created_at"},
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

	stringToObjectID(filter)
	err := c.Find(filter).One(&record)
	if err != nil {
		if err == mgo.ErrNotFound {
			return goa.ErrNotFound(err)
		}
		return goa.ErrInternal(err)
	}

	record["id"] = record["_id"].(bson.ObjectId).Hex()
	err = mapToInterface(&record, &result)
	if err != nil {
		return goa.ErrInternal(err)
	}

	return nil
}

// GetAll fetches all matched records for given filter
func (c *MongoCollection) GetAll(filter map[string]interface{}, results interface{}, order string, sorting string, limit int, offset int) error {

	var records []map[string]interface{}

	stringToObjectID(filter)
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

	err = mapToInterface(&records, &results)
	if err != nil {
		return goa.ErrInternal(err)
	}

	return nil
}

// Save creates new record unless it does not exist, otherwise it updates the record
func (c *MongoCollection) Save(object interface{}, filter map[string]interface{}) (interface{}, error) {

	var result interface{}

	if filter == nil {
		payload, err := interfaceToMap(object)
		if err != nil {
			return nil, goa.ErrInternal(err)
		}

		id := bson.NewObjectId()
		(*payload)["_id"] = id

		err = c.Insert(payload)
		if err != nil {
			if mgo.IsDup(err) {
				return nil, goa.ErrBadRequest("record already exists!")
			}
			return nil, goa.ErrInternal(err)
		}

		(*payload)["id"] = id.Hex()
		err = mapToInterface(payload, &result)
		if err != nil {
			return nil, goa.ErrInternal(err)
		}

		return result, nil
	}

	stringToObjectID(filter)
	err := c.Update(filter, object)
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

	stringToObjectID(filter)
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

	stringToObjectID(filter)
	_, err := c.RemoveAll(filter)
	if err != nil {
		if err == mgo.ErrNotFound {
			return goa.ErrNotFound(err)
		}
		return goa.ErrInternal(err)
	}

	return nil
}
