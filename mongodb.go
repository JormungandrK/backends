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
				collectionInfo["TTL"].(int),
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
func PrepareDB(session *mgo.Session, db string, dbCollection string, indexes []string, enableTTL bool, TTL int) (*mgo.Collection, error) {
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
	result, err = mapToInterface(record)
	if err != nil {
		return goa.ErrInternal(err)
	}

	return nil
}

// GetAll fetches all matched records for given filter
func (c *MongoCollection) GetAll(filter map[string]interface{}, order string, limit int, offset int) (interface{}, error) {

	stringToObjectID(filter)
	query := c.Find(filter)
	if order != "" {
		query = query.Sort(order)
	}
	if offset != 0 {
		query = query.Skip(offset)
	}
	if limit != 0 {
		query = query.Limit(limit)
	}

	var results []map[string]interface{}
	err := query.All(&results)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, goa.ErrNotFound(err)
		}
		return nil, goa.ErrInternal(err)
	}

	return results, nil
}

// Save creates new record unless it does not exist, otherwise it updates the record
func (c *MongoCollection) Save(object interface{}, filter map[string]interface{}) (interface{}, error) {

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
		result, err := mapToInterface(payload)
		if err != nil {
			return nil, goa.ErrInternal(err)
		}

		return result, nil

		// filter1 := map[string]interface{}{
		// 	"email": bson.RegEx{"^" + "keitaro", ""},
		// }
		// results, err := c.GetAll(&filter1, "", 0, 0)
		// if err != nil {
		// 	return nil, goa.ErrInternal(err)
		// }
		// fmt.Println("RESULTS: ", results)

		// filter2 := map[string]interface{}{
		// 	"email": bson.RegEx{"^" + "keitaro", ""},
		// }
		// err = c.DeleteAll(&filter2)
		// if err != nil {
		// 	return nil, goa.ErrInternal(err)
		// }
	}

	stringToObjectID(filter)
	err := c.Update(filter, object)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, goa.ErrNotFound(err)
		}
		return nil, goa.ErrInternal(err)
	}

	var result interface{}
	err = c.GetOne(filter, result)
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
