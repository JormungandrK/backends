package backends

import (
	"strings"

	"github.com/JormungandrK/microservice-tools/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/goadesign/goa"
	"github.com/guregu/dynamo"
	"github.com/satori/go.uuid"
)

type Keys struct {
	HashKey  string
	RangeKey string
}

var KEYS = map[string]Keys{}

// init creates dynamoDB backend builder and add it as knows backend type
func init() {
	builder := func(dbInfo *config.DBInfo) (map[string]Repository, error) {
		sess, err := session.NewSession(&aws.Config{
			Region:      aws.String(dbInfo.AWSRegion),
			Credentials: credentials.NewSharedCredentials(dbInfo.AWSCredentials, ""),
		})

		if err != nil {
			return nil, err
		}

		db := dynamo.New(sess)

		KEYS["users"] = Keys{"email", "id"}
		KEYS["tokens"] = Keys{"email", "id"}

		usersTable := db.Table("users")
		tokensTable := db.Table("tokens")

		collections := map[string]Repository{
			"users":  &DynamoCollection{&usersTable},
			"tokens": &DynamoCollection{&tokensTable},
		}

		return collections, nil
	}

	AddBackendType("dynamodb", builder)
}

// DynamoCollection wraps a dynamo.Table to embed methods in models.
type DynamoCollection struct {
	*dynamo.Table
}

// GetOne looks up for an item by given filter
// Example filter:
//	filter := map[string]interface{}{
// 		"id":    "54acb6c5-baeb-4213-b10f-e707a6055e64",
// }
func (c *DynamoCollection) GetOne(filter map[string]interface{}, result interface{}) error {

	var record map[string]interface{}
	var records []map[string]interface{}

	var query []string
	var args []interface{}
	for k, v := range filter {
		query = append(query, "$ = ?")
		args = append(args, k)
		args = append(args, v)
	}

	err := c.Scan().Filter(strings.Join(query, " AND "), args...).Consistent(true).All(&records)
	if err != nil {
		return goa.ErrInternal(err)
	}
	if records == nil {
		return goa.ErrNotFound("not found")
	}

	record = records[0]
	err = mapToInterface(&record, &result)
	if err != nil {
		return goa.ErrInternal(err)
	}

	return nil
}

func (c *DynamoCollection) GetAll(filter map[string]interface{}, results interface{}, order string, sorting string, limit int, offset int) error {

	var records []map[string]interface{}

	var query []string
	var args []interface{}
	for k, v := range filter {
		query = append(query, "$ = ?")
		args = append(args, k)
		args = append(args, v)
	}

	err := c.Scan().Filter(strings.Join(query, " AND "), args...).Consistent(true).All(&records)
	if err != nil {
		return goa.ErrInternal(err)
	}
	if records == nil {
		return goa.ErrNotFound("not found")
	}

	if offset != 0 {
		records = records[offset:]
	}
	if limit != 0 {
		records = records[0:limit]
	}

	err = mapToInterface(&records, &results)
	if err != nil {
		return goa.ErrInternal(err)
	}

	return nil

}

// Save creates new item or updates the existing one
func (c *DynamoCollection) Save(object interface{}, filter map[string]interface{}) (interface{}, error) {

	var result interface{}

	payload, err := interfaceToMap(object)
	if err != nil {
		return nil, goa.ErrInternal(err)
	}

	collectionName := c.Name()
	hashKey := KEYS[collectionName].HashKey
	rangeKey := KEYS[collectionName].RangeKey

	if filter == nil {
		// Create item

		id := uuid.NewV4().String()
		(*payload)["id"] = id

		av, err := dynamodbattribute.MarshalMap(payload)
		if err != nil {
			return nil, goa.ErrInternal(err)
		}

		err = c.Put(av).If("attribute_not_exists($)", hashKey).Run()
		if err != nil {
			if IsConditionalCheckErr(err) {
				return nil, goa.ErrBadRequest("record already exists!")
			}
			return nil, goa.ErrInternal(err)
		}

	} else {
		// Update item

		var item interface{}
		err = c.GetOne(filter, &item)
		if err != nil {
			return nil, err
		}
		res := item.(map[string]interface{})

		query := c.Update(hashKey, res[hashKey])
		if rangeKey != "" {
			query = query.Range(rangeKey, res[rangeKey])
		}

		for k, v := range *payload {
			if k != hashKey && k != rangeKey {
				query = query.Set(k, v)
			}
		}

		var updatedItem map[string]interface{}
		err = query.Value(&updatedItem)
		if err != nil {
			return nil, goa.ErrInternal(err)
		}

		payload = &updatedItem
	}

	err = mapToInterface(payload, &result)
	if err != nil {
		return nil, goa.ErrInternal(err)
	}

	return result, nil
}

// DeleteOne deletes only one item at the time
// Example filter:
//	filter := map[string]interface{}{
// 		"email": "keitaro-user1@keitaro.com",
// }
func (c *DynamoCollection) DeleteOne(filter map[string]interface{}) error {

	collectionName := c.Name()
	hashKey := KEYS[collectionName].HashKey
	rangeKey := KEYS[collectionName].RangeKey

	var item interface{}
	err := c.GetOne(filter, &item)
	if err != nil {
		return err
	}
	result := item.(map[string]interface{})

	query := c.Delete(hashKey, result[hashKey])

	if rangeKey != "" {
		query = query.Range(rangeKey, result[rangeKey])
	}

	var old map[string]interface{}
	err = query.OldValue(&old)
	if err != nil {
		if err == dynamo.ErrNotFound {
			return goa.ErrNotFound(err)
		}
		return goa.ErrInternal(err)
	}

	return nil
}

// DeleteAll deletes batch of items
// Example filter:
// filter := map[string]interface{}{
// 			"email": []string{"keitaro-user1@keitaro.com", "keitaro-user1@keitaro.com"},
// 			"id":    []string{"378d9777-6a32-4453-849e-858ff243635b", "462e5d47-b88c-4de7-9aaf-89f6c718dddc"},
// 		}
// email is the hash key, id is the range key
func (c *DynamoCollection) DeleteAll(filter map[string]interface{}) error {

	collectionName := c.Name()
	hashKey := KEYS[collectionName].HashKey
	rangeKey := KEYS[collectionName].RangeKey
	hashAndRangeKeyName := []string{hashKey}
	if rangeKey != "" {
		hashAndRangeKeyName = append(hashAndRangeKeyName, rangeKey)
	}

	hashValues, ok := filter[hashKey].([]string)
	if !ok {
		return goa.ErrInternal("hash key not specified in the filter")
	}

	rangeValues := []string{}
	if rangeKey != "" {
		rangeValues, ok = filter[rangeKey].([]string)
		if !ok {
			return goa.ErrInternal("range key not specified in the filter")
		}

		if len(hashValues) != len(rangeValues) {
			return goa.ErrInternal("length of the values for hash and range key in the filter must be equal")
		}
	}

	var keys []dynamo.Keyed
	for index, _ := range hashValues {
		if len(rangeValues) > 0 {
			keys = append(keys, dynamo.Keys{hashValues[index], rangeValues[index]})
		} else {
			keys = append(keys, dynamo.Keys{hashValues[index]})
		}
	}

	_, err := c.Batch(hashAndRangeKeyName...).Write().Delete(keys...).Run()
	if err != nil {
		return goa.ErrInternal(err)
	}

	return nil
}
