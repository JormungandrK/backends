package backends

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/JormungandrK/microservice-tools/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/goadesign/goa"
	"github.com/guregu/dynamo"
	"github.com/satori/go.uuid"
)

// mutexDynamo is an exclusion lock
var mutexDynamo = &sync.Mutex{}

// DYNAMO_CTX_KEY is dynamoDB context key
var DYNAMO_CTX_KEY = "DYNAMO_SESSION"

// DynamoCollection wraps a dynamo.Table to embed methods in models.
type DynamoCollection struct {
	*dynamo.Table
	RepositoryDefinition
}

// DynamoDBRepoBuilder builds new dynamo table.
// If it does not exist builder will create it
func DynamoDBRepoBuilder(repoDef RepositoryDefinition, backend Backend) (Repository, error) {

	sessionObj := backend.GetFromContext(DYNAMO_CTX_KEY)
	if sessionObj == nil {
		return nil, fmt.Errorf("dynamo session not configured")
	}

	sessionAWS, ok := sessionObj.(*session.Session)
	if !ok {
		return nil, fmt.Errorf("unknown session type")
	}

	databaseName := backend.GetConfig().DatabaseName
	if databaseName == "" {
		return nil, fmt.Errorf("database name is missing and required")
	}

	tableName := repoDef.GetName()
	if tableName == "" {
		return nil, fmt.Errorf("table name is missing and required")
	}

	err := createTable(sessionAWS, repoDef)
	if err != nil {
		return nil, err
	}

	db := dynamo.New(sessionAWS)
	table := db.Table(tableName)

	return &DynamoCollection{
		&table,
		repoDef,
	}, nil
}

// DynamoDBBackendBuilder returns RepositoriesBackend
func DynamoDBBackendBuilder(dbInfo *config.DBInfo, manager BackendManager) (Backend, error) {

	if dbInfo.AWSRegion == "" {
		return nil, fmt.Errorf("AWS region is missing from config")
	}

	configAWS := &aws.Config{
		Region: aws.String(dbInfo.AWSRegion),
	}

	if dbInfo.AWSEndpoint != "" {
		configAWS.Endpoint = aws.String(dbInfo.AWSEndpoint)
	} else if dbInfo.AWSCredentials != "" {
		configAWS.Credentials = credentials.NewSharedCredentials(dbInfo.AWSCredentials, "")
	} else {
		return nil, fmt.Errorf("AWS credentials or endpoint must be specified in the config")
	}

	sess, err := session.NewSession(configAWS)
	if err != nil {
		return nil, err
	}

	ctx := context.WithValue(context.Background(), DYNAMO_CTX_KEY, sess)
	cleanup := func() {}

	return NewRepositoriesBackend(ctx, dbInfo, DynamoDBRepoBuilder, cleanup), nil

}

// createTable creates table if it does not exist
func createTable(sessionAWS *session.Session, repoDef RepositoryDefinition) error {

	mutexDynamo.Lock()
	defer mutexDynamo.Unlock()

	svc := dynamodb.New(sessionAWS)

	result, err := svc.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		return err
	}

	var attributes []*dynamodb.AttributeDefinition
	var keySchemaElements []*dynamodb.KeySchemaElement
	var globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndex

	tableName := repoDef.GetName()
	tableNames := result.TableNames
	hashKey := repoDef.GetHashKey()
	rangeKey := repoDef.GetRangeKey()

	if contains(tableNames, tableName) {
		return nil
	}

	if hashKey != "" {
		attributes = append(attributes, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(hashKey),
			AttributeType: aws.String("S"),
		})

		keySchemaElements = append(keySchemaElements, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(hashKey),
			KeyType:       aws.String("HASH"),
		})

	} else {
		return goa.ErrInternal(fmt.Sprintf("Hash key is missing for table %s", tableName))
	}

	if rangeKey != "" {
		attributes = append(attributes, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(rangeKey),
			AttributeType: aws.String("S"),
		})

		keySchemaElements = append(keySchemaElements, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(rangeKey),
			KeyType:       aws.String("RANGE"),
		})
	}

	if len(repoDef.GetIndexes()) > 0 {
		for _, index := range repoDef.GetIndexes() {
			globalSecondaryIndexes = append(globalSecondaryIndexes, &dynamodb.GlobalSecondaryIndex{
				IndexName: aws.String(fmt.Sprintf("%s-index", index)),
				KeySchema: []*dynamodb.KeySchemaElement{{
					AttributeName: aws.String(index),
					KeyType:       aws.String("HASH"),
				}},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String("ALL"),
				},
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(2),
					WriteCapacityUnits: aws.Int64(2),
				},
			})
		}
	}

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions:   attributes,
		KeySchema:              keySchemaElements,
		GlobalSecondaryIndexes: globalSecondaryIndexes,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(repoDef.GetReadCapacity()),
			WriteCapacityUnits: aws.Int64(repoDef.GetWriteCapacity()),
		},
		TableName: aws.String(tableName),
	}

	// Create the table
	_, err = svc.CreateTable(input)
	if err != nil {
		return err
	}

	// Wait until table is created
	tableNotCreated := true
	for tableNotCreated {
		result, err := svc.ListTables(&dynamodb.ListTablesInput{})
		if err != nil {
			return err
		}

		tables := result.TableNames

		if contains(tables, tableName) {
			tableNotCreated = false
		}
	}

	// Set TimeToLive attribute
	if repoDef.EnableTTL() {
		enabled := repoDef.EnableTTL()
		attribute := repoDef.GetTTLAttribute()
		TTL := repoDef.GetTTL()

		if attribute == "" {
			return fmt.Errorf("TTL attribute is reqired when TTL is enabled")
		}

		if TTL == 0 {
			return fmt.Errorf("TTL value is missing and must be greater than zero")
		}

		_, err = svc.UpdateTimeToLive(&dynamodb.UpdateTimeToLiveInput{
			TableName: &tableName,
			TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
				AttributeName: &attribute,
				Enabled:       &enabled,
			},
		})

		if err != nil {
			return err
		}
	}

	return nil
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

	err := c.Table.Scan().Filter(strings.Join(query, " AND "), args...).Consistent(true).All(&records)
	if err != nil {
		return goa.ErrInternal(err)
	}
	if records == nil {
		return goa.ErrNotFound("not found")
	}

	record = records[0]
	err = MapToInterface(&record, &result)
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

	err := c.Table.Scan().Filter(strings.Join(query, " AND "), args...).All(&records)
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

	err = MapToInterface(&records, &results)
	if err != nil {
		return goa.ErrInternal(err)
	}

	return nil

}

// Save creates new item or updates the existing one
func (c *DynamoCollection) Save(object interface{}, filter map[string]interface{}) (interface{}, error) {

	var result interface{}

	payload, err := InterfaceToMap(object)
	if err != nil {
		return nil, goa.ErrInternal(err)
	}

	hashKey := c.RepositoryDefinition.GetHashKey()
	rangeKey := c.RepositoryDefinition.GetRangeKey()

	if filter == nil {
		// Create item

		id, err := uuid.NewV4()
		if err != nil {
			return nil, goa.ErrInternal(err)
		}

		(*payload)["id"] = id.String()
		av, err := dynamodbattribute.MarshalMap(payload)
		if err != nil {
			return nil, goa.ErrInternal(err)
		}

		if c.RepositoryDefinition.EnableTTL() {
			attribute := c.RepositoryDefinition.GetTTLAttribute()
			// TTL := c.RepositoryDefinition.GetTTL()
			(*payload)[attribute] = time.Now().Add(time.Second * 10)
		}

		err = c.Table.Put(av).If("attribute_not_exists($)", hashKey).Run()
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

		query := c.Table.Update(hashKey, res[hashKey])
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

	err = MapToInterface(payload, &result)
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

	hashKey := c.RepositoryDefinition.GetHashKey()
	rangeKey := c.RepositoryDefinition.GetRangeKey()

	var item interface{}
	err := c.GetOne(filter, &item)
	if err != nil {
		return err
	}
	result := item.(map[string]interface{})

	query := c.Table.Delete(hashKey, result[hashKey])

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

	hashKey := c.RepositoryDefinition.GetHashKey()
	rangeKey := c.RepositoryDefinition.GetRangeKey()
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

	_, err := c.Table.Batch(hashAndRangeKeyName...).Write().Delete(keys...).Run()
	if err != nil {
		return goa.ErrInternal(err)
	}

	return nil
}
