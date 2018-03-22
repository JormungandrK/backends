package backends

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/JormungandrK/microservice-tools/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/guregu/dynamo"
	"github.com/satori/go.uuid"
)

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
		return nil, ErrBackendError("dynamo session not configured")
	}

	sessionAWS, ok := sessionObj.(*session.Session)
	if !ok {
		return nil, ErrBackendError("unknown session type")
	}

	databaseName := backend.GetConfig().DatabaseName
	if databaseName == "" {
		return nil, ErrBackendError("database name is missing and required")
	}

	tableName := repoDef.GetName()
	if tableName == "" {
		return nil, ErrBackendError("table name is missing and required")
	}

	svc := dynamodb.New(sessionAWS)
	err := createTable(svc, repoDef)
	if err != nil {
		return nil, err
	}

	err = setTTL(svc, repoDef)
	if err != nil {
		return nil, err
	}

	db := dynamo.New(sessionAWS)
	table := db.Table(tableName)

	fmt.Println("Table created")
	return &DynamoCollection{
		&table,
		repoDef,
	}, nil
}

// DynamoDBBackendBuilder returns RepositoriesBackend
func DynamoDBBackendBuilder(dbInfo *config.DBInfo, manager BackendManager) (Backend, error) {

	if dbInfo.AWSRegion == "" {
		return nil, ErrBackendError("AWS region is missing from config")
	}

	configAWS := &aws.Config{
		Region: aws.String(dbInfo.AWSRegion),
	}

	if dbInfo.AWSEndpoint != "" {
		configAWS.Endpoint = aws.String(dbInfo.AWSEndpoint)
		fmt.Println("With endpoint, no creds")
	} else if dbInfo.AWSCredentials != "" {
		configAWS.Credentials = credentials.NewSharedCredentials(dbInfo.AWSCredentials, "")
	} else {
		return nil, ErrBackendError("AWS credentials or endpoint must be specified in the config")
	}
	fmt.Println("***1")
	sess, err := session.NewSession(configAWS)
	if err != nil {
		fmt.Printf("Create session didnt work")
		return nil, err
	}

	ctx := context.WithValue(context.Background(), DYNAMO_CTX_KEY, sess)
	cleanup := func() {}

	return NewRepositoriesBackend(ctx, dbInfo, DynamoDBRepoBuilder, cleanup), nil

}

// createTable creates table if it does not exist
func createTable(svc *dynamodb.DynamoDB, repoDef RepositoryDefinition) error {

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
		return ErrBackendError(fmt.Sprintf("Hash key is missing for table %s", tableName))
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

	gsi := repoDef.GetGSI()
	if gsi != nil {
		for index, value := range gsi {

			var keySchemaGSI []*dynamodb.KeySchemaElement
			if index == hashKey {
				keySchemaGSI = append(keySchemaGSI, &dynamodb.KeySchemaElement{
					AttributeName: aws.String(index),
					KeyType:       aws.String("HASH"),
				})
			} else if index == rangeKey {
				keySchemaGSI = append(keySchemaGSI, &dynamodb.KeySchemaElement{
					AttributeName: aws.String(index),
					KeyType:       aws.String("RANGE"),
				})
			} else {
				return ErrBackendError("GSI must be hash or range key")
			}

			v := value.(map[string]interface{})
			globalSecondaryIndexes = append(globalSecondaryIndexes, &dynamodb.GlobalSecondaryIndex{
				IndexName: aws.String(fmt.Sprintf("%s-index", index)),
				KeySchema: keySchemaGSI,
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String("ALL"),
				},
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(int64(v["readCapacity"].(int))),
					WriteCapacityUnits: aws.Int64(int64(v["writeCapacity"].(int))),
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
	cto, err := svc.CreateTable(input)
	if err != nil {
		return err
	}

	fmt.Printf("Table created: %v\n", cto)

	return nil
}

// setTTL sets TimeToLive to the table
func setTTL(svc *dynamodb.DynamoDB, repoDef RepositoryDefinition) error {

	if repoDef.EnableTTL() {
		enabled := repoDef.EnableTTL()
		attribute := repoDef.GetTTLAttribute()
		tableName := repoDef.GetName()
		TTL := repoDef.GetTTL()

		if attribute == "" {
			return ErrBackendError("TTL attribute is reqired when TTL is enabled")
		}

		if TTL == 0 {
			return ErrBackendError("TTL value is missing and must be greater than zero")
		}

		err := svc.WaitUntilTableExists(&dynamodb.DescribeTableInput{
			TableName: &tableName,
		})
		if err != nil {
			return nil
		}

		svc.UpdateTimeToLive(&dynamodb.UpdateTimeToLiveInput{
			TableName: &tableName,
			TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
				AttributeName: &attribute,
				Enabled:       &enabled,
			},
		})
	}

	return nil
}

// GetOne looks up for an item by given filter
// Example filter:
//	filter := Filter{
// 		"id":    "54acb6c5-baeb-4213-b10f-e707a6055e64",
// }
func (c *DynamoCollection) GetOne(filter Filter, result interface{}) (interface{}, error) {

	var record map[string]interface{}
	var records []map[string]interface{}

	var query []string
	var args []interface{}
	for k, v := range filter {
		query = append(query, "$ = ?")
		args = append(args, k)
		args = append(args, v)
	}

	if c.RepositoryDefinition.EnableTTL() {
		query = append(query, "$ > ?")
		args = append(args, c.RepositoryDefinition.GetTTLAttribute())
		args = append(args, time.Now())
	}

	err := c.Table.Scan().Filter(strings.Join(query, " AND "), args...).Limit(int64(1)).All(&records)
	if err != nil {
		return nil, err
	}
	if records == nil {
		return nil, ErrNotFound("Record not found")
	}

	record = records[0]
	err = MapToInterface(&record, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetAll returns all matched records. You can specify limit and offset as well.
func (c *DynamoCollection) GetAll(filter Filter, resultsTypeHint interface{}, order string, sorting string, limit int, offset int) (interface{}, error) {
	var results reflect.Value

	resultHint := AsPtr(resultsTypeHint)

	results = NewSliceOfType(resultHint)
	fmt.Println("Results type: ", resultHint)

	var query []string
	var args []interface{}
	for k, v := range filter {
		query = append(query, "$ = ?")
		args = append(args, k)
		args = append(args, v)
	}

	if c.RepositoryDefinition.EnableTTL() {
		query = append(query, "$ > ?")
		args = append(args, c.RepositoryDefinition.GetTTLAttribute())
		args = append(args, time.Now())
	}

	startFrom := 1
	if offset != 0 {
		startFrom = offset + 1
	}

	itr := c.Table.Scan().Filter(strings.Join(query, " AND "), args...).SearchLimit(int64(startFrom)).Iter()
	for i := 0; ; i++ {
		record, err := CreateNewAsExample(resultHint)
		if err != nil {
			return nil, err
		}
		more := itr.Next(record)
		if itr.Err() != nil {
			return nil, itr.Err()
		}
		if !more {
			break
		}
		if limit != 0 && i >= limit {
			break
		}
		results = reflect.ValueOf(reflect.Append(results, reflect.ValueOf(record)).Interface())

		itr = c.Table.Scan().StartFrom(itr.LastEvaluatedKey()).SearchLimit(1).Iter()
	}

	return results.Interface(), nil
}

// Save creates new item or updates the existing one
func (c *DynamoCollection) Save(object interface{}, filter Filter) (interface{}, error) {

	var result interface{}

	payload, err := InterfaceToMap(object)
	if err != nil {
		return nil, err
	}

	hashKey := c.RepositoryDefinition.GetHashKey()
	rangeKey := c.RepositoryDefinition.GetRangeKey()

	if filter == nil {
		// Create item

		id, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}

		(*payload)["id"] = id.String()

		if c.RepositoryDefinition.EnableTTL() {
			attribute := c.RepositoryDefinition.GetTTLAttribute()
			TTL := c.RepositoryDefinition.GetTTL()

			(*payload)[attribute] = time.Now().Add(time.Second * time.Duration(TTL))
		}

		av, err := dynamodbattribute.MarshalMap(payload)
		if err != nil {
			return nil, err
		}

		err = c.Table.Put(av).If("attribute_not_exists($)", hashKey).Run()
		if err != nil {
			if IsConditionalCheckErr(err) {
				return nil, ErrAlreadyExists("record already exists!")
			}
			return nil, err
		}
	} else {
		// Update item

		var item interface{}
		_, err = c.GetOne(filter, &item)
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
			return nil, err
		}

		payload = &updatedItem
	}

	err = MapToInterface(payload, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// DeleteOne deletes only one item at the time
// Example filter:
//	filter := map[string]interface{}{
// 		"email": "keitaro-user1@keitaro.com",
// }
func (c *DynamoCollection) DeleteOne(filter Filter) error {

	hashKey := c.RepositoryDefinition.GetHashKey()
	rangeKey := c.RepositoryDefinition.GetRangeKey()

	var item interface{}
	_, err := c.GetOne(filter, &item)
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
			return ErrNotFound(err)
		}
		return err
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
func (c *DynamoCollection) DeleteAll(filter Filter) error {

	hashKey := c.RepositoryDefinition.GetHashKey()
	rangeKey := c.RepositoryDefinition.GetRangeKey()
	hashAndRangeKeyName := []string{hashKey}

	if rangeKey != "" {
		hashAndRangeKeyName = append(hashAndRangeKeyName, rangeKey)
	}

	hashValues, ok := filter[hashKey].([]string)
	if !ok {
		return ErrBackendError("hash key not specified in the filter")
	}

	rangeValues := []string{}
	if rangeKey != "" {
		rangeValues, ok = filter[rangeKey].([]string)
		if !ok {
			return ErrBackendError("range key not specified in the filter")
		}

		if len(hashValues) != len(rangeValues) {
			return ErrBackendError("length of the values for hash and range key in the filter must be equal")
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
		return err
	}

	return nil
}
