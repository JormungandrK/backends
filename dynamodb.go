package backends

import (
	"encoding/json"
	"reflect"
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

		usersTable := db.Table("users")

		collections := map[string]Repository{
			"users":  &DynamoCollection{&usersTable},
			"tokens": &DynamoCollection{&usersTable},
		}

		return collections, nil
	}

	AddBackendType("dynamodb", builder)
}

// DynamoCollection wraps a dynamo.Table to embed methods in models.
type DynamoCollection struct {
	*dynamo.Table
}

func (c *DynamoCollection) GetOne(filter map[string]interface{}, result interface{}) error {
	return nil
}

func (c *DynamoCollection) GetAll(filter map[string]interface{}, results interface{}, order string, sorting string, limit int, offset int) error {
	return nil
}

func (c *DynamoCollection) Save(object interface{}, filter map[string]interface{}) (interface{}, error) {

	if filter == nil {
		payload := &map[string]interface{}{}

		rValue := reflect.ValueOf(object).Elem()
		rKind := rValue.Kind()

		switch rKind {

		case reflect.Struct:
			typeOfObject := rValue.Type()

			for i := 0; i < rValue.NumField(); i++ {
				f := rValue.Field(i)
				key := strings.ToLower(typeOfObject.Field(i).Name)
				value := f.Interface()
				(*payload)[key] = value
			}
		case reflect.Map:

			if _, ok := object.(*map[string]interface{}); ok {
				payload = object.(*map[string]interface{})
			} else {
				return nil, goa.ErrInternal("invalid map type, should be *map[string]interface{}")
			}
		default:

			return nil, goa.ErrInternal("invalid object type, it should be struct pointer or *map[string]interface{}")
		}

		id := uuid.NewV4().String()
		(*payload)["id"] = id

		av, err := dynamodbattribute.MarshalMap(payload)
		if err != nil {
			return nil, goa.ErrInternal(err)
		}

		// Putting an item
		err = c.Put(av).Run()
		if err != nil {
			return nil, goa.ErrInternal(err)
		}

		var result interface{}
		jsonStruct, err := json.Marshal(payload)
		if err != nil {
			return nil, goa.ErrInternal(err)
		}

		json.Unmarshal(jsonStruct, &result)

		return result, nil
	}

	return nil, nil
}

func (c *DynamoCollection) DeleteOne(filter map[string]interface{}) error {
	return nil
}

func (c *DynamoCollection) DeleteAll(filter map[string]interface{}) error {
	return nil
}
