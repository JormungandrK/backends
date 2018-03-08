package backends

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/mgo.v2/bson"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

// interfaceToMap converts interface type (struct or map pointer) to *map[string]interface{}
func interfaceToMap(object interface{}) (*map[string]interface{}, error) {
	if reflect.ValueOf(object).Kind() != reflect.Ptr {
		return nil, fmt.Errorf("object should be of pointer type")
	}

	result := &map[string]interface{}{}
	rValue := reflect.ValueOf(object).Elem()
	rKind := rValue.Kind()

	switch rKind {

	case reflect.Struct:
		typeOfObject := rValue.Type()

		for i := 0; i < rValue.NumField(); i++ {
			f := rValue.Field(i)
			key := strings.ToLower(typeOfObject.Field(i).Name)
			value := f.Interface()
			(*result)[key] = value
		}
	case reflect.Map:

		if _, ok := object.(*map[string]interface{}); ok {
			result = object.(*map[string]interface{})
		} else {
			return nil, fmt.Errorf("invalid map type, should be *map[string]interface{}")
		}
	default:

		return nil, fmt.Errorf("invalid object type, it should be struct pointer or *map[string]interface{}")
	}

	return result, nil
}

// MapToInterface converts map to interface{} type
func MapToInterface(object interface{}, result interface{}) error {

	jsonStruct, err := json.Marshal(object)
	if err != nil {
		return err
	}

	json.Unmarshal(jsonStruct, result)

	return nil
}

// stringToObjectID converts _id key from string to bson.ObjectId
func stringToObjectID(object map[string]interface{}) {
	if id, ok := object["id"]; ok {
		delete(object, "id")
		if reflect.TypeOf(id).String() != "bson.ObjectId" {
			object["_id"] = bson.ObjectIdHex(id.(string))
		}
	}
}

// IsConditionalCheckErr check if err is dynamoDB condition error
func IsConditionalCheckErr(err error) bool {
	if ae, ok := err.(awserr.RequestFailure); ok {
		return ae.Code() == "ConditionalCheckFailedException"
	}
	return false
}

// contains checks if
func contains(s []*string, item string) bool {
	for _, a := range s {
		if *a == item {
			return true
		}
	}
	return false
}
