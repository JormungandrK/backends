package backends

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

// interfaceToMap converts interface type (struct or map pointer) to *map[string]interface{}
func interfaceToMap(object interface{}) (*map[string]interface{}, error) {
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

// mapToInterface converts map to interface{} type
func mapToInterface(object interface{}) (interface{}, error) {
	var result interface{}
	jsonStruct, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}

	json.Unmarshal(jsonStruct, &result)

	return result, nil
}

// stringToObjectID convert string to bson.ObjectId
func stringToObjectID(object map[string]interface{}) {
	if id, ok := object["_id"]; ok {
		if reflect.TypeOf(id).String() != "bson.ObjectId" {
			object["_id"] = bson.ObjectIdHex(id.(string))
		}
	}
}
