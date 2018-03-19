package backends

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/mgo.v2/bson"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

// InterfaceToMap converts interface type (struct or map pointer) to *map[string]interface{}
func InterfaceToMap(object interface{}) (*map[string]interface{}, error) {
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

// MapToInterface decodes object to result
func MapToInterface(object interface{}, result interface{}) error {

	jsonStruct, err := json.Marshal(object)
	if err != nil {
		return err
	}

	json.Unmarshal(jsonStruct, result)

	return nil
}

// IterateOverSlice iterates over a slice viewed as generic itnerface{}. A callback function is called for
// every item in the slice. If the callback returns an error, the iteration will break and the function will
// return that error.
func IterateOverSlice(slice interface{}, callback func(i int, item interface{}) error) error {
	if slice == nil {
		return nil
	}

	st := reflect.TypeOf(slice)
	if st.Kind() == reflect.Ptr {
		st = st.Elem()
	}
	if st.Kind() != reflect.Slice {
		return fmt.Errorf("not slice")
	}

	stVal := reflect.ValueOf(slice)
	for i := 0; i < stVal.Len(); i++ {
		item := stVal.Index(i)
		err := callback(i, item)
		if err != nil {
			return err
		}
	}

	return nil
}

// stringToObjectID converts _id key from string to bson.ObjectId
func stringToObjectID(object map[string]interface{}) error {
	if id, ok := object["id"]; ok {
		delete(object, "id")
		if !bson.IsObjectIdHex(id.(string)) {
			return fmt.Errorf("id is a invalid hex representation of an ObjectId")
		}

		if reflect.TypeOf(id).String() != "bson.ObjectId" {
			object["_id"] = bson.ObjectIdHex(id.(string))
		}
	}

	return nil
}

// IsConditionalCheckErr check if err is dynamoDB condition error
func IsConditionalCheckErr(err error) bool {
	if ae, ok := err.(awserr.RequestFailure); ok {
		return ae.Code() == "ConditionalCheckFailedException"
	}
	return false
}

// contains checks if item is in s array
func contains(s []*string, item string) bool {
	for _, a := range s {
		if *a == item {
			return true
		}
	}
	return false
}

// CreateNewAsExample creates a new value of the same type as the "example" passed to the function.
// The function always returns a pointer to the created value.
func CreateNewAsExample(example interface{}) (interface{}, error) {
	exampleType := reflect.TypeOf(example)
	if exampleType.Kind() == reflect.Ptr {
		fmt.Println("Already a ptr")
		exampleType = exampleType.Elem()
		fmt.Println("    => but now: ", exampleType.Kind())
	}

	value, err := createNewFromType(exampleType)
	if err != nil {
		return nil, err
	}
	return value.Interface(), nil
}

func createNewFromType(valueType reflect.Type) (reflect.Value, error) {
	switch kind := valueType.Kind(); kind {
	case reflect.Map:
		return valueOrError(reflect.New(valueType))
	case reflect.Slice:
		sliceVal := reflect.ValueOf(valueType)
		slen := 0
		scap := 0
		if !sliceVal.IsValid() {
			slen = sliceVal.Len()
			scap = sliceVal.Cap()
		}
		return valueOrError(reflect.MakeSlice(valueType, slen, scap))
	default:
		return valueOrError(reflect.New(valueType))
	}
}

// AsPtr returns a pointer to the value passed as an argument to this function.
// If the value is already a pointer to a value, the pointer passed is returned back
// (no new pointer is created).
func AsPtr(val interface{}) interface{} {
	valType := reflect.TypeOf(val)
	if valType.Kind() == reflect.Ptr {
		return val
	}

	return reflect.New(valType).Interface()
}

// NewSliceOfType creates new slice with len 0 and cap 0 with elements of
// the type passed as an example to the function.
func NewSliceOfType(elementTypeHint interface{}) reflect.Value {
	elemType := reflect.TypeOf(elementTypeHint)
	return reflect.MakeSlice(reflect.SliceOf(elemType), 0, 0)
}

func valueOrError(val reflect.Value) (reflect.Value, error) {
	if !val.IsValid() {
		return val, fmt.Errorf("invalid value")
	}
	return val, nil
}
