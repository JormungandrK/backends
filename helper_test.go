package backends

import (
	"fmt"
	"testing"
)

func TestInterfaceToMap(t *testing.T) {
	testMap := map[string]interface{}{
		"key": "val",
	}

	result, err := InterfaceToMap(&testMap)
	if err != nil {
		t.Errorf(err.Error())
	}

	if result == nil {
		t.Errorf("Nil map")
	}
}

func TestMapToInterface(t *testing.T) {
	testMap := map[string]interface{}{
		"key": "val",
	}

	var result interface{}
	err := MapToInterface(testMap, &result)
	if err != nil {
		t.Errorf(err.Error())
	}

	if result == nil {
		t.Errorf("Nil map")
	}
}

func TestStringToObjectID(t *testing.T) {
	testMap := map[string]interface{}{
		"id": "5975c461f9f8eb02aae053f3",
	}

	err := stringToObjectID(testMap)
	if err != nil {
		t.Errorf(err.Error())
	}

	if _, ok := testMap["_id"]; !ok {
		t.Errorf("ID not transformed")
	}
}

func TestIsConditionalCheckErr(t *testing.T) {
	ok := IsConditionalCheckErr(fmt.Errorf("Some error"))

	if ok {
		t.Errorf("Error is not ConditionalCheckFailedException")
	}
}

func TestContains(t *testing.T) {
	val := "value"
	arr := []*string{&val}

	ok := contains(arr, val)

	if !ok {
		t.Errorf("Expected array to contain the item 'value'")
	}
}
