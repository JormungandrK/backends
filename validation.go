package backends

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type ValidationResult struct {
	Valid  bool
	Errors []string
}

func ValidateBackend(backendProps map[string]interface{}, backendSchema map[string]interface{}) (*ValidationResult, error) {
	result := &ValidationResult{
		Valid:  true,
		Errors: []string{},
	}
	errorMessages, err := validateObject(backendProps, backendSchema)
	if err != nil {
		return nil, err
	}
	if errorMessages != nil && len(errorMessages) > 0 {
		result.Valid = false
		result.Errors = errorMessages
	}
	return result, nil
}

func validateObject(obj map[string]interface{}, objProperties map[string]interface{}) ([]string, error) {
	errors := []string{}
	for propName, def := range objProperties {
		propDef := def.(map[string]interface{})
		required := false
		if _, ok := propDef["required"]; ok {
			var castOk bool
			required, castOk = propDef["required"].(bool)
			if !castOk {
				return nil, fmt.Errorf("property 'required' in the object schema must be boolean")
			}
		}

		value, present := obj[propName]
		if required && !present {
			errors = append(errors, fmt.Sprintf("%s required", propName))
			continue
		}

		if !present || value == nil {
			continue
		}

		valueType := strings.ToLower(reflect.TypeOf(value).String())

		expectedType := safeGet(propDef, "type", "string").(string)
		switch expectedType {
		case "string":
			if valueType == "string" {
				continue
			}
			errors = append(errors, fmt.Sprintf("%s should be string, but instead is of type %s", propName, valueType))
		case "int", "integer":
			switch valueType {
			case "string":
				if _, err := strconv.ParseInt(value.(string), 10, 64); err != nil {
					errors = append(errors, fmt.Sprintf("%s is not valid integer", propName))
				}
			case "int", "int32", "int64":
				continue
			default:
				errors = append(errors, fmt.Sprintf("%s is expected to be integer, but instead is of type %s", propName, valueType))
			}
		case "float", "number":
			switch valueType {
			case "string":
				if _, err := strconv.ParseFloat(value.(string), 64); err != nil {
					errors = append(errors, fmt.Sprintf("%s is not valid integer", propName))
				}
			}
		case "bool", "boolean":
			switch valueType {
			case "boolean", "bool":
				continue
			case "string":
				if _, err := strconv.ParseBool(value.(string)); err != nil {
					errors = append(errors, fmt.Sprintf("%s is not boolean: %s", propName, valueType))
				}
			default:
				errors = append(errors, fmt.Sprintf("%s is not boolean: %s", propName, valueType))
			}
		case "array":
			// iterate array with reflection
			kind := reflect.TypeOf(value).Kind()

			if kind == reflect.Slice || kind == reflect.Array {
				val := reflect.ValueOf(value)

				elemDef := map[string]interface{}{
					"type": safeGet(propDef, "elementType", "string"),
				}

				if _, ok := propDef["elementProperties"]; ok {
					elemDef["properties"] = propDef["elementProperties"]
				}

				for i := 0; i < val.Len(); i++ {
					element := val.Index(i)
					key := fmt.Sprintf("%s[%d]", propName, i)
					errorList, err := validateObject(map[string]interface{}{
						key: element.Interface(),
					}, map[string]interface{}{
						key: elemDef,
					})
					if err != nil {
						return nil, err
					}
					if errorList != nil {
						errors = append(errors, errorList...)
					}
				}
			} else {
				errors = append(errors, fmt.Sprintf("%s expected to be an array, but it is %s instead", propName, valueType))
			}
		case "map":
			// iterate map with reflection
			if reflect.TypeOf(value).Kind() != reflect.Map {
				errors = append(errors, fmt.Sprintf("%s expected to be a map, but instead got %s", propName, valueType))
				continue
			}
			val := reflect.ValueOf(value)

			for _, key := range val.MapKeys() {

				// 1. validate key
				validationKey := fmt.Sprintf("%s<key<%s>>", propName, key)
				keyDef := safeGet(propDef, "key", map[string]interface{}{"type": "string"})
				errorList, err := validateObject(map[string]interface{}{
					validationKey: key.Interface(),
				}, map[string]interface{}{
					validationKey: keyDef,
				})
				if err != nil {
					return nil, err
				}
				if errorList != nil && len(errorList) > 0 {
					errors = append(errors, errorList...)
				}

				// 2. validate value
				validationKey = fmt.Sprintf("%s:<%s>", propName, key)
				elemValue := val.MapIndex(key)
				elemDef := safeGet(propDef, "value", map[string]interface{}{"type": "any"})
				errorList, err = validateObject(map[string]interface{}{
					validationKey: elemValue.Interface(),
				}, map[string]interface{}{
					validationKey: elemDef,
				})
				if err != nil {
					return nil, err
				}
				if errorList != nil && len(errorList) > 0 {
					errors = append(errors, errorList...)
				}
			}
		case "object":
			//recursive
			objectDef := safeGet(propDef, "properties", map[string]interface{}{})
			objectDefMap, ok := objectDef.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("%s object definition must be a map[string]interface{}", propName)
			}
			object, ok := value.(map[string]interface{})
			if !ok {
				errors = append(errors, fmt.Sprintf("%s was expected to be an object (map[string]interface{}), but instead got %s", propName, valueType))
				continue
			}
			errorList, err := validateObject(object, objectDefMap)
			if err != nil {
				return nil, err
			}
			if errorList != nil && len(errorList) > 0 {
				errors = append(errors, errorList...)
			}
		case "any":
			// no checks here
			continue
		default:
			return nil, fmt.Errorf("%s was expected to be of type %s, which cannot be validated", propName, expectedType)
		}

	}
	return errors, nil
}

func safeGet(m map[string]interface{}, key string, defaultValue interface{}) interface{} {
	v, ok := m[key]
	if !ok {
		return defaultValue
	}
	if v == nil {
		return defaultValue
	}
	return v
}
