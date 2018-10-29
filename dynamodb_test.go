package backends

import (
	"testing"
)

func TestTokenize(t *testing.T) {
	tokens := tokenize("abcde")
	if tokens == nil || len(tokens) != 1 {
		t.Fatal("expected one token")
	}
	if tokens[0] != "abcde" {
		t.Fatal("invalid tokens")
	}

	tokens = tokenize("%abcde")
	if tokens == nil || len(tokens) != 1 {
		t.Fatal("expected one token")
	}
	if tokens[0] != "abcde" {
		t.Fatal("invalid tokens. Got: ", tokens)
	}

	tokens = tokenize("abcde%")
	if tokens == nil || len(tokens) != 1 {
		t.Fatal("expected one token")
	}
	if tokens[0] != "abcde" {
		t.Fatal("invalid tokens. Got: ", tokens)
	}

	tokens = tokenize("%abcde%")
	if tokens == nil || len(tokens) != 1 {
		t.Fatal("expected one token")
	}
	if tokens[0] != "abcde" {
		t.Fatal("invalid tokens. Got: ", tokens)
	}

	tokens = tokenize("%%abcde")
	if tokens == nil || len(tokens) != 1 {
		t.Fatal("expected one token")
	}
	if tokens[0] != "%abcde" {
		t.Fatal("invalid tokens. Got: ", tokens)
	}

	tokens = tokenize("abcde%%")
	if tokens == nil || len(tokens) != 1 {
		t.Fatal("expected one token")
	}
	if tokens[0] != "abcde%" {
		t.Fatal("invalid tokens. Got: ", tokens)
	}

	tokens = tokenize("ab%de")
	if len(tokens) != 2 {
		t.Fatal("Expected 2 tokens. Got: ", len(tokens), tokens)
	}

	if !strArrEq(tokens, []string{"ab", "de"}) {
		t.Fatal("Invalid tokens. Got: ", tokens)
	}

	tokens = tokenize("ab%de%gh")
	if len(tokens) != 3 {
		t.Fatal("Expected 3 tokens. Got: ", len(tokens), tokens)
	}

	if !strArrEq(tokens, []string{"ab", "de", "gh"}) {
		t.Fatal("Invalid tokens. Got: ", tokens)
	}

}

func strArrEq(a, b []string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, av := range a {
		if av != b[i] {
			return false
		}
	}
	return true
}

func patternCondArrEqual(a, b []*patternCondition) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, av := range a {
		if !av.Equals(b[i]) {
			return false
		}
	}
	return true
}

func TestPatternToDynamoDBCondition(t *testing.T) {

	conds := patternToDynamodbCondition("abcd")
	if conds == nil || len(conds) != 1 {
		t.Fatal("Expected 1 condition to be parsed.")
	}
	if !conds[0].Equals(&patternCondition{
		condition: "EQ",
		value:     "abcd",
	}) {
		t.Fatal("Invalid condition. Got: ", conds[0])
	}

	conds = patternToDynamodbCondition("%abcd")
	if conds == nil || len(conds) != 1 {
		t.Fatal("Expected 1 condition to be parsed.")
	}
	if !conds[0].Equals(&patternCondition{
		condition: "CONTAINS",
		value:     "abcd",
	}) {
		t.Fatal("Invalid condition. Got: ", conds[0])
	}

	conds = patternToDynamodbCondition("%abcd%")
	if conds == nil || len(conds) != 1 {
		t.Fatal("Expected 1 condition to be parsed.")
	}
	if !conds[0].Equals(&patternCondition{
		condition: "CONTAINS",
		value:     "abcd",
	}) {
		t.Fatal("Invalid condition. Got: ", conds[0])
	}

	conds = patternToDynamodbCondition("abcd%")
	if conds == nil || len(conds) != 1 {
		t.Fatal("Expected 1 condition to be parsed.")
	}
	if !conds[0].Equals(&patternCondition{
		condition: "BEGINS_WITH",
		value:     "abcd",
	}) {
		t.Fatal("Invalid condition. Got: ", conds[0])
	}

	conds = patternToDynamodbCondition("%%abcd")
	if conds == nil || len(conds) != 1 {
		t.Fatal("Expected 1 condition to be parsed.")
	}
	if !conds[0].Equals(&patternCondition{
		condition: "EQ",
		value:     "%abcd",
	}) {
		t.Fatal("Invalid condition. Got: ", conds[0])
	}

	conds = patternToDynamodbCondition("%%abcd%%")
	if conds == nil || len(conds) != 1 {
		t.Fatal("Expected 1 condition to be parsed.")
	}
	if !conds[0].Equals(&patternCondition{
		condition: "EQ",
		value:     "%abcd%",
	}) {
		t.Fatal("Invalid condition. Got: ", conds[0])
	}

	conds = patternToDynamodbCondition("%%ab%cd%%")
	if conds == nil || len(conds) != 2 {
		t.Fatal("Expected 2 conditions to be parsed.")
	}
	if patternCondArrEqual(conds, []*patternCondition{
		&patternCondition{
			condition: "BEGINS_WITH",
			value:     "%ab",
		},
		&patternCondition{
			condition: "CONTAINS",
			value:     "cd%",
		},
	}) {
		t.Fatal("Invalid conditions. Got: ", conds)
	}
}
