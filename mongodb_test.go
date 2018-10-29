package backends

import (
	"reflect"
	"testing"

	"github.com/Microkubes/microservice-tools/config"
)

func TestToMongoPattern(t *testing.T) {
	pattern := toMongoPattern("not-changed")
	if pattern != "^not-changed$" {
		t.Fatal("Expected the pattern to be unchanged. Got: ", pattern)
	}

	pattern = toMongoPattern("in the %middle")
	if pattern != "^in the .*middle$" {
		t.Fatal("Expected the pattern to be in the middle. Got: ", pattern)
	}

	pattern = toMongoPattern("%at beginning")
	if pattern != ".*at beginning$" {
		t.Fatal("Expected the pattern to be at the beginning. Got: ", pattern)
	}

	pattern = toMongoPattern("at end%")
	if pattern != "^at end.*" {
		t.Fatal("Expected the pattern to be at the end. Got: ", pattern)
	}

	pattern = toMongoPattern("%start%middle and end%")
	if pattern != ".*start.*middle and end.*" {
		t.Fatal("Expected the pattern to be on multiple places. Got: ", pattern)
	}

	pattern = toMongoPattern("escape %% it")
	if pattern != "^escape % it$" {
		t.Fatal("Expected the pattern to escaped. Got: ", pattern)
	}

	pattern = toMongoPattern("triple %%%")
	if pattern != "^triple %.*" {
		t.Fatal("Expected the pattern to be at the end. Got: ", pattern)
	}

}

type TestEntry struct {
	ID    string `json:"id" bson:"id"`
	Value string `json:"value" bson:"value"`
}

func TestMongoDBIntergration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode.")
	}

	bm := NewBackendSupport(map[string]*config.DBInfo{
		"mongodb": &config.DBInfo{
			DatabaseName: "users",
			Host:         "localhost:27017",
			Username:     "restapi",
			Password:     "restapi",
		},
	})

	backend, err := bm.GetBackend("mongodb")
	if err != nil {
		t.Fatal(err)
	}

	repo, err := backend.DefineRepository("test_coll", RepositoryDefinitionMap{
		"name": "test_coll",
		"indexes": []Index{
			NewUniqueIndex("value"),
		},
	})

	defer func() {
		repo.DeleteAll(NewFilter().Match("value", "aa"))
		repo.DeleteAll(NewFilter().Match("value", "ab"))
		repo.DeleteAll(NewFilter().Match("value", "ba"))
	}()

	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range []TestEntry{
		TestEntry{
			Value: "aa",
		},
		TestEntry{
			Value: "ab",
		},
		TestEntry{
			Value: "ba",
		},
	} {
		if _, err := repo.Save(&entry, nil); err != nil {
			t.Fatal(err)
		}
	}

	results, err := repo.GetAll(NewFilter().MatchPattern("value", "a%"), &TestEntry{}, "value", "asc", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	resArr, ok := results.(*[]*TestEntry)
	if !ok {
		t.Fatal("Expected a pointer to an array of entries. Got type: ", reflect.TypeOf(results))
	}
	if len(*resArr) != 2 {
		t.Fatal("Expected 2 results, but got: ", len(*resArr))
	}

	// make sure we didn't break anything

	results, err = repo.GetAll(NewFilter().Match("value", "ba"), &TestEntry{}, "value", "asc", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	resArr, ok = results.(*[]*TestEntry)
	if !ok {
		t.Fatal("Expected a pointer to an array of entries. Got type: ", reflect.TypeOf(results))
	}
	if len(*resArr) != 1 {
		t.Fatal("Expected exactly 1 result, but got: ", len(*resArr))
	}
}
