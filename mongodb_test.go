package backends

import "testing"

func TestToMongoPattern(t *testing.T) {
	pattern := toMongoPattern("not-changed")
	if pattern != "not-changed" {
		t.Fatal("Expected the pattern to be unchanged. Got: ", pattern)
	}

	pattern = toMongoPattern("in the %middle")
	if pattern != "in the .*middle" {
		t.Fatal("Expected the pattern to be in the middle. Got: ", pattern)
	}

	pattern = toMongoPattern("%at beginning")
	if pattern != ".*at beginning" {
		t.Fatal("Expected the pattern to be at the beginning. Got: ", pattern)
	}

	pattern = toMongoPattern("at end%")
	if pattern != "at end.*" {
		t.Fatal("Expected the pattern to be at the end. Got: ", pattern)
	}

	pattern = toMongoPattern("%start%middle and end%")
	if pattern != ".*start.*middle and end.*" {
		t.Fatal("Expected the pattern to be on multiple places. Got: ", pattern)
	}

	pattern = toMongoPattern("escape %% it")
	if pattern != "escape % it" {
		t.Fatal("Expected the pattern to escaped. Got: ", pattern)
	}

	pattern = toMongoPattern("triple %%%")
	if pattern != "triple %.*" {
		t.Fatal("Expected the pattern to be at the end. Got: ", pattern)
	}

}
