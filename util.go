package gomongo

import (
	"fmt"
	"strings"
)

// ParseNamespace splits a namespace string into the database and collection.
// The first return value is the database, the second, the collection. An error
// is returned if either the database or the collection doesn't exist.
func ParseNamespace(namespace string) (string, string, error) {
	index := strings.Index(namespace, ".")
	if index < 0 || index >= len(namespace) {
		return "", "", fmt.Errorf("not a namespace")
	}
	database, collection := namespace[0:index], namespace[index+1:]

	// Error if empty database or collection
	if len(database) == 0 {
		return "", "", fmt.Errorf("empty database field")
	}

	if len(collection) == 0 {
		return "", "", fmt.Errorf("empty collection field")
	}

	return database, collection, nil
}
