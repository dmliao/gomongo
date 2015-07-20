package main

import (
	"fmt"
	"github.com/dmliao/gomongo"
	"gopkg.in/mgo.v2/bson"
)

func main() {
	conn, err := gomongo.Connect("localhost")
	if err != nil {
		fmt.Printf("Error: %v", err)
	}
	fmt.Printf("%#v", conn)
	opts := &gomongo.FindOpts{
		BatchSize: 2,
	}
	cursor, err := conn.Find("test.foo", bson.M{}, opts)
	if err != nil {
		fmt.Printf("Error: %v", err)
	}

	var result bson.M
	for cursor.HasNext() {
		err = cursor.Next(&result)
		fmt.Printf("%#v\n", result)
	}
	err = conn.Run("admin", bson.M{"isMaster": 1}, &result)
	fmt.Printf("%#v\n", result)

	err = conn.Insert("test.driver", bson.M{"price": 5})
	cursor2, err := conn.Find("test.driver", bson.M{}, nil)
	for cursor2.HasNext() {
		err = cursor2.Next(&result)
		fmt.Printf("%#v\n", result)
	}
}
