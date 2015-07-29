package main

import (
	"fmt"
	"github.com/dmliao/gomongo"
	"gopkg.in/mgo.v2/bson"
)

func main() {
	mongo, err := gomongo.Connect("localhost")
	if err != nil {
		fmt.Printf("Error connecting to mongoDB: %v", err)
		return
	}
	fmt.Printf("%#v", mongo)
	opts := &gomongo.FindOpts{
		BatchSize: 2,
	}

	cursor, err := mongo.GetDB("test").GetCollection("foo").Find(bson.M{}, opts)

	if err != nil {
		fmt.Printf("Error: %v", err)
		return
	}

	var result bson.M
	for cursor.HasNext() {
		err = cursor.Next(&result)
		fmt.Printf("%#v\n", result)
	}

	err = mongo.GetDB("admin").ExecuteCommand(bson.M{"isMaster": 1}, &result)
	fmt.Printf("%#v\n", result)

	c := mongo.GetDB("test").GetCollection("driver")

	err = c.Insert(bson.M{"price": 5})
	err = c.Update(bson.M{"price": 5}, bson.M{"price": 15}, nil)
	err = c.Remove(bson.M{"price": 15}, nil)
	cursor2, err := c.Find(bson.M{}, nil)
	for cursor2.HasNext() {
		err = cursor2.Next(&result)
		fmt.Printf("%#v\n", result)
	}

	mongo.Close()
}
