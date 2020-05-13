package gomongofixtures

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var uri = func() string {
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}

	return uri
}

const (
	testDB  = "test_gomongofixture"
	testCol = "data"
)

type elem struct {
	ID   primitive.ObjectID
	I    int       `bson:"i"`
	Time time.Time `bson:"time"`
}

func TestLoad(t *testing.T) {
	ctx := context.Background()

	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(uri()))
	if err != nil {
		t.Error(err)
	}

	col := cli.Database(testDB).Collection(testCol)

	defer func() {
		if _, err := col.DeleteMany(ctx, bson.M{}); err != nil {
			t.Error(err)
		}
	}()

	err = Load(ctx, uri(), Fixture{
		DB:   testDB,
		Path: "testdata/data.json",
	})
	if err != nil {
		t.Error(err)
	}

	cur, err := col.Find(ctx, bson.M{}, &options.FindOptions{
		Sort: bson.M{
			"i": 1,
		},
	})
	if err != nil {
		t.Error(err)
	}

	var elems []elem

	for cur.Next(ctx) {
		var e elem
		if err := cur.Decode(&e); err != nil {
			t.Error(err)
		}

		elems = append(elems, e)
	}

	if len(elems) != 10 {
		t.Error("Invalid count in the database")
	}

	for i, e := range elems {
		if e.I != i {
			t.Error("Invalid number")
		}
		if e.Time.IsZero() {
			t.Error("Time is zero")
		}
	}
}
