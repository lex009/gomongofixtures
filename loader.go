// Package gomongofixture contains methods for loading MongoDB data from files
// generated by mongoexport utility into your database for testing reasons or whatever reason you'd like.
//
// Usage:
// 	if err := Load(context.Background(), "localhost:27017", Fixture{
// 		DB:   "db",
// 		Path: "data.json",
// 	}); err != nil {
// 		panic(err)
// 	}
package gomongofixture

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mongodb/mongo-tools-common/bsonutil"
	"github.com/mongodb/mongo-tools-common/json"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"
)

// Loader loads fixtures into a database.
type Loader struct {
	URI  string // URI of MongoDB.
	Path string // Path to fixture file.
	DB   string // Database name to load fixture.
	Col  string // Collection to load fixture.
}

// Load connects to the database and loads the fixture.
func (l *Loader) Load(ctx context.Context) error {
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(l.URI))
	if err != nil {
		return err
	}

	f, err := os.Open(l.Path)
	if err != nil {
		return err
	}

	d := json.NewDecoder(f)
	for {
		obj, err := d.ScanObject()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		doc, err := json.UnmarshalBsonD(obj)
		if err != nil {
			return err
		}

		bsonD, err := bsonutil.GetExtendedBsonD(doc)
		if err != nil {
			panic(err)
		}

		var newD primitive.D
		for _, e := range bsonD {
			pe := primitive.E{
				Key:   e.Name,
				Value: e.Value,
			}

			if v, ok := e.Value.(bson.ObjectId); ok {
				id, err := primitive.ObjectIDFromHex(v.Hex())
				if err != nil {
					panic(err)
				}

				pe.Value = id
			}
			newD = append(newD, pe)
		}

		if _, err := cli.Database(l.DB).Collection(l.Col).InsertOne(ctx, newD); err != nil {
			return err
		}
	}

	return nil
}

// Fixture describes a fixture: path to a file, database and collection the fixture
// should be loaded in.
type Fixture struct {
	Path string
	DB   string
}

// Load loads the given fixture into a database with the given URI using Loader.
// Each time it creates new Loader and new connection to the database.
// It uses file name in the path of the fixture as a collection name.
func Load(ctx context.Context, uri string, f Fixture) error {
	col := strings.TrimSuffix(filepath.Base(f.Path), filepath.Ext(f.Path))

	l := Loader{
		Path: f.Path,
		DB:   f.DB,
		Col:  col,
	}

	return l.Load(ctx)
}