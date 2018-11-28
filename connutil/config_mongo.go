package connutil

import (
	"log"
	"os"
	"strconv"

	"github.com/TerrexTech/agg-shipment-cmd/model"
	"github.com/TerrexTech/go-agg-builder/builder"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

// LoadMongoConfig loads Mongo collection from using params set in env.
func LoadMongoConfig() (*builder.MongoConfig, error) {
	hosts := *commonutil.ParseHosts(
		os.Getenv("MONGO_HOSTS"),
	)

	database := os.Getenv("MONGO_DATABASE")
	aggCollection := os.Getenv("MONGO_AGG_COLLECTION")
	metaCollection := os.Getenv("MONGO_META_COLLECTION")

	connTimeoutStr := os.Getenv("MONGO_CONNECTION_TIMEOUT_MS")
	connTimeout, err := strconv.Atoi(connTimeoutStr)
	if err != nil {
		err = errors.Wrap(err, "Error converting MONGO_CONNECTION_TIMEOUT_MS to integer")
		log.Println(err)
		log.Println("A defalt value of 3000 will be used for MONGO_CONNECTION_TIMEOUT_MS")
		connTimeout = 3000
	}

	mongoConfig := mongo.ClientConfig{
		Hosts:               hosts,
		Username:            os.Getenv("MONGO_USERNAME"),
		Password:            os.Getenv("MONGO_PASSWORD"),
		TimeoutMilliseconds: uint32(connTimeout),
	}

	// MongoDB Client
	client, err := mongo.NewClient(mongoConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating MongoClient")
		log.Fatalln(err)
	}

	resTimeoutStr := os.Getenv("MONGO_CONNECTION_TIMEOUT_MS")
	resTimeout, err := strconv.Atoi(resTimeoutStr)
	if err != nil {
		err = errors.Wrap(err, "Error converting MONGO_RESOURCE_TIMEOUT_MS to integer")
		log.Println(err)
		log.Println("A defalt value of 5000 will be used for MONGO_RESOURCE_TIMEOUT_MS")
		resTimeout = 5000
	}
	conn := &mongo.ConnectionConfig{
		Client:  client,
		Timeout: uint32(resTimeout),
	}

	aggMongoCollection, err := createMongoCollection(conn, database, aggCollection)
	if err != nil {
		err = errors.Wrap(err, "Error creating MongoCollection")
		return nil, err
	}

	return &builder.MongoConfig{
		AggregateID:        model.AggregateID,
		AggCollection:      aggMongoCollection,
		Connection:         conn,
		MetaDatabaseName:   database,
		MetaCollectionName: metaCollection,
	}, nil
}

func createMongoCollection(
	conn *mongo.ConnectionConfig, db string, coll string,
) (*mongo.Collection, error) {
	// Index Configuration
	indexConfigs := []mongo.IndexConfig{
		mongo.IndexConfig{
			ColumnConfig: []mongo.IndexColumnConfig{
				mongo.IndexColumnConfig{
					Name: "itemID",
				},
			},
			IsUnique: true,
			Name:     "itemID_index",
		},
		mongo.IndexConfig{
			ColumnConfig: []mongo.IndexColumnConfig{
				mongo.IndexColumnConfig{
					Name:        "timestamp",
					IsDescOrder: true,
				},
			},
			Name: "timestamp_index",
		},
	}

	// Create New Collection
	collection, err := mongo.EnsureCollection(&mongo.Collection{
		Connection:   conn,
		Database:     db,
		Name:         coll,
		SchemaStruct: &model.Item{},
		Indexes:      indexConfigs,
	})
	if err != nil {
		err = errors.Wrap(err, "Error creating MongoCollection")
		return nil, err
	}
	return collection, nil
}
