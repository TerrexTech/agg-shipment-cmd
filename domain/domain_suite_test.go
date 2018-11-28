package domain

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"

	"github.com/TerrexTech/agg-shipment-cmd/connutil"
	"github.com/TerrexTech/agg-shipment-cmd/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestEvent tests Event-handling.
func TestEvent(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"SERVICE_NAME",

		"KAFKA_BROKERS",

		"KAFKA_CONSUMER_GROUP_REQUEST",
		"KAFKA_CONSUMER_TOPIC_REQUEST",

		"KAFKA_CONSUMER_GROUP_ESRESP",
		"KAFKA_CONSUMER_TOPIC_ESRESP",

		"KAFKA_PRODUCER_TOPIC_ESREQ",
		"KAFKA_PRODUCER_TOPIC_EVENTS",

		"KAFKA_END_OF_STREAM_TOKEN",

		"MONGO_HOSTS",
		"MONGO_DATABASE",
		"MONGO_AGG_COLLECTION",
		"MONGO_META_COLLECTION",

		"MONGO_CONNECTION_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "EventHandler Suite")
}

var _ = Describe("EventHandler", func() {
	var (
		coll *mongo.Collection
	)

	BeforeSuite(func() {
		mc, err := connutil.LoadMongoConfig()
		Expect(err).ToNot(HaveOccurred())
		coll = mc.AggCollection
	})

	Describe("ItemDeleted", func() {
		It("should delete item", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID: itemID.String(),
				Lot:    itemID.String(),
			}
			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			marshalItem, err := json.Marshal(mockItem)
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &cmodel.Event{
				Action:        "ItemDeleted",
				AggregateID:   1,
				CorrelationID: cid,
				Data:          marshalItem,
				NanoTime:      time.Now().UTC().UnixNano(),
				Source:        "test-source",
				UserUUID:      uid,
				UUID:          uuid,
				Version:       1,
				YearBucket:    2018,
			}

			err = itemDeleted(coll, mockEvent)
			Expect(err).ToNot(HaveOccurred())

			_, err = coll.FindOne(mockItem)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("ItemRegistered", func() {
		It("should delete item", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			custID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID:       itemID.String(),
				DateArrived:  time.Now().UTC().Unix(),
				Lot:          "test-lot",
				Name:         "test-name",
				Origin:       "test-origin",
				Price:        12.3,
				RSCustomerID: custID.String(),
				SKU:          "test-sku",
				Timestamp:    time.Now().UTC().Unix(),
				Weight:       4.7,
				UPC:          "test-upc",
			}

			marshalItem, err := json.Marshal(mockItem)
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &cmodel.Event{
				Action:        "ItemRegistered",
				AggregateID:   1,
				CorrelationID: cid,
				Data:          marshalItem,
				NanoTime:      time.Now().UTC().UnixNano(),
				Source:        "test-source",
				UserUUID:      uid,
				UUID:          uuid,
				Version:       1,
				YearBucket:    2018,
			}

			err = itemAdded(coll, mockEvent)
			Expect(err).ToNot(HaveOccurred())

			result, err := coll.FindOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			findItem, assertOK := result.(*model.Item)
			Expect(assertOK).To(BeTrue())
			Expect(mockItem).To(Equal(*findItem))
		})
	})

	Describe("ItemUpdated", func() {
		It("should delete item", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID: itemID.String(),
				Lot:    itemID.String(),
			}
			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			newLot, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			params := &updateParams{
				Filter: map[string]interface{}{
					"itemID": itemID.String(),
				},
				Update: map[string]interface{}{
					"lot": newLot.String(),
				},
			}

			marshalParams, err := json.Marshal(params)
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &cmodel.Event{
				Action:        "ItemUpdated",
				AggregateID:   1,
				CorrelationID: cid,
				Data:          marshalParams,
				NanoTime:      time.Now().UTC().UnixNano(),
				Source:        "test-source",
				UserUUID:      uid,
				UUID:          uuid,
				Version:       1,
				YearBucket:    2018,
			}

			err = itemUpdated(coll, mockEvent)
			Expect(err).ToNot(HaveOccurred())

			mockItem.Lot = newLot.String()
			result, err := coll.FindOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			findItem, assertOK := result.(*model.Item)
			Expect(assertOK).To(BeTrue())
			Expect(findItem.Lot).To(Equal(newLot.String()))
		})
	})
})
