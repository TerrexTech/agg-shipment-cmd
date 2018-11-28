package command

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"

	"github.com/TerrexTech/agg-shipment-cmd/connutil"
	"github.com/TerrexTech/agg-shipment-cmd/model"
	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestCommand tests Command-handling.
func TestCommand(t *testing.T) {
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
	RunSpecs(t, "CommandHandler Suite")
}

func testError(coll *mongo.Collection, action string, data []byte) {
	uitemID, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())
	cid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	mockCmd := &cmodel.Command{
		Action:        action,
		CorrelationID: cid,
		Data:          data,
		ResponseTopic: "test-topic",
		Source:        "test-source",
		SourceTopic:   "test_source-topic",
		Timestamp:     time.Now().UTC().Unix(),
		TTLSec:        15,
		UUID:          uitemID,
	}

	c := &cmdConfig{
		coll:        coll,
		serviceName: "test-svc",
		cmd:         mockCmd,
	}

	result, event, cmdErr := addItem(c)
	Expect(result).To(BeNil())
	Expect(event).To(BeNil())
	Expect(cmdErr.Code).ToNot(BeZero())
	Expect(cmdErr.Message).ToNot(BeEmpty())
}

func testValid(coll *mongo.Collection, action string, data []byte) ([]byte, *cmodel.Event) {
	uitemID, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())
	cid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	mockCmd := &cmodel.Command{
		Action:        action,
		CorrelationID: cid,
		Data:          data,
		ResponseTopic: "test-topic",
		Source:        "test-source",
		SourceTopic:   "test_source-topic",
		Timestamp:     time.Now().UTC().Unix(),
		TTLSec:        15,
		UUID:          uitemID,
	}

	c := &cmdConfig{
		coll:        coll,
		serviceName: "test-svc",
		cmd:         mockCmd,
	}

	var (
		result []byte
		event  *cmodel.Event
		cmdErr *cmodel.Error
	)
	switch action {
	case "AddItem":
		result, event, cmdErr = addItem(c)
	case "DeleteItem":
		result, event, cmdErr = deleteItem(c)
	case "UpdateItem":
		result, event, cmdErr = updateItem(c)
	}

	Expect(cmdErr).To(BeNil())
	Expect(event.CorrelationID).To(Equal(mockCmd.UUID))

	return result, event
}

var _ = Describe("CommanHandler", func() {
	var (
		coll *mongo.Collection
	)

	BeforeSuite(func() {
		mc, err := connutil.LoadMongoConfig()
		Expect(err).ToNot(HaveOccurred())
		coll = mc.AggCollection
	})

	Describe("DeleteItem", func() {
		It("should return error if item is not found", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			item := model.Item{
				ItemID: itemID.String(),
				Lot:    "test-lot",
			}
			marshalItem, err := json.Marshal(item)
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "DeleteItem", marshalItem)
		})

		It("should return ItemDeleted event", func() {
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

			result, event := testValid(coll, "DeleteItem", marshalItem)

			delResult := &deleteResult{}

			err = json.Unmarshal(result, delResult)
			Expect(err).ToNot(HaveOccurred())
			Expect(delResult.MatchedCount).To(BeNumerically(">", 0))
			err = json.Unmarshal(event.Data, delResult)
			Expect(err).ToNot(HaveOccurred())
			Expect(delResult.MatchedCount).To(BeNumerically(">", 0))
		})
	})

	Describe("AddItem", func() {
		Describe("Validations", func() {
			var item *model.Item

			BeforeEach(func() {
				itemID, err := uuuid.NewV4()
				Expect(err).ToNot(HaveOccurred())
				custID, err := uuuid.NewV4()
				Expect(err).ToNot(HaveOccurred())
				item = &model.Item{
					ItemID:       itemID.String(),
					DateArrived:  time.Now().UTC().Unix(),
					Lot:          "test-lot",
					Name:         "test-name",
					Origin:       "test-origin",
					Price:        12.3,
					RSCustomerID: custID.String(),
					SKU:          "test-sku",
					Timestamp:    time.Now().UTC().Unix(),
					TotalWeight:       4.7,
					UPC:          "test-upc",
				}
			})

			It("should create ItemID if it is blank", func() {
				item.ItemID = ""
				marshalItem, err := json.Marshal(item)
				Expect(err).ToNot(HaveOccurred())

				result, event := testValid(coll, "AddItem", marshalItem)

				item := &model.Item{}
				err = json.Unmarshal(result, item)
				Expect(err).ToNot(HaveOccurred())
				Expect(item.ItemID).ToNot(BeEmpty())

				item = &model.Item{}
				err = json.Unmarshal(event.Data, item)
				Expect(err).ToNot(HaveOccurred())
				Expect(item.ItemID).ToNot(BeEmpty())
			})

			It("should return error if DateArrived is missing", func() {
				item.DateArrived = 0
				marshalItem, err := json.Marshal(item)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "AddItem", marshalItem)
			})

			It("should return error if Lot is missing", func() {
				item.Lot = ""
				marshalItem, err := json.Marshal(item)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "AddItem", marshalItem)
			})

			It("should return error if Name is missing", func() {
				item.Name = ""
				marshalItem, err := json.Marshal(item)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "AddItem", marshalItem)
			})

			It("should return error if Origin is missing", func() {
				item.Origin = ""
				marshalItem, err := json.Marshal(item)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "AddItem", marshalItem)
			})

			It("should return error if Price is missing", func() {
				item.Price = 0
				marshalItem, err := json.Marshal(item)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "AddItem", marshalItem)
			})

			It("should return error if RSCustomerID is missing", func() {
				item.RSCustomerID = ""
				marshalItem, err := json.Marshal(item)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "AddItem", marshalItem)
			})

			It("should return error if SKU is missing", func() {
				item.SKU = ""
				marshalItem, err := json.Marshal(item)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "AddItem", marshalItem)
			})

			It("should return error if Timestamp is missing", func() {
				item.Timestamp = 0
				marshalItem, err := json.Marshal(item)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "AddItem", marshalItem)
			})

			It("should return error if TotalWeight is missing", func() {
				item.TotalWeight = 0
				marshalItem, err := json.Marshal(item)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "AddItem", marshalItem)
			})

			It("should return error if UPC is missing", func() {
				item.UPC = ""
				marshalItem, err := json.Marshal(item)
				Expect(err).ToNot(HaveOccurred())
				testError(coll, "AddItem", marshalItem)
			})

			// 	It("should return error if Email is blank", func() {
			// 		item.Email = ""
			// 		marshalItem, err := json.Marshal(item)
			// 		Expect(err).ToNot(HaveOccurred())
			// 		testError(coll, "AddItem", marshalItem)
			// 	})

			// 	It("should return error if FirstName is blank", func() {
			// 		item.FirstName = ""
			// 		marshalItem, err := json.Marshal(item)
			// 		Expect(err).ToNot(HaveOccurred())
			// 		testError(coll, "AddItem", marshalItem)
			// 	})

			// 	It("should return error if Lot is blank", func() {
			// 		item.Lot = ""
			// 		marshalItem, err := json.Marshal(item)
			// 		Expect(err).ToNot(HaveOccurred())
			// 		testError(coll, "AddItem", marshalItem)
			// 	})

			// 	It("should return error if Password is blank", func() {
			// 		item.Password = ""
			// 		marshalItem, err := json.Marshal(item)
			// 		Expect(err).ToNot(HaveOccurred())
			// 		testError(coll, "AddItem", marshalItem)
			// 	})

			// 	It("should return error if Role is blank", func() {
			// 		item.Role = ""
			// 		marshalItem, err := json.Marshal(item)
			// 		Expect(err).ToNot(HaveOccurred())
			// 		testError(coll, "AddItem", marshalItem)
			// 	})
		})

		It("should return error if itemname or itemID already exists", func() {
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

			testError(coll, "AddItem", marshalItem)
		})

		It("should return ItemAdded event on valid params", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			custID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			item := &model.Item{
				ItemID:       itemID.String(),
				DateArrived:  time.Now().UTC().Unix(),
				Lot:          "test-lot",
				Name:         "test-name",
				Origin:       "test-origin",
				Price:        12.3,
				RSCustomerID: custID.String(),
				SKU:          "test-sku",
				Timestamp:    time.Now().UTC().Unix(),
				TotalWeight:       4.7,
				UPC:          "test-upc",
			}
			marshalItem, err := json.Marshal(item)
			Expect(err).ToNot(HaveOccurred())

			result, event := testValid(coll, "AddItem", marshalItem)

			regItem := &model.Item{}
			err = json.Unmarshal(result, regItem)
			Expect(err).ToNot(HaveOccurred())
			Expect(regItem).To(Equal(item))

			regItem = &model.Item{}
			err = json.Unmarshal(event.Data, regItem)
			Expect(err).ToNot(HaveOccurred())
			Expect(regItem).To(Equal(item))
		})
	})

	Describe("Update", func() {
		It("should return error if Filter is nil", func() {
			params, err := json.Marshal(updateParams{
				Update: &model.Item{
					Lot: "test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "UpdateItem", params)
		})

		It("should return error if Update is nil", func() {
			params, err := json.Marshal(updateParams{
				Filter: &model.Item{
					Lot: "test",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "UpdateItem", params)
		})

		It("should return error if ItemID is attempted to be changed", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID: itemID.String(),
				Lot:    itemID.String(),
			}
			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			params, err := json.Marshal(updateParams{
				Filter: &model.Item{
					Lot: itemID.String(),
				},
				Update: &model.Item{
					ItemID: "test-id",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "UpdateItem", params)
		})

		It("should return error if Lot is attempted to be changed", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID: itemID.String(),
				Lot:    itemID.String(),
			}
			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			params, err := json.Marshal(updateParams{
				Filter: &model.Item{
					Lot: itemID.String(),
				},
				Update: &model.Item{
					Lot: "test-lot",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "UpdateItem", params)
		})

		It("should return error if Filter returns no items", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			params, err := json.Marshal(updateParams{
				Filter: &model.Item{
					Lot: itemID.String(),
				},
				Update: &model.Item{
					Lot: "test-lot",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			testError(coll, "UpdateItem", params)
		})

		It("should return ItemUpdated event on valid params", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID: itemID.String(),
				Lot:    itemID.String(),
			}
			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			params, err := json.Marshal(updateParams{
				Filter: &model.Item{
					Lot: itemID.String(),
				},
				Update: &model.Item{
					Lot: "test-lot",
				},
			})
			Expect(err).ToNot(HaveOccurred())

			result, event := testValid(coll, "UpdateItem", params)
			upResult := map[string]interface{}{}

			var _ = event
			err = json.Unmarshal(result, &upResult)
			Expect(err).ToNot(HaveOccurred())

			updatedItem, assertOK := upResult["update"].(map[string]interface{})
			Expect(assertOK).To(BeTrue())

			Expect(updatedItem).To(HaveKeyWithValue("itemID", mockItem.ItemID))
			Expect(updatedItem).To(HaveKeyWithValue("lot", "test-lot"))

			filter, assertOK := upResult["filter"].(map[string]interface{})
			Expect(assertOK).To(BeTrue())
			Expect(filter).To(HaveKeyWithValue("lot", itemID.String()))
		})
	})
})
