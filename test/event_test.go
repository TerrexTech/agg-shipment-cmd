package test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/agg-shipment-cmd/connutil"
	"github.com/TerrexTech/agg-shipment-cmd/model"
	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EventTest", func() {
	var (
		coll *mongo.Collection

		reqTopic   string
		eventTopic string

		producer   *kafka.Producer
		consConfig *kafka.ConsumerConfig
	)

	BeforeEach(func() {
		var _ = coll
		mc, err := connutil.LoadMongoConfig()
		Expect(err).ToNot(HaveOccurred())
		coll = mc.AggCollection

		kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
		kafkaBrokers := *commonutil.ParseHosts(kafkaBrokersStr)

		producer, err = kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		})
		Expect(err).ToNot(HaveOccurred())

		eventTopic = "event.rns_eventstore.events"
		reqTopic = os.Getenv("KAFKA_CONSUMER_TOPIC_REQUEST")
		consConfig = &kafka.ConsumerConfig{
			KafkaBrokers: kafkaBrokers,
			Topics:       []string{eventTopic},
			GroupName:    "test.group.1",
		}
	})

	Describe("AddItem", func() {
		It("should create AddItem response", func(done Done) {
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

			mockCmd := createCmd(producer.Input(), "AddItem", reqTopic, "test-topic", marshalItem)

			consumer, err := kafka.NewConsumer(consConfig)
			Expect(err).ToNot(HaveOccurred())

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				event := &cmodel.Event{}
				err := json.Unmarshal(msg.Value, event)
				Expect(err).ToNot(HaveOccurred())

				if event.CorrelationID == mockCmd.UUID {
					itemModel := &model.Item{}
					err = json.Unmarshal(event.Data, itemModel)
					Expect(err).ToNot(HaveOccurred())

					if itemModel.ItemID == mockItem.ItemID {
						Expect(*itemModel).To(Equal(mockItem))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = consumer.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = consumer.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 15)
	})

	Describe("DeleteItem", func() {
		It("should create DeleteItem response", func(done Done) {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			custID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := &model.Item{
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
			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			marshalItem, err := json.Marshal(mockItem)
			Expect(err).ToNot(HaveOccurred())

			mockCmd := createCmd(producer.Input(), "DeleteItem", reqTopic, "test-topic", marshalItem)

			consumer, err := kafka.NewConsumer(consConfig)
			Expect(err).ToNot(HaveOccurred())

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				event := &cmodel.Event{}
				err := json.Unmarshal(msg.Value, event)
				Expect(err).ToNot(HaveOccurred())

				if event.CorrelationID == mockCmd.UUID {
					delResult := &model.Item{}
					err = json.Unmarshal(event.Data, delResult)
					Expect(err).ToNot(HaveOccurred())

					Expect(mockItem).To(Equal(delResult))
					return true
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = consumer.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = consumer.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 15)
	})

	Describe("UpdateItem", func() {
		It("should create UpdateItem response", func(done Done) {
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
			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			newLot, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			params := map[string]interface{}{
				"filter": &model.Item{
					ItemID: itemID.String(),
				},
				"update": &model.Item{
					Lot: newLot.String(),
				},
			}
			marshalParams, err := json.Marshal(params)
			Expect(err).ToNot(HaveOccurred())

			mockCmd := createCmd(producer.Input(), "UpdateItem", reqTopic, "test-topic", marshalParams)

			consumer, err := kafka.NewConsumer(consConfig)
			Expect(err).ToNot(HaveOccurred())

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				event := &cmodel.Event{}
				err := json.Unmarshal(msg.Value, event)
				Expect(err).ToNot(HaveOccurred())

				if event.CorrelationID == mockCmd.UUID {
					updateResult := map[string]interface{}{}
					err = json.Unmarshal(event.Data, &updateResult)
					Expect(err).ToNot(HaveOccurred())

					updatedItem, assertOK := updateResult["update"].(map[string]interface{})

					Expect(assertOK).To(BeTrue())

					// Since large numbers transform into scientific-notation
					mockDateArr := fmt.Sprintf("%d", mockItem.DateArrived)
					updatedItemDateArr := fmt.Sprintf("%.0f", updatedItem["dateArrived"])
					Expect(mockDateArr).To(Equal(updatedItemDateArr))

					Expect(updatedItem).To(HaveKeyWithValue("itemID", mockItem.ItemID))
					Expect(updatedItem).To(HaveKeyWithValue("lot", newLot.String()))
					Expect(updatedItem).To(HaveKeyWithValue("name", mockItem.Name))
					Expect(updatedItem).To(HaveKeyWithValue("origin", mockItem.Origin))
					Expect(updatedItem).To(HaveKeyWithValue("price", mockItem.Price))
					Expect(updatedItem).To(HaveKeyWithValue("rsCustomerID", mockItem.RSCustomerID))
					Expect(updatedItem).To(HaveKeyWithValue("sku", mockItem.SKU))
					Expect(updatedItem).To(HaveKeyWithValue("totalWeight", mockItem.Weight))
					Expect(updatedItem).To(HaveKeyWithValue("upc", mockItem.UPC))

					filter, assertOK := updateResult["filter"].(map[string]interface{})
					Expect(assertOK).To(BeTrue())
					Expect(filter).To(HaveKeyWithValue("itemID", itemID.String()))

					return true
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = consumer.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			err = consumer.Close()
			Expect(err).ToNot(HaveOccurred())
			close(done)
		}, 15)
	})
})
