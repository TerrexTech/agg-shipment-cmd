package test

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/Shopify/sarama"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/uuuid"
	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

// TestItem runs integrations tests for service.
func TestItem(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_TOPIC_REQUEST",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "EventHandler Suite")
}

func createCmd(
	prodInput chan<- *sarama.ProducerMessage,
	action string,
	reqTopic string,
	respTopic string,
	data []byte,
) *model.Command {
	cid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())
	uuid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	mockCmd := &model.Command{
		Action:        action,
		CorrelationID: cid,
		Data:          data,
		ResponseTopic: respTopic,
		Source:        "test-source",
		SourceTopic:   reqTopic,
		Timestamp:     time.Now().UTC().Unix(),
		TTLSec:        15,
		UUID:          uuid,
	}
	marshalCmd, err := json.Marshal(mockCmd)
	Expect(err).ToNot(HaveOccurred())
	msg := kafka.CreateMessage(reqTopic, marshalCmd)
	prodInput <- msg

	return mockCmd
}
