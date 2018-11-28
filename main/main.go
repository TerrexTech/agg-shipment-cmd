package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/TerrexTech/agg-shipment-cmd/command"
	"github.com/TerrexTech/agg-shipment-cmd/connutil"
	"github.com/TerrexTech/agg-shipment-cmd/model"
	"github.com/TerrexTech/go-agg-builder/builder"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

// validateEnv checks if all required environment-variables are set.
func validateEnv() {
	// Load environment-file.
	// Env vars will be read directly from environment if this file fails loading
	err := godotenv.Load()
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
		err = errors.Wrapf(err, "Env-var %s is required, but is not set", missingVar)
		log.Fatalln(err)
	}
}

func main() {
	validateEnv()

	kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
	kafkaBrokers := *commonutil.ParseHosts(kafkaBrokersStr)

	kafkaProdConfig := &kafka.ProducerConfig{
		KafkaBrokers: kafkaBrokers,
	}

	// KafkaConfig for Agg-Builder
	esQueryRespTopic := fmt.Sprintf(
		"%s.%d",
		os.Getenv("KAFKA_CONSUMER_TOPIC_ESRESP"),
		model.AggregateID,
	)
	esQueryRespGroup := os.Getenv("KAFKA_CONSUMER_TOPIC_ESRESP")
	esQueryReqTopic := os.Getenv("KAFKA_PRODUCER_TOPIC_ESREQ")
	eosToken := os.Getenv("KAFKA_END_OF_STREAM_TOKEN")
	kc := builder.KafkaConfig{
		ESQueryResCons: &kafka.ConsumerConfig{
			KafkaBrokers: kafkaBrokers,
			Topics:       []string{esQueryRespTopic},
			GroupName:    esQueryRespGroup,
		},
		ESQueryReqProd:  kafkaProdConfig,
		ESQueryReqTopic: esQueryReqTopic,
		EOSToken:        eosToken,
	}

	// Mongo Config
	mc, err := connutil.LoadMongoConfig()
	if err != nil {
		err = errors.Wrap(err, "Error initializing MongoConfig")
		log.Fatalln(err)
	}
	eventsIO, err := builder.Init(builder.IOConfig{
		KafkaConfig: kc,
		MongoConfig: *mc,
	})
	if err != nil {
		err = errors.Wrap(err, "Error initializing Aggregate-eventsIO")
		log.Fatalln(err)
	}

	prodConfig := &producerConfig{
		ctx:         eventsIO.Context(),
		kafkaConfig: kafkaProdConfig,
		g:           eventsIO.ErrGroup(),
	}
	// Event Producer
	eventsTopic := os.Getenv("KAFKA_PRODUCER_TOPIC_EVENTS")
	eventChan, err := eventProducer(prodConfig, eventsTopic)
	if err != nil {
		err = errors.Wrap(err, "Error creating EventProducer")
		log.Fatalln(err)
	}
	// Response Producer
	respChan, err := respProducer(prodConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating ResponseProducer")
		log.Fatalln(err)
	}

	// Command Handler
	serviceName := os.Getenv("SERVICE_NAME")
	cmdHandler, err := command.NewHandler(&command.HandlerConfig{
		Coll:        mc.AggCollection,
		ServiceName: serviceName,
		EventProd:   eventChan,
		ResultProd:  respChan,
	})
	if err != nil {
		err = errors.Wrap(err, "Error initializing command-handler")
		log.Fatalln(err)
	}

	// Command Consumer
	cmdConsGroup := os.Getenv("KAFKA_CONSUMER_GROUP_REQUEST")
	cmdConsTopic := os.Getenv("KAFKA_CONSUMER_TOPIC_REQUEST")
	cmdCons, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		KafkaBrokers: kafkaBrokers,
		GroupName:    cmdConsGroup,
		Topics:       []string{cmdConsTopic},
	})
	if err != nil {
		err = errors.Wrap(err, "Error creating consumer")
		log.Fatalln(err)
	}

	builderTimeoutSecStr := os.Getenv("AGG_BUILDER_TIMEOUT_SEC")
	builderTimeoutSec, err := strconv.Atoi(builderTimeoutSecStr)
	if err != nil {
		err = errors.Wrap(err, "Error converting AGG_BUILDER_TIMEOUT_SEC to integer")
		log.Println(err)
		log.Println("A defalt value of 5 will be used for AGG_BUILDER_TIMEOUT_SEC")
		builderTimeoutSec = 5
	}
	handler, err := newCmdConsumer(cmdConsConfig{
		collection:        mc.AggCollection,
		builderFunc:       eventsIO.BuildState,
		builderTimeoutSec: builderTimeoutSec,
		handle:            cmdHandler.Handle,
	})
	if err != nil {
		err = errors.Wrap(err, "Error initializing Cmd-Handler")
		log.Fatalln(err)
	}
	err = cmdCons.Consume(eventsIO.Context(), handler)
	if err != nil {
		err = errors.Wrap(err, "Error while attempting to consume Commands")
		log.Fatalln(err)
	}
}
