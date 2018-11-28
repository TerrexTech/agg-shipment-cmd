package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/TerrexTech/agg-shipment-cmd/domain"

	"github.com/TerrexTech/go-mongoutils/mongo"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/pkg/errors"
)

type cmdConsConfig struct {
	collection        *mongo.Collection
	builderFunc       domain.BuilderFunc
	builderTimeoutSec int

	handle func(*model.Command)
}

// Handler for Consumer Messages
type cmdConsumer struct {
	cmdConsConfig
}

func newCmdConsumer(config cmdConsConfig) (*cmdConsumer, error) {
	if config.collection == nil {
		err := errors.New("collection cannot be nil")
		return nil, err
	}
	if config.builderFunc == nil {
		err := errors.New("builderFunc cannot be nil")
		return nil, err
	}
	if config.builderTimeoutSec == 0 {
		err := errors.New("builderTimeoutSec cannot be 0")
		return nil, err
	}
	if config.handle == nil {
		err := errors.New("handle cannot be nil")
		return nil, err
	}

	return &cmdConsumer{
		config,
	}, nil
}

func (*cmdConsumer) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing Kafka CmdConsumer")
	return nil
}

func (*cmdConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing Kafka CmdConsumer")
	return nil
}

func (m *cmdConsumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	log.Println("Listening for Commands...")

	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}

		go func(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
			session.MarkMessage(msg, "")

			cmd := &model.Command{}
			err := json.Unmarshal(msg.Value, cmd)
			if err != nil {
				err = errors.Wrap(err, "Error unmarshalling to Command")
				log.Println(err)
				return
			}
			log.Printf("Received Command with ID: %s", cmd.UUID)

			if cmd.ResponseTopic == "" {
				log.Println("Command contains empty ResponseTopic")
				return
			}
			if cmd.Action == "" {
				log.Println("Comman contains empty Action")
				return
			}

			ttlSec := time.Duration(cmd.TTLSec) * time.Second
			expTime := time.Unix(cmd.Timestamp, 0).Add(ttlSec).UTC()
			curTime := time.Now().UTC()
			if expTime.Before(curTime) {
				log.Printf("Command expired, ignoring")
				return
			}

			err = domain.BuildState(m.collection, m.builderFunc, m.builderTimeoutSec)
			if err != nil {
				err = errors.Wrap(err, "Error building Aggregate-state")
				log.Println(err)
				return
			}

			m.handle(cmd)
		}(session, msg)
	}
	return errors.New("context-closed")
}
