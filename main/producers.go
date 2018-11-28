package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"golang.org/x/sync/errgroup"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/pkg/errors"
)

type producerInput struct {
	data  interface{}
	topic string
}

type producerConfig struct {
	ctx         context.Context
	kafkaConfig *kafka.ProducerConfig
	g           *errgroup.Group
}

func eventProducer(config *producerConfig, topic string) (chan<- *model.Event, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}
	if topic == "" {
		return nil, errors.New("topic cannot be empty")
	}

	eventChan := make(chan *model.Event, 256)
	prodChan := make(chan *producerInput, 256)

	err := producer(config, (<-chan *producerInput)(prodChan))
	if err != nil {
		err = errors.Wrap(err, "Error creating EventProducer")
		return nil, err
	}
	go func() {
		for event := range eventChan {
			prodChan <- &producerInput{
				data:  event,
				topic: topic,
			}
		}
		close(prodChan)
	}()

	return (chan<- *model.Event)(eventChan), nil
}

func respProducer(config *producerConfig) (chan<- *model.Document, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}

	respChan := make(chan *model.Document, 256)
	prodChan := make(chan *producerInput, 256)

	err := producer(config, (<-chan *producerInput)(prodChan))
	if err != nil {
		err = errors.Wrap(err, "Error creating RespProducer")
		return nil, err
	}
	go func() {
		for resp := range respChan {
			if resp.Topic == "" {
				err = fmt.Errorf("RespProducer: Empty Topic in Response: %s", resp.UUID)
				log.Println(err)
			}
			prodChan <- &producerInput{
				data:  resp,
				topic: resp.Topic,
			}
		}
		close(prodChan)
	}()

	return (chan<- *model.Document)(respChan), nil
}

func producer(config *producerConfig, inputChan <-chan *producerInput) error {
	prod, err := kafka.NewProducer(config.kafkaConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating Event-Producer")
		log.Println(err)
		return err
	}

	config.g.Go(func() error {
		var prodErr error
	prodLoop:
		for {
			select {
			case <-config.ctx.Done():
				prodErr = errors.New("Event-Producer: session closed")
				break prodLoop

			case err := <-prod.Errors():
				if err != nil && err.Err != nil {
					parsedErr := errors.Wrap(err.Err, "Error in ESQueryRequest-Producer")
					log.Println(parsedErr)
					log.Println(err)
				}

			case input := <-inputChan:
				marshalInput, err := json.Marshal(input.data)
				if err != nil {
					err = errors.Wrap(err, "Error Marshalling Input")
					log.Println(err)
					continue
				}
				msg := kafka.CreateMessage(input.topic, marshalInput)
				prod.Input() <- msg
			}
		}
		return prodErr
	})

	return nil
}
