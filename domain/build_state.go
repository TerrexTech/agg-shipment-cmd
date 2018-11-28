package domain

import (
	"log"

	"github.com/TerrexTech/go-agg-builder/builder"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

// BuilderFunc is the function used for building Aggregate-State by fetching event.
type BuilderFunc func(
	correlationID uuuid.UUID,
	timeoutSec int,
) (<-chan *builder.EventResponse, error)

// BuildState builds Aggregate-State by applying previous Events.
func BuildState(coll *mongo.Collection, builderFunc BuilderFunc, timeoutSec int) error {
	cid, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Error generating CorrelationID")
		return err
	}
	eventRespChan, err := builderFunc(cid, timeoutSec)
	if err != nil {
		err = errors.Wrap(err, "Failed to build Aggregate-state from Event-stream")
		return err
	}

	for eventResp := range eventRespChan {
		if eventResp == nil {
			continue
		}
		if eventResp.Error != nil {
			err = errors.Wrap(err, "BuildState: Error in EventResp")
			log.Println(err)
		}

		event := &eventResp.Event
		switch event.Action {
		case "ItemAdded":
			err := itemAdded(coll, event)
			if err != nil {
				err = errors.Wrap(err, "Error adding item")
				log.Println(err)
			}

		case "ItemUpdated":
			err := itemUpdated(coll, event)
			if err != nil {
				err = errors.Wrap(err, "Error updating item")
				log.Println(err)
			}

		case "ItemDeleted":
			err := itemDeleted(coll, event)
			if err != nil {
				err = errors.Wrap(err, "Error deleting item")
				log.Println(err)
			}

		default:
			log.Printf("Event contains unregistered Action: %s", event.Action)
		}
	}

	return nil
}
