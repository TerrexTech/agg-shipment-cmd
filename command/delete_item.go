package command

import (
	"encoding/json"
	"time"

	"github.com/TerrexTech/agg-shipment-cmd/model"
	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

type deleteResult struct {
	MatchedCount int `json:"matchedCount,omitempty"`
}

func deleteItem(c *cmdConfig) ([]byte, *cmodel.Event, *cmodel.Error) {
	inv := &model.Item{}
	err := json.Unmarshal(c.cmd.Data, inv)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling cmd-data into Item")
		return nil, nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	matches, err := c.coll.Find(inv)
	if err != nil || len(matches) == 0 {
		err = errors.New("item not found")
		return nil, nil, cmodel.NewError(cmodel.UserError, err.Error())
	}
	result := deleteResult{
		MatchedCount: len(matches),
	}
	marshalResult, err := json.Marshal(result)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling result")
		return nil, nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	uuid, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Error generating Event-UUID")
		return nil, nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}
	event := &cmodel.Event{
		Action:        "ItemDeleted",
		AggregateID:   model.AggregateID,
		CorrelationID: c.cmd.UUID,
		Data:          c.cmd.Data,
		NanoTime:      time.Now().UnixNano(),
		Source:        "agg-shipment-cmd",
		UUID:          uuid,
		YearBucket:    2018,
	}

	return marshalResult, event, nil
}
