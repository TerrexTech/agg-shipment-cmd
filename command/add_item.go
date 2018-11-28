package command

import (
	"encoding/json"
	"time"

	"github.com/TerrexTech/agg-shipment-cmd/model"
	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

func addItem(c *cmdConfig) ([]byte, *cmodel.Event, *cmodel.Error) {
	item := &model.Item{}
	err := json.Unmarshal(c.cmd.Data, item)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling command-data into Item")
		return nil, nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	item, idErr := updateItemID(item)
	if idErr != nil {
		return nil, nil, idErr
	}
	validateErr := validateItem(c.coll, item)
	if validateErr != nil {
		return nil, nil, validateErr
	}

	cmdData, err := json.Marshal(item)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling Item")
		return nil, nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	eventID, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Error generating EventID")
		return nil, nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}
	event := &cmodel.Event{
		Action:        "ItemRegistered",
		AggregateID:   model.AggregateID,
		CorrelationID: c.cmd.UUID,
		Data:          cmdData,
		NanoTime:      time.Now().UnixNano(),
		Source:        c.serviceName,
		UUID:          eventID,
		YearBucket:    2018,
	}

	return cmdData, event, nil
}

func updateItemID(item *model.Item) (*model.Item, *cmodel.Error) {
	var itemID uuuid.UUID
	var err error

	if item.ItemID != "" {
		itemID, err = uuuid.FromString(item.ItemID)
		if err != nil {
			err = errors.Wrap(err, "Error parsing ItemID")
			return nil, cmodel.NewError(cmodel.UserError, err.Error())
		}
	}

	if itemID == (uuuid.UUID{}) {
		itemID, err := uuuid.NewV4()
		if err != nil {
			err = errors.Wrap(err, "Error generating ItemID")
			return nil, cmodel.NewError(cmodel.UserError, err.Error())
		}
		item.ItemID = itemID.String()
	}

	return item, nil
}

func validateItem(coll *mongo.Collection, item *model.Item) *cmodel.Error {
	if item.DateArrived == 0 {
		err := errors.New("missing DateArrived for item")
		return cmodel.NewError(cmodel.UserError, err.Error())
	}
	if item.Lot == "" {
		err := errors.New("missing Lot for item")
		return cmodel.NewError(cmodel.UserError, err.Error())
	}
	if item.Name == "" {
		err := errors.New("missing Name for item")
		return cmodel.NewError(cmodel.UserError, err.Error())
	}
	if item.Origin == "" {
		err := errors.New("missing Origin for item")
		return cmodel.NewError(cmodel.UserError, err.Error())
	}
	if item.Price == 0 {
		err := errors.New("missing Price for item")
		return cmodel.NewError(cmodel.UserError, err.Error())
	}
	if item.RSCustomerID == "" {
		err := errors.New("missing RSCustomerID for item")
		return cmodel.NewError(cmodel.UserError, err.Error())
	}
	if item.SKU == "" {
		err := errors.New("missing SKU for item")
		return cmodel.NewError(cmodel.UserError, err.Error())
	}
	if item.Timestamp == 0 {
		err := errors.New("missing Timestamp for item")
		return cmodel.NewError(cmodel.UserError, err.Error())
	}
	if item.TotalWeight == 0 {
		err := errors.New("missing TotalWeight for item")
		return cmodel.NewError(cmodel.UserError, err.Error())
	}
	if item.UPC == "" {
		err := errors.New("missing UPC for item")
		return cmodel.NewError(cmodel.UserError, err.Error())
	}

	_, err := coll.FindOne(model.Item{
		ItemID: item.ItemID,
	})
	if err == nil {
		err = errors.New("item already exists")
		return cmodel.NewError(cmodel.UserError, err.Error())
	}

	return nil
}
