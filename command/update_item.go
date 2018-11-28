package command

import (
	"encoding/json"
	"time"

	"github.com/TerrexTech/agg-shipment-cmd/model"
	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

type updateParams struct {
	Filter *model.Item `json:"filter,omitempty"`
	Update *model.Item `json:"update,omitempty"`
}

func updateItem(c *cmdConfig) ([]byte, *cmodel.Event, *cmodel.Error) {
	params := &updateParams{}
	err := json.Unmarshal(c.cmd.Data, params)
	if err != nil {
		err = errors.Wrap(err, "Error while unmarshalling cmd-data")
		return nil, nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	validateErr := validateParams(params)
	if err != nil {
		return nil, nil, validateErr
	}

	match, err := c.coll.FindOne(params.Filter)
	if err != nil {
		err = errors.Wrap(err, "Error finding Item")
		return nil, nil, cmodel.NewError(cmodel.UserError, err.Error())
	}
	matchedItem, assertOK := match.(*model.Item)
	if !assertOK {
		err = errors.New("error asserting find-result to item-map")
		return nil, nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	itemMap, err := itemToMap(matchedItem)
	if err != nil {
		err = errors.Wrap(err, "error getting item-map")
		return nil, nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	updatedItem, err := patchItem(c.cmd.Data, itemMap)
	if err != nil {
		err = errors.Wrap(err, "Error patching item")
		return nil, nil, cmodel.NewError(cmodel.InternalError, err.Error())
	}

	updateResult := map[string]interface{}{
		"filter": params.Filter,
		"update": updatedItem,
	}
	marshalResult, err := json.Marshal(updateResult)
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
		Action:        "ItemUpdated",
		AggregateID:   model.AggregateID,
		CorrelationID: c.cmd.UUID,
		Data:          marshalResult,
		NanoTime:      time.Now().UnixNano(),
		Source:        c.serviceName,
		UUID:          uuid,
		YearBucket:    2018,
	}

	return marshalResult, event, nil
}

func itemToMap(u *model.Item) (map[string]interface{}, error) {
	marshalItem, err := json.Marshal(u)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling item")
		return nil, err
	}

	itemMap := map[string]interface{}{}
	err = json.Unmarshal(marshalItem, &itemMap)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling item into map")
		return nil, err
	}
	return itemMap, nil
}

func patchItem(
	updateParams []byte,
	itemMap map[string]interface{},
) (map[string]interface{}, error) {
	// Extract Update from UpdateParams
	updateParamsMap := map[string]map[string]interface{}{}
	err := json.Unmarshal(updateParams, &updateParamsMap)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling UpdateParams")
		return nil, err
	}

	if updateParamsMap["update"] == nil {
		err = errors.New("Update-field not found in updateParamsMap")
		return nil, err
	}
	for k, v := range updateParamsMap["update"] {
		itemMap[k] = v
	}

	return itemMap, nil
}

func validateParams(params *updateParams) *cmodel.Error {
	// filter := params.Filter
	// if filter == nil {
	// 	err := errors.New("nil filter provided")
	// 	return cmodel.NewError(cmodel.UserError, err.Error())
	// }

	// update := params.Update
	// if update == nil {
	// 	err := errors.New("nil update provided")
	// 	return cmodel.NewError(cmodel.UserError, err.Error())
	// }
	// if update.ItemID != "" {
	// 	err := errors.New("itemID cannot be changed")
	// 	return cmodel.NewError(cmodel.UserError, err.Error())
	// }
	// if update.ItemName != "" {
	// 	err := errors.New("itemname cannot be changed")
	// 	return cmodel.NewError(cmodel.UserError, err.Error())
	// }
	// if update.Password == "" {
	// 	err := errors.New("found blank password")
	// 	return cmodel.NewError(cmodel.UserError, err.Error())
	// }

	return nil
}
