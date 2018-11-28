package domain

import (
	"encoding/json"

	"github.com/TerrexTech/go-common-models/model"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

type updateParams struct {
	Filter map[string]interface{} `json:"filter"`
	Update map[string]interface{} `json:"update"`
}

func itemUpdated(coll *mongo.Collection, event *model.Event) error {
	params := &updateParams{}
	err := json.Unmarshal(event.Data, params)
	if err != nil {
		err = errors.Wrap(err, "Error while unmarshalling Event-data")
		return err
	}

	_, err = coll.UpdateMany(params.Filter, params.Update)
	if err != nil {
		err = errors.Wrap(err, "Error Updating Item in database")
		return err
	}

	return nil
}
