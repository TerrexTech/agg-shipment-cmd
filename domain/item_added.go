package domain

import (
	"encoding/json"

	model "github.com/TerrexTech/agg-shipment-cmd/model"
	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

func itemAdded(coll *mongo.Collection, event *cmodel.Event) error {
	item := &model.Item{}
	err := json.Unmarshal(event.Data, item)
	if err != nil {
		err = errors.Wrap(err, "Error while unmarshalling Event-data")
		return err
	}

	_, err = coll.InsertOne(item)
	if err != nil {
		err = errors.Wrap(err, "Error Inserting Item into database")
		return err
	}

	return nil
}
