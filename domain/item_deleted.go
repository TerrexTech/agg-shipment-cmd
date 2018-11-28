package domain

import (
	"encoding/json"

	"github.com/TerrexTech/go-common-models/model"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

func itemDeleted(coll *mongo.Collection, event *model.Event) error {
	params := map[string]interface{}{}
	err := json.Unmarshal(event.Data, &params)
	if err != nil {
		err = errors.Wrap(err, "Error while unmarshalling Event-data")
		return err
	}

	_, err = coll.DeleteMany(params)
	if err != nil {
		err = errors.Wrap(err, "Error Deleting Item from database")
		return err
	}

	return nil
}
