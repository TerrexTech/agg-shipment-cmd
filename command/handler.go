package command

import (
	"log"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

type cmdConfig struct {
	coll        *mongo.Collection
	serviceName string
	cmd         *model.Command
}

// HandlerConfig is the config for Command-Handler.
type HandlerConfig struct {
	Coll        *mongo.Collection
	ServiceName string

	EventProd  chan<- *model.Event
	ResultProd chan<- *model.Document
}

// Handler for commands.
type Handler struct {
	*HandlerConfig
}

// NewHandler creates a new Command-Handler.
func NewHandler(config *HandlerConfig) (*Handler, error) {
	if config.Coll == nil {
		return nil, errors.New("Coll cannot be nil")
	}
	if config.EventProd == nil {
		return nil, errors.New("EventProd cannot be nil")
	}
	if config.ServiceName == "" {
		return nil, errors.New("ServiceName cannot be blank")
	}

	return &Handler{
		config,
	}, nil
}

// Handle handles the provided command.
func (h *Handler) Handle(cmd *model.Command) {
	var (
		result []byte
		event  *model.Event
		cmdErr *model.Error
	)

	config := &cmdConfig{
		coll:        h.Coll,
		serviceName: h.ServiceName,
		cmd:         cmd,
	}

	switch cmd.Action {
	case "AddItem":
		result, event, cmdErr = addItem(config)
		if cmdErr == nil {
			h.EventProd <- event
		} else {
			log.Println(cmdErr.Message)
		}

	case "DeleteItem":
		result, event, cmdErr = deleteItem(config)
		if cmdErr == nil {
			h.EventProd <- event
		} else {
			log.Println(cmdErr.Message)
		}

	case "UpdateItem":
		result, event, cmdErr = updateItem(config)
		if cmdErr == nil {
			h.EventProd <- event
		} else {
			log.Println(cmdErr.Message)
		}

	default:
		log.Printf("Command contains unregistered Action: %s", cmd.Action)
	}

	// Producer result
	docID, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Erro generating CorrelationID")
		log.Println(err)
	}
	var (
		cmdErrMsg  string
		cmdErrCode int16
	)
	if cmdErr != nil {
		cmdErrMsg = cmdErr.Message
		cmdErrCode = cmdErr.Code
	}
	doc := &model.Document{
		CorrelationID: cmd.UUID,
		Data:          result,
		Error:         cmdErrMsg,
		ErrorCode:     cmdErrCode,
		Source:        h.ServiceName,
		Topic:         cmd.ResponseTopic,
		UUID:          docID,
	}
	h.ResultProd <- doc
}
