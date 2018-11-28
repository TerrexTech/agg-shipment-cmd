package model

// AggregateID for Shipment aggregate.
const AggregateID = 2

// Item defines the an item in Shipment.
type Item struct {
	ItemID       string  `bson:"itemID,omitempty" json:"itemID,omitempty"`
	DateArrived  int64   `bson:"dateArrived,omitempty" json:"dateArrived,omitempty"`
	Lot          string  `bson:"lot,omitempty" json:"lot,omitempty"`
	Name         string  `bson:"name,omitempty" json:"name,omitempty"`
	Origin       string  `bson:"origin,omitempty" json:"origin,omitempty"`
	Price        float64 `bson:"price,omitempty" json:"price,omitempty"`
	RSCustomerID string  `bson:"rsCustomerID,omitempty" json:"rsCustomerID,omitempty"`
	SKU          string  `bson:"sku,omitempty" json:"sku,omitempty"`
	Timestamp    int64   `bson:"timestamp,omitempty" json:"timestamp,omitempty"`
	Weight       float64 `bson:"totalWeight,omitempty" json:"totalWeight,omitempty"`
	UPC          string  `bson:"upc,omitempty" json:"upc,omitempty"`
}
