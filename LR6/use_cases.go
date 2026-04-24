package main

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Service struct {
	mdb *mongo.Database
}

type Order struct {
	ID        int         `bson:"_id"`
	UserID    int         `bson:"userId"`
	Status    string      `bson:"status"`
	CreatedAt string      `bson:"createdAt"`
	Items     []OrderItem `bson:"items,omitempty"`
}

type Product struct {
	ID         int     `bson:"_id"`
	Name       string  `bson:"name"`
	CategoryID int     `bson:"categoryId"`
	Price      float64 `bson:"price"`
}

type OrderItem struct {
	ID             int     `bson:"id"`
	OrderID        int     `bson:"orderId"`
	ProductID      int     `bson:"productId"`
	Quantity       int64   `bson:"quantity"`
	Price          float64 `bson:"price"`
	ProductDetails Product `bson:"product_details,omitempty"`
}

func (s *Service) GetUserOrders(ctx context.Context, userId int) ([]Order, error) {
	var orders []Order
	filter := bson.D{{Key: "userId", Value: userId}}

	cursor, err := s.mdb.Collection("orders").Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	if err := cursor.All(ctx, &orders); err != nil {
		log.Fatal(err)
		return nil, err
	}

	return orders, nil
}

func (s *Service) GetOrdersWithDetails(ctx context.Context, orderId int) ([]Order, error) {

	pipeline := mongo.Pipeline{
		// Match the order by its _id (from 'orders' collection)
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: orderId}}}},

		// Join with 'orderItems' collection
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "orderItems"},
			{Key: "localField", Value: "_id"},
			{Key: "foreignField", Value: "orderId"},
			{Key: "as", Value: "items"},
		}}},

		// Unwind items to join with products
		{{Key: "$unwind", Value: bson.D{
			{Key: "path", Value: "$items"},
			{Key: "preserveNullAndEmptyArrays", Value: true},
		}}},

		// Join with 'products' collection
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "products"},
			{Key: "localField", Value: "items.productId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "items.product_details"},
		}}},

		// Unwind product details
		{{Key: "$unwind", Value: bson.D{
			{Key: "path", Value: "$items.product_details"},
			{Key: "preserveNullAndEmptyArrays", Value: true},
		}}},

		// Re-group into original Order structure
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$_id"},
			{Key: "userId", Value: bson.D{{Key: "$first", Value: "$userId"}}},
			{Key: "status", Value: bson.D{{Key: "$first", Value: "$status"}}},
			{Key: "createdAt", Value: bson.D{{Key: "$first", Value: "$createdAt"}}},
			{Key: "items", Value: bson.D{{Key: "$push", Value: "$items"}}},
		}}},
	}

	cursor, err := s.mdb.Collection("orders").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}

	var orders []Order

	if err := cursor.All(ctx, &orders); err != nil {
		return nil, err
	}

	return orders, nil
}

func (s *Service) GetTopProductsByRevenue(limit int) {

}
