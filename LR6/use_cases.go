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

func (s *Service) GetTopProductsByRevenue(ctx context.Context, limit int) ([]bson.M, error) {

	pipeline := mongo.Pipeline{
		// GROUP BY productId, SUM(quantity * price)
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$productId"},
			{Key: "totalRevenue", Value: bson.D{
				{Key: "$sum", Value: bson.D{
					{Key: "$multiply", Value: bson.A{"$quantity", "$price"}},
				}},
			}},
			{Key: "totalQuantity", Value: bson.D{{Key: "$sum", Value: "$quantity"}}},
		}}},

		// ORDER BY totalRevenue DESC
		{{Key: "$sort", Value: bson.D{{Key: "totalRevenue", Value: -1}}}},

		// LIMIT
		{{Key: "$limit", Value: limit}},

		// JOIN with products to get names
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "products"},
			{Key: "localField", Value: "_id"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "product_info"},
		}}},

		// Unwind product_info (array -> object)
		{{Key: "$unwind", Value: "$product_info"}},

		// SELECT productId, name, revenue
		{{Key: "$project", Value: bson.D{
			{Key: "productId", Value: "$_id"},
			{Key: "name", Value: "$product_info.name"},
			{Key: "revenue", Value: "$totalRevenue"},
			{Key: "quantitySold", Value: "$totalQuantity"},
		}}},
	}

	cursor, err := s.mdb.Collection("orderItems").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}

	var result []bson.M
	if err := cursor.All(ctx, &result); err != nil {
		return nil, err
	}

	return result, nil
}
