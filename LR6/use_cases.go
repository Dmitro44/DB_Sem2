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
	ID        int         `bson:"orderId"`
	UserID    int         `bson:"userId"`
	Status    string      `bson:"status"`
	CreatedAt string      `bson:"createdAt"`
	Items     []OrderItem `bson:"items"`
}

type OrderItem struct {
	ID        int     `bson:"orderItemId"`
	ProductID int     `bson:"productId"`
	Quantity  int64   `bson:"quantity"`
	Price     float64 `bson:"price"`
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

func (s *Service) GetOrdersWithDetails(orderId int) {

}

func (s *Service) GetTopProductsByRevenue(limit int) {

}
