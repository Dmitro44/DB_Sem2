package main

import "go.mongodb.org/mongo-driver/v2/mongo"

type Service struct {
	mdb *mongo.Client
}

func (s *Service) GetUserOrders(userId int) {

}

func (s *Service) GetOrdersWithDetails(orderId int) {

}

func (s *Service) GetTopProductsByRevenue(limit int) {

}
