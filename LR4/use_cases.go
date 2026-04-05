package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Service struct {
	rdb *redis.Client
}

type Order struct {
	OrderId   int    `redis:"order_id"`
	UserId    int    `redis:"user_id"`
	CreatedAt string `redis:"created_at"`
	Status    string `redis:"status"`
}

func (s *Service) GetRecentOrders(ctx context.Context, userId int, limit int64) ([]Order, error) {
	key := fmt.Sprintf("user:%d:orders", userId)
	orderIDs, err := s.rdb.LRange(ctx, key, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}

	var orders []Order

	for _, id := range orderIDs {
		var order Order
		orderKey := fmt.Sprintf("order:%s", id)

		err := s.rdb.HGetAll(ctx, orderKey).Scan(&order)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}
	return orders, nil
}
