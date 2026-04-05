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

type Product struct {
	ProductID  int     `redis:"product_id"`
	Name       string  `redis:"name"`
	CategoryID int     `redis:"category_id"`
	Price      float64 `redis:"price"`
}

type ProductStat struct {
	Product Product
	Score   float64
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

func (s *Service) GetTopProducts(ctx context.Context, n int64, sortBy string) ([]ProductStat, error) {
	var key string
	if sortBy == "revenue" {
		key = "products:by_revenue"
	} else {
		key = "products:by_sales"
	}

	results, err := s.rdb.ZRevRangeWithScores(ctx, key, 0, n-1).Result()
	if err != nil {
		return nil, err
	}

	var stats []ProductStat
	for _, z := range results {
		productID := z.Member.(string)
		var p Product
		productKey := fmt.Sprintf("product:%s", productID)

		err := s.rdb.HGetAll(ctx, productKey).Scan(&p)
		if err != nil {
			return nil, err
		}

		stats = append(stats, ProductStat{
			Product: p,
			Score:   z.Score,
		})
	}

	return stats, nil
}
