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

type OrderItem struct {
	OrderItemId int     `redis:"order_item_id"`
	OrderId     int     `redis:"order_id"`
	ProductId   int     `redis:"product_id"`
	Quantity    int     `redis:"quantity"`
	Price       float64 `redis:"price"`
}

type OrderWithItems struct {
	Order Order
	Items []OrderItemWithProduct
}

type OrderItemWithProduct struct {
	OrderItem
	ProductName  string
	ProductPrice float64
}

func (s *Service) GetRecentOrders(ctx context.Context, userId int, limit int64) ([]OrderWithItems, error) {
	key := fmt.Sprintf("user:%d:orders", userId)
	orderIDs, err := s.rdb.LRange(ctx, key, 0, limit-1).Result()
	if err != nil {
		return nil, err
	}

	var orders []OrderWithItems

	for _, id := range orderIDs {
		var order Order
		orderKey := fmt.Sprintf("order:%s", id)

		err := s.rdb.HGetAll(ctx, orderKey).Scan(&order)
		if err != nil {
			return nil, err
		}

		// Get order items
		orderItemsKey := fmt.Sprintf("order:%s:items", id)
		orderItemIDs, _ := s.rdb.SMembers(ctx, orderItemsKey).Result()

		var items []OrderItemWithProduct
		for _, itemID := range orderItemIDs {
			var item OrderItem
			itemKey := fmt.Sprintf("order_item:%s", itemID)
			err := s.rdb.HGetAll(ctx, itemKey).Scan(&item)
			if err != nil {
				continue
			}

			// Get product details
			var product Product
			productKey := fmt.Sprintf("product:%d", item.ProductId)
			s.rdb.HGetAll(ctx, productKey).Scan(&product)

			items = append(items, OrderItemWithProduct{
				OrderItem:    item,
				ProductName:  product.Name,
				ProductPrice: product.Price,
			})
		}

		orders = append(orders, OrderWithItems{
			Order: order,
			Items: items,
		})
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

func (s *Service) GetProductsByCategory(ctx context.Context, categoryID int, minPrice, maxPrice float64) ([]Product, error) {
	key := fmt.Sprintf("category:%d:products", categoryID)

	var start, stop any
	start = "-inf"
	stop = "+inf"

	if minPrice > 0 {
		start = minPrice
	}
	if maxPrice > 0 {
		stop = maxPrice
	}

	productIDs, err := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     key,
		ByScore: true,
		Start:   start,
		Stop:    stop,
	}).Result()

	if err != nil {
		return nil, err
	}

	var products []Product
	for _, id := range productIDs {
		var product Product
		productKey := fmt.Sprintf("product:%s", id)

		err := s.rdb.HGetAll(ctx, productKey).Scan(&product)
		if err != nil {
			continue
		}
		products = append(products, product)
	}
	return products, nil
}

func (s *Service) GetProductsByPriceRange(ctx context.Context, minPrice, maxPrice float64) ([]Product, error) {
	var start, stop any
	start = "-inf"
	stop = "+inf"

	if minPrice > 0 {
		start = minPrice
	}
	if maxPrice > 0 {
		stop = maxPrice
	}
	results, err := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     "products:by_price",
		ByScore: true,
		Start:   start,
		Stop:    stop,
	}).Result()

	if err != nil {
		return nil, err
	}

	var products []Product
	for _, id := range results {
		var p Product
		productKey := fmt.Sprintf("product:%s", id)
		err := s.rdb.HGetAll(ctx, productKey).Scan(&p)
		if err != nil {
			continue
		}
		products = append(products, p)
	}
	return products, nil
}

func (s *Service) GetSimilarUsersRecommendations(ctx context.Context, userID int, limit int64) ([]Product, error) {
	userPurchasedKey := fmt.Sprintf("user:%d:purchased", userID)

	userProducts, err := s.rdb.SMembers(ctx, userPurchasedKey).Result()
	if err != nil {
		return nil, err
	}

	if len(userProducts) == 0 {
		return []Product{}, nil
	}

	allUserKeys, err := s.rdb.Keys(ctx, "user:*:purchased").Result()
	if err != nil {
		return nil, err
	}

	var similarUserKeys []string

	// Find similar users
	for _, key := range allUserKeys {
		if key == userPurchasedKey {
			continue
		}

		// find common products
		commonProducts, err := s.rdb.SInter(ctx, userPurchasedKey, key).Result()
		if err != nil {
			continue
		}

		if len(commonProducts) > 0 {
			similarUserKeys = append(similarUserKeys, key)
		}
	}

	if len(similarUserKeys) == 0 {
		return []Product{}, nil
	}

	// Union all products from similar users
	allSimilarProducts, err := s.rdb.SUnion(ctx, similarUserKeys...).Result()
	if err != nil {
		return nil, err
	}

	// union in temp key for SDiff
	tempKey := fmt.Sprintf("temp:recommendations:%d", userID)
	s.rdb.Del(ctx, tempKey)

	// Add all products to temporary set using pipeline for efficiency
	pipe := s.rdb.Pipeline()
	for _, p := range allSimilarProducts {
		pipe.SAdd(ctx, tempKey, p)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		s.rdb.Del(ctx, tempKey)
		return nil, err
	}

	// remove products already purchased by user using SDiff
	recommendations, err := s.rdb.SDiff(ctx, tempKey, userPurchasedKey).Result()
	if err != nil {
		s.rdb.Del(ctx, tempKey)
		return nil, err
	}

	// clean up
	s.rdb.Del(ctx, tempKey)

	// Limit results
	if int64(len(recommendations)) > limit {
		recommendations = recommendations[:limit]
	}

	var products []Product
	for _, productID := range recommendations {
		var p Product
		productKey := fmt.Sprintf("product:%s", productID)
		err := s.rdb.HGetAll(ctx, productKey).Scan(&p)
		if err != nil {
			continue
		}
		products = append(products, p)
	}

	return products, nil
}
