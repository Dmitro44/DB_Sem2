package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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

func (s *Service) GetMonthlyRevenueByCategory(ctx context.Context) ([]bson.M, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "orderItems"},
			{Key: "localField", Value: "_id"},
			{Key: "foreignField", Value: "orderId"},
			{Key: "as", Value: "items"},
		}}},
		{{Key: "$unwind", Value: "$items"}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "products"},
			{Key: "localField", Value: "items.productId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "product"},
		}}},
		{{Key: "$unwind", Value: "$product"}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "categories"},
			{Key: "localField", Value: "product.categoryId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "category"},
		}}},
		{{Key: "$unwind", Value: "$category"}},
		{{Key: "$project", Value: bson.D{
			{Key: "month", Value: bson.D{{Key: "$substr", Value: bson.A{"$createdAt", 0, 7}}}},
			{Key: "categoryName", Value: "$category.name"},
			{Key: "revenue", Value: bson.D{{Key: "$multiply", Value: bson.A{"$items.quantity", "$items.price"}}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "month", Value: "$month"},
				{Key: "category", Value: "$categoryName"},
			}},
			{Key: "totalRevenue", Value: bson.D{{Key: "$sum", Value: "$revenue"}}},
		}}},
		{{Key: "$sort", Value: bson.D{
			{Key: "_id.month", Value: 1},
			{Key: "totalRevenue", Value: -1},
		}}},
		{{Key: "$project", Value: bson.D{
			{Key: "month", Value: "$_id.month"},
			{Key: "category", Value: "$_id.category"},
			{Key: "revenue", Value: "$totalRevenue"},
		}}},
	}

	cursor, err := s.mdb.Collection("orders").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	var results []bson.M
	err = cursor.All(ctx, &results)
	return results, err
}

func (s *Service) GetMarketBasketAnalysis(ctx context.Context) ([]bson.M, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "orderItems"},
			{Key: "localField", Value: "_id"},
			{Key: "foreignField", Value: "orderId"},
			{Key: "as", Value: "items"},
		}}},
		{{Key: "$match", Value: bson.D{
			{Key: "items.1", Value: bson.D{{Key: "$exists", Value: true}}},
		}}},
		{{Key: "$project", Value: bson.D{
			{Key: "productIds", Value: "$items.productId"},
		}}},
		{{Key: "$unwind", Value: "$productIds"}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "orderItems"},
			{Key: "localField", Value: "_id"},
			{Key: "foreignField", Value: "orderId"},
			{Key: "as", Value: "otherItems"},
		}}},
		{{Key: "$unwind", Value: "$otherItems"}},
		{{Key: "$project", Value: bson.D{
			{Key: "p1", Value: "$productIds"},
			{Key: "p2", Value: "$otherItems.productId"},
		}}},
		{{Key: "$match", Value: bson.D{
			{Key: "$expr", Value: bson.D{{Key: "$lt", Value: bson.A{"$p1", "$p2"}}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "p1", Value: "$p1"},
				{Key: "p2", Value: "$p2"},
			}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "count", Value: -1}}}},
		{{Key: "$limit", Value: 10}},
		{{Key: "$project", Value: bson.D{
			{Key: "productA", Value: "$_id.p1"},
			{Key: "productB", Value: "$_id.p2"},
			{Key: "count", Value: "$count"},
		}}},
	}

	cursor, err := s.mdb.Collection("orders").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	var results []bson.M
	err = cursor.All(ctx, &results)
	return results, err
}

func (s *Service) GetRFMAnalysis(ctx context.Context) ([]bson.M, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "orderItems"},
			{Key: "localField", Value: "_id"},
			{Key: "foreignField", Value: "orderId"},
			{Key: "as", Value: "items"},
		}}},
		{{Key: "$addFields", Value: bson.D{
			{Key: "orderTotal", Value: bson.D{{Key: "$sum", Value: bson.D{
				{Key: "$map", Value: bson.D{
					{Key: "input", Value: "$items"},
					{Key: "as", Value: "i"},
					{Key: "in", Value: bson.D{{Key: "$multiply", Value: bson.A{"$$i.quantity", "$$i.price"}}}},
				}},
			}}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$userId"},
			{Key: "lastOrderDate", Value: bson.D{{Key: "$max", Value: "$createdAt"}}},
			{Key: "frequency", Value: bson.D{{Key: "$sum", Value: 1}}},
			{Key: "monetary", Value: bson.D{{Key: "$sum", Value: "$orderTotal"}}},
		}}},
		{{Key: "$project", Value: bson.D{
			{Key: "userId", Value: "$_id"},
			{Key: "recency", Value: "$lastOrderDate"},
			{Key: "frequency", Value: "$frequency"},
			{Key: "monetary", Value: "$monetary"},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "monetary", Value: -1}}}},
	}

	cursor, err := s.mdb.Collection("orders").Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	var results []bson.M
	err = cursor.All(ctx, &results)
	return results, err
}

func (s *Service) CreateMonthlyRevenueMaterializedView(ctx context.Context) error {
	pipeline := mongo.Pipeline{
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "orderItems"},
			{Key: "localField", Value: "_id"},
			{Key: "foreignField", Value: "orderId"},
			{Key: "as", Value: "items"},
		}}},
		{{Key: "$unwind", Value: "$items"}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "products"},
			{Key: "localField", Value: "items.productId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "product"},
		}}},
		{{Key: "$unwind", Value: "$product"}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "categories"},
			{Key: "localField", Value: "product.categoryId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "category"},
		}}},
		{{Key: "$unwind", Value: "$category"}},
		{{Key: "$project", Value: bson.D{
			{Key: "month", Value: bson.D{{Key: "$substr", Value: bson.A{"$createdAt", 0, 7}}}},
			{Key: "categoryName", Value: "$category.name"},
			{Key: "revenue", Value: bson.D{{Key: "$multiply", Value: bson.A{"$items.quantity", "$items.price"}}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{
				{Key: "month", Value: "$month"},
				{Key: "category", Value: "$categoryName"},
			}},
			{Key: "revenue", Value: bson.D{{Key: "$sum", Value: "$revenue"}}},
		}}},
		{{Key: "$merge", Value: bson.D{
			{Key: "into", Value: "mv_monthly_revenue"},
			{Key: "whenMatched", Value: "replace"},
			{Key: "whenNotMatched", Value: "insert"},
		}}},
	}

	_, err := s.mdb.Collection("orders").Aggregate(ctx, pipeline)
	return err
}

func (s *Service) GetMonthlyRevenueFromMV(ctx context.Context) ([]bson.M, error) {
	cursor, err := s.mdb.Collection("mv_monthly_revenue").Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	var results []bson.M
	err = cursor.All(ctx, &results)
	return results, err
}

func (s *Service) GetUserOrders(ctx context.Context, userId int) ([]Order, error) {
	var orders []Order
	filter := bson.D{{Key: "userId", Value: userId}}

	cursor, err := s.mdb.Collection("orders").Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	if err := cursor.All(ctx, &orders); err != nil {
		return nil, err
	}

	return orders, nil
}

func (s *Service) GetOrdersWithDetails(ctx context.Context, orderId int) ([]Order, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "_id", Value: orderId}}}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "orderItems"},
			{Key: "localField", Value: "_id"},
			{Key: "foreignField", Value: "orderId"},
			{Key: "as", Value: "items"},
		}}},
		{{Key: "$unwind", Value: bson.D{
			{Key: "path", Value: "$items"},
			{Key: "preserveNullAndEmptyArrays", Value: true},
		}}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "products"},
			{Key: "localField", Value: "items.productId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "items.product_details"},
		}}},
		{{Key: "$unwind", Value: bson.D{
			{Key: "path", Value: "$items.product_details"},
			{Key: "preserveNullAndEmptyArrays", Value: true},
		}}},
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
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$productId"},
			{Key: "totalRevenue", Value: bson.D{
				{Key: "$sum", Value: bson.D{
					{Key: "$multiply", Value: bson.A{"$quantity", "$price"}},
				}},
			}},
			{Key: "totalQuantity", Value: bson.D{{Key: "$sum", Value: "$quantity"}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "totalRevenue", Value: -1}}}},
		{{Key: "$limit", Value: limit}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "products"},
			{Key: "localField", Value: "_id"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "product_info"},
		}}},
		{{Key: "$unwind", Value: "$product_info"}},
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

func (s *Service) TransferStock(ctx context.Context, productID int, from, to string, qty int) error {
	invColl := s.mdb.Collection("inventory")
	_, err := invColl.UpdateOne(ctx,
		bson.D{{Key: "productId", Value: productID}, {Key: "warehouse", Value: from}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "quantity", Value: -qty}}}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return err
	}

	_, err = invColl.UpdateOne(ctx,
		bson.D{{Key: "productId", Value: productID}, {Key: "warehouse", Value: to}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "quantity", Value: qty}}}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return err
	}

	histColl := s.mdb.Collection("inventoryHistory")
	_, err = histColl.InsertOne(ctx, bson.D{
		{Key: "productId", Value: productID},
		{Key: "from", Value: from},
		{Key: "to", Value: to},
		{Key: "quantity", Value: qty},
		{Key: "timestamp", Value: time.Now().Format(time.RFC3339)},
	})
	return err
}

func (s *Service) BuyLastItem(ctx context.Context, productID int, user string) error {
	invColl := s.mdb.Collection("inventory")

	res, err := invColl.UpdateOne(ctx,
		bson.D{
			{Key: "productId", Value: productID},
			{Key: "quantity", Value: bson.D{{Key: "$gt", Value: 0}}},
		},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "quantity", Value: -1}}}},
	)
	if err != nil {
		return err
	}

	if res.ModifiedCount == 0 {
		return fmt.Errorf("item out of stock")
	}

	return nil
}
