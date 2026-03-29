package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	// Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password
		DB:       0,  // use default DB
	})

	// Check connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}
	fmt.Println("Connected to Redis successfully")

	// Flush DB (optional - for repeated runs)
	fmt.Println("Flushing database...")
	rdb.FlushDB(ctx)

	// Migrate data
	fmt.Println("\nStarting data migration...")

	migrateUsers(rdb)
	migrateCategories(rdb)
	migrateProducts(rdb)
	migrateOrders(rdb)
	migrateOrderItems(rdb)

	// Build indexes and aggregates
	buildIndexes(rdb)

	fmt.Println("\nMigration completed successfully!")
	fmt.Println("\nStatistics:")
	printStats(rdb)
}

// Migrate users
func migrateUsers(rdb *redis.Client) {
	fmt.Println("  > Loading Users...")
	file, err := os.Open("Data_csv/userid-name-email-createdat.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	count := 0
	for _, row := range records[1:] { // skip header
		userID := row[0]
		key := fmt.Sprintf("user:%s", userID)

		// Create Hash for user
		err := rdb.HSet(ctx, key, map[string]interface{}{
			"user_id":    userID,
			"name":       row[1],
			"email":      row[2],
			"created_at": row[3],
		}).Err()

		if err != nil {
			log.Printf("Error adding user:%s: %v", userID, err)
		}
		count++
	}
	fmt.Printf("    Loaded users: %d\n", count)
}

// Migrate categories
func migrateCategories(rdb *redis.Client) {
	fmt.Println("  > Loading Categories...")
	file, err := os.Open("Data_csv/categoryid-name.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	count := 0
	for _, row := range records[1:] {
		categoryID := row[0]
		key := fmt.Sprintf("category:%s", categoryID)

		err := rdb.HSet(ctx, key, map[string]interface{}{
			"category_id": categoryID,
			"name":        row[1],
		}).Err()

		if err != nil {
			log.Printf("Error adding category:%s: %v", categoryID, err)
		}
		count++
	}
	fmt.Printf("    Loaded categories: %d\n", count)
}

// Migrate products
func migrateProducts(rdb *redis.Client) {
	fmt.Println("  > Loading Products...")
	file, err := os.Open("Data_csv/productid-name-categoryid-price.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	count := 0
	for _, row := range records[1:] {
		productID := row[0]
		categoryID := row[2]
		price := row[3]
		key := fmt.Sprintf("product:%s", productID)

		// Hash for product
		err := rdb.HSet(ctx, key, map[string]interface{}{
			"product_id":  productID,
			"name":        row[1],
			"category_id": categoryID,
			"price":       price,
		}).Err()

		if err != nil {
			log.Printf("Error adding product:%s: %v", productID, err)
			continue
		}

		// Add product to category set
		categoryKey := fmt.Sprintf("category:%s:products", categoryID)
		rdb.SAdd(ctx, categoryKey, productID)

		// Add to Sorted Set by price
		priceFloat, _ := strconv.ParseFloat(price, 64)
		rdb.ZAdd(ctx, "products:by_price", redis.Z{
			Score:  priceFloat,
			Member: productID,
		})

		count++
	}
	fmt.Printf("    Loaded products: %d\n", count)
}

// Migrate orders
func migrateOrders(rdb *redis.Client) {
	fmt.Println("  > Loading Orders...")
	file, err := os.Open("Data_csv/orderid-userid-createdat-status.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	count := 0
	for _, row := range records[1:] {
		orderID := row[0]
		userID := row[1]
		createdAt := row[2]
		status := row[3]
		key := fmt.Sprintf("order:%s", orderID)

		// Hash for order
		err := rdb.HSet(ctx, key, map[string]interface{}{
			"order_id":   orderID,
			"user_id":    userID,
			"created_at": createdAt,
			"status":     status,
		}).Err()

		if err != nil {
			log.Printf("Error adding order:%s: %v", orderID, err)
			continue
		}

		// Add order to user's list (recent orders)
		userOrdersKey := fmt.Sprintf("user:%s:orders", userID)
		rdb.LPush(ctx, userOrdersKey, orderID)

		// Add to Sorted Set by creation time
		timestamp, _ := time.Parse("2006-01-02 15:04:05", createdAt)
		rdb.ZAdd(ctx, "orders:by_time", redis.Z{
			Score:  float64(timestamp.Unix()),
			Member: orderID,
		})

		count++
	}
	fmt.Printf("    Loaded orders: %d\n", count)
}

// Migrate order items
func migrateOrderItems(rdb *redis.Client) {
	fmt.Println("  > Loading OrderItems...")
	file, err := os.Open("Data_csv/orderitemid-orderid-productid-quantity-price.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	count := 0
	for _, row := range records[1:] {
		orderItemID := row[0]
		orderID := row[1]
		productID := row[2]
		quantity := row[3]
		price := row[4]
		key := fmt.Sprintf("order_item:%s", orderItemID)

		// Hash for order item
		err := rdb.HSet(ctx, key, map[string]interface{}{
			"order_item_id": orderItemID,
			"order_id":      orderID,
			"product_id":    productID,
			"quantity":      quantity,
			"price":         price,
		}).Err()

		if err != nil {
			log.Printf("Error adding order_item:%s: %v", orderItemID, err)
			continue
		}

		// Add item to order's set
		orderItemsKey := fmt.Sprintf("order:%s:items", orderID)
		rdb.SAdd(ctx, orderItemsKey, orderItemID)

		count++
	}
	fmt.Printf("    Loaded order items: %d\n", count)
}

// Build indexes and aggregates
func buildIndexes(rdb *redis.Client) {
	fmt.Println("\nBuilding indexes and aggregates...")

	// Calculate sales and revenue by products
	buildProductSalesIndex(rdb)
	buildProductRevenueIndex(rdb)

	fmt.Println("    Indexes built")
}

// Index: products by sales count
func buildProductSalesIndex(rdb *redis.Client) {
	// Read all order_items and calculate sales
	file, err := os.Open("Data_csv/orderitemid-orderid-productid-quantity-price.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	salesMap := make(map[string]int)

	for _, row := range records[1:] {
		productID := row[2]
		quantity, _ := strconv.Atoi(row[3])
		salesMap[productID] += quantity
	}

	// Add to Sorted Set
	for productID, totalSales := range salesMap {
		rdb.ZAdd(ctx, "products:by_sales", redis.Z{
			Score:  float64(totalSales),
			Member: productID,
		})
	}

	fmt.Println("    Index products:by_sales created")
}

// Index: products by revenue
func buildProductRevenueIndex(rdb *redis.Client) {
	file, err := os.Open("Data_csv/orderitemid-orderid-productid-quantity-price.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	revenueMap := make(map[string]float64)

	for _, row := range records[1:] {
		productID := row[2]
		quantity, _ := strconv.Atoi(row[3])
		price, _ := strconv.ParseFloat(row[4], 64)
		revenueMap[productID] += float64(quantity) * price
	}

	// Add to Sorted Set
	for productID, totalRevenue := range revenueMap {
		rdb.ZAdd(ctx, "products:by_revenue", redis.Z{
			Score:  totalRevenue,
			Member: productID,
		})
	}

	fmt.Println("    Index products:by_revenue created")
}

// Print statistics
func printStats(rdb *redis.Client) {
	userCount, _ := rdb.Keys(ctx, "user:*").Result()
	productCount, _ := rdb.Keys(ctx, "product:*").Result()
	orderCount, _ := rdb.Keys(ctx, "order:*").Result()

	fmt.Printf("  Users: %d\n", countNonSubkeys(userCount))
	fmt.Printf("  Products: %d\n", len(productCount))
	fmt.Printf("  Orders: %d\n", countNonSubkeys(orderCount))

	// Top-3 products by sales
	fmt.Println("\nTop-3 products by sales:")
	topSales, _ := rdb.ZRevRangeWithScores(ctx, "products:by_sales", 0, 2).Result()
	for i, z := range topSales {
		productID := z.Member.(string)
		productName, _ := rdb.HGet(ctx, fmt.Sprintf("product:%s", productID), "name").Result()
		fmt.Printf("  %d. %s (sold: %.0f units)\n", i+1, productName, z.Score)
	}
}

// Helper function to count primary keys (without subkeys)
func countNonSubkeys(keys []string) int {
	count := 0
	for _, key := range keys {
		// Count only keys like "user:1", not "user:1:orders"
		if len(key) > 0 && key[len(key)-1] >= '0' && key[len(key)-1] <= '9' {
			count++
		}
	}
	return count
}
