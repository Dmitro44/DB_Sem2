package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func main() {
	// Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password
		DB:       0,
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}
	fmt.Println("Connected to Redis successfully")

	runMenu(rdb)

}

func runMenu(rdb *redis.Client) {
	serv := Service{
		rdb: rdb,
	}

	r := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("\n========== Redis Lab 4 ==========")
		fmt.Println("1. Flush DB and migrate")
		fmt.Println("2. GetRecentOrders for user")
		fmt.Println("3. GetTopProducts")
		fmt.Println("4. GetProductsByCategory")
		fmt.Println("5. FilterProductsByPrice")
		fmt.Println("6. Exit")
		fmt.Print("\nChoose option: ")

		choice, _ := r.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			flushAndMigrate(rdb)
		case "2":
			handleGetRecentOrders(r, &serv)
		case "3":
			handleGetTopProducts(r, &serv)
		case "4":
			handleGetProductsByCategory(r, &serv)
		case "5":
			handleGetProductsByPrice(r, &serv)
		case "6":
			fmt.Println("Goodbye!")
			os.Exit(0)
		default:
			fmt.Println("Invalid option, try again")
		}
	}
}

func handleGetTopProducts(r *bufio.Reader, serv *Service) {
	fmt.Print("Enter N (count): ")
	nStr, _ := r.ReadString('\n')
	nStr = strings.TrimSpace(nStr)
	n, _ := strconv.ParseInt(nStr, 10, 64)
	if n <= 0 {
		n = 5
	}

	fmt.Print("Sort by (sales/revenue, default sales): ")
	sortBy, _ := r.ReadString('\n')
	sortBy = strings.TrimSpace(sortBy)
	if sortBy != "revenue" {
		sortBy = "sales"
	}

	stats, err := serv.GetTopProducts(ctx, n, sortBy)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("\nTop %d Products by %s:\n", len(stats), sortBy)
	fmt.Printf("%-5s | %-30s | %-10s | %-10s\n", "ID", "Name", "Price", "Score")
	fmt.Println(strings.Repeat("-", 60))
	for _, s := range stats {
		fmt.Printf("%-5d | %-30s | %-10.2f | %-10.2f\n", s.Product.ProductID, s.Product.Name, s.Product.Price, s.Score)
	}
}

func handleGetProductsByCategory(r *bufio.Reader, serv *Service) {
	fmt.Print("Enter category ID: ")
	idStr, _ := r.ReadString('\n')
	idStr = strings.TrimSpace(idStr)
	categoryID, _ := strconv.Atoi(idStr)

	fmt.Print("Enter minimal price (0 for no filter): ")
	minStr, _ := r.ReadString('\n')
	minStr = strings.TrimSpace(minStr)
	minPrice, _ := strconv.ParseFloat(minStr, 64)

	fmt.Print("Enter maximal price (0 for no filter): ")
	maxStr, _ := r.ReadString('\n')
	maxStr = strings.TrimSpace(maxStr)
	maxPrice, _ := strconv.ParseFloat(maxStr, 64)

	products, err := serv.GetProductsByCategory(ctx, categoryID, minPrice, maxPrice)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(products) == 0 {
		fmt.Println("Products not found.")
		return
	}

	fmt.Printf("\nProducts in category %d (Price filter: %.2f - %.2f):\n", categoryID, minPrice, maxPrice)
	fmt.Printf("%-5s | %-30s | %-12s | %-10s\n", "ID", "Name", "Category", "Price")
	fmt.Println(strings.Repeat("-", 65))
	for _, p := range products {
		fmt.Printf("%-5d | %-30s | %-12d | %-10.2f\n", p.ProductID, p.Name, p.CategoryID, p.Price)
	}
}

func handleGetProductsByPrice(r *bufio.Reader, serv *Service) {
	fmt.Print("Enter minimal price: ")
	minStr, _ := r.ReadString('\n')
	minStr = strings.TrimSpace(minStr)
	minim, _ := strconv.ParseFloat(minStr, 64)

	fmt.Print("Enter maximal price: ")
	maxStr, _ := r.ReadString('\n')
	maxStr = strings.TrimSpace(maxStr)
	maxim, _ := strconv.ParseFloat(maxStr, 64)

	products, err := serv.GetProductsByPriceRange(ctx, minim, maxim)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(products) == 0 {
		fmt.Println("Products in this range not found.")
		return
	}

	fmt.Printf("\nProducts in range from %.2f to %.2f:\n", minim, maxim)
	fmt.Printf("%-5s | %-30s | %-12s | %-10s\n", "ID", "Name", "Category", "Price")
	fmt.Println(strings.Repeat("-", 65))
	for _, p := range products {
		fmt.Printf("%-5d | %-30s | %-12d | %-10.2f\n", p.ProductID, p.Name, p.CategoryID, p.Price)
	}
}

func flushAndMigrate(rdb *redis.Client) {
	fmt.Println("Flushing database...")
	rdb.FlushDB(ctx)

	fmt.Println("\nStarting data migration...")

	migrateUsers(rdb)
	migrateCategories(rdb)
	migrateProducts(rdb)
	migrateOrders(rdb)
	migrateOrderItems(rdb)

	buildIndexes(rdb)

	fmt.Println("\nMigration completed successfully!")
	fmt.Println("\nStatistics:")
	printStats(rdb)
}

func handleGetRecentOrders(r *bufio.Reader, serv *Service) {
	fmt.Print("Enter User ID: ")
	idStr, _ := r.ReadString('\n')
	idStr = strings.TrimSpace(idStr)
	userID, _ := strconv.Atoi(idStr)

	fmt.Print("Enter limit (default 10): ")
	limStr, _ := r.ReadString('\n')
	limStr = strings.TrimSpace(limStr)
	limit, _ := strconv.ParseInt(limStr, 10, 64)
	if limit <= 0 {
		limit = 10
	}

	orders, err := serv.GetRecentOrders(ctx, userID, limit)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(orders) == 0 {
		fmt.Println("No orders found.")
		return
	}

	fmt.Printf("\nRecent %d orders for user %d:\n", len(orders), userID)
	fmt.Printf("%-10s | %-20s | %-10s\n", "Order ID", "Created At", "Status")
	fmt.Println(strings.Repeat("-", 45))
	for _, o := range orders {
		fmt.Printf("%-10d | %-20s | %-10s\n", o.OrderId, o.CreatedAt, o.Status)
	}
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
		err := rdb.HSet(ctx, key, map[string]any{
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

		err := rdb.HSet(ctx, key, map[string]any{
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
		err := rdb.HSet(ctx, key, map[string]any{
			"product_id":  productID,
			"name":        row[1],
			"category_id": categoryID,
			"price":       price,
		}).Err()

		if err != nil {
			log.Printf("Error adding product:%s: %v", productID, err)
			continue
		}

		categoryKey := fmt.Sprintf("category:%s:products", categoryID)
		priceFloat, _ := strconv.ParseFloat(price, 64)
		rdb.ZAdd(ctx, categoryKey, redis.Z{
			Score:  priceFloat,
			Member: productID,
		})

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
		err := rdb.HSet(ctx, key, map[string]any{
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
		err := rdb.HSet(ctx, key, map[string]any{
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
