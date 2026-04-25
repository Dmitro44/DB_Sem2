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

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	uri := "mongodb://admin:password@localhost:27017/lr6_db?authSource=admin"
	mongoClient, _ := mongo.Connect(options.Client().ApplyURI(uri))

	runMenu(mongoClient)
}

func runMenu(mcl *mongo.Client) {
	mdb := mcl.Database("lr6_db")
	serv := Service{
		mdb: mdb,
	}
	ctx := context.Background()

	r := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("\n========== Mongo Lab 6 ==========")
		fmt.Println("1. Flush DB and migrate")
		fmt.Println("2. Run Integrity Checks")
		fmt.Println("3. GetUserOrders")
		fmt.Println("4. GetOrdersWithDetails")
		fmt.Println("5. GetTopProductsByRevenue")
		fmt.Println("0. Exit")
		fmt.Print("\nChoose option: ")

		choice, _ := r.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			flushAndMigrate(mdb)
		case "2":
			runIntegrityChecks(mdb)
		case "3":
			handleGetUserOrders(ctx, r, &serv)
		case "4":
			handleGetOrdersWithDetails(ctx, r, &serv)
		case "5":
			handleGetTopProductsByRevenue(ctx, r, &serv)
		case "0":
			fmt.Println("Goodbye!")
			os.Exit(0)
		default:
			fmt.Println("Invalid option, try again")
		}
	}
}

func flushAndMigrate(mdb *mongo.Database) {
	ctx := context.Background()
	err := mdb.Drop(ctx)
	if err != nil {
		log.Fatalf("failed to drop database: %v", err)
	}
	fmt.Println("Database flushed")

	fmt.Println("Migration started")
	migrateUsers(ctx, mdb)
	migrateCategories(ctx, mdb)
	migrateProducts(ctx, mdb)
	migrateOrders(ctx, mdb)
	migrateOrderItems(ctx, mdb)
}

func migrateUsers(ctx context.Context, mdb *mongo.Database) {
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

	collection := mdb.Collection("users")

	count := 0
	for _, row := range records[1:] {
		userID, _ := strconv.Atoi(row[0])

		user := bson.D{
			{"_id", userID},
			{"name", row[1]},
			{"email", row[2]},
			{"createdAt", row[3]},
		}

		_, err := collection.InsertOne(ctx, user)
		if err != nil {
			log.Printf("Error adding user:%d: %v", userID, err)
		}
		count++
	}
	fmt.Printf("    Loaded users: %d\n", count)
}

func migrateCategories(ctx context.Context, mdb *mongo.Database) {
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

	collection := mdb.Collection("categories")

	count := 0
	for _, row := range records[1:] {
		categoryID, _ := strconv.Atoi(row[0])

		category := bson.D{
			{"_id", categoryID},
			{"name", row[1]},
		}

		_, err := collection.InsertOne(ctx, category)
		if err != nil {
			log.Printf("Error adding category:%d: %v", categoryID, err)
		}
		count++
	}
	fmt.Printf("    Loaded categories: %d\n", count)
}

func migrateProducts(ctx context.Context, mdb *mongo.Database) {
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

	collection := mdb.Collection("products")

	count := 0
	for _, row := range records[1:] {
		productID, _ := strconv.Atoi(row[0])
		categoryID, _ := strconv.Atoi(row[2])
		price, _ := strconv.ParseFloat(row[3], 64)

		product := bson.D{
			{"_id", productID},
			{"name", row[1]},
			{"categoryId", categoryID},
			{"price", price},
		}

		_, err := collection.InsertOne(ctx, product)
		if err != nil {
			log.Printf("Error adding product:%d: %v", productID, err)
		}
		count++
	}
	fmt.Printf("    Loaded products: %d\n", count)
}

func migrateOrders(ctx context.Context, mdb *mongo.Database) {
	file, err := os.Open("Data_csv/orderid-userid-createdat-status.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	itemRecords, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	collection := mdb.Collection("orders")

	count := 0
	for _, row := range itemRecords[1:] {
		orderID, _ := strconv.Atoi(row[0])
		userID, _ := strconv.Atoi(row[1])

		order := bson.D{
			{"_id", orderID},
			{"userId", userID},
			{"createdAt", row[2]},
			{"status", row[3]},
		}
		_, err := collection.InsertOne(ctx, order)
		if err != nil {
			log.Printf("Error adding order:%d: %v", orderID, err)
		}
		count++
	}
	fmt.Printf("    Loaded orders: %d\n", count)
}

func migrateOrderItems(ctx context.Context, mdb *mongo.Database) {
	file, err := os.Open("Data_csv/orderitemid-orderid-productid-quantity-price.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	itemRecords, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	collection := mdb.Collection("orderItems")

	count := 0
	for _, row := range itemRecords[1:] {
		orderItemID, _ := strconv.Atoi(row[0])
		orderId, _ := strconv.Atoi(row[1])
		productID, _ := strconv.Atoi(row[2])

		quantity, _ := strconv.ParseInt(row[3], 10, 64)
		price, _ := strconv.ParseFloat(row[4], 64)

		orderItem := bson.D{
			{"_id", orderItemID},
			{"orderId", orderId},
			{"productId", productID},
			{"quantity", quantity},
			{"price", price},
		}
		_, err := collection.InsertOne(ctx, orderItem)
		if err != nil {
			log.Printf("Error adding order item:%d: %v", orderItemID, err)
		}
		count++
	}
	fmt.Printf("    Loaded order items: %d\n", count)
}

func runIntegrityChecks(mdb *mongo.Database) {
	ctx := context.Background()
	fmt.Println("\n--- Data Integrity Checks ---")

	// Count documents in each collection
	collections := []string{"users", "categories", "products", "orders"}
	for _, collName := range collections {
		count, err := mdb.Collection(collName).CountDocuments(ctx, bson.D{})
		if err != nil {
			log.Printf("Error counting %s: %v", collName, err)
			continue
		}
		fmt.Printf("Collection %s: %d documents\n", collName, count)
	}

	// Validate user_id in orders via $lookup
	pipeline := mongo.Pipeline{
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "users"},
			{Key: "localField", Value: "userId"},
			{Key: "foreignField", Value: "id"},
			{Key: "as", Value: "user_details"},
		}}},
		{{Key: "$match", Value: bson.D{
			{Key: "user_details", Value: bson.D{{Key: "$size", Value: 0}}},
		}}},
		{{Key: "$count", Value: "invalid_orders_count"}},
	}

	cursor, err := mdb.Collection("orders").Aggregate(ctx, pipeline)
	if err != nil {
		log.Printf("Integrity check for orders failed: %v", err)
	} else {
		var results []bson.M
		if err = cursor.All(ctx, &results); err == nil && len(results) > 0 {
			fmt.Printf("Invalid orders (missing user): %v\n", results[0]["invalid_orders_count"])
		} else {
			fmt.Println("All orders have valid user_id references.")
		}
	}

	// Find orders with total price > 1000
	totalPipeline := mongo.Pipeline{
		{{Key: "$addFields", Value: bson.D{
			{Key: "calculated_total", Value: bson.D{
				{Key: "$sum", Value: bson.D{
					{Key: "$map", Value: bson.D{
						{Key: "input", Value: "$items"},
						{Key: "as", Value: "item"},
						{Key: "in", Value: bson.D{
							{Key: "$multiply", Value: bson.A{"$$item.quantity", "$$item.price"}},
						}},
					}},
				}},
			}},
		}}},
		{{Key: "$match", Value: bson.D{
			{Key: "calculated_total", Value: bson.D{{Key: "$gt", Value: 1000}}},
		}}},
		{{Key: "$count", Value: "expensive_orders_count"}},
	}

	cursor, err = mdb.Collection("orders").Aggregate(ctx, totalPipeline)
	if err != nil {
		log.Printf("Integrity check orders with total price failed: %v", err)
	} else {
		var results []bson.M
		if err = cursor.All(ctx, &results); err == nil && len(results) > 0 {
			fmt.Printf("Orders with total price > 1000: %v\n", results[0]["expensive_orders_count"])
		} else {
			fmt.Println("No orders found with total price > 1000.")
		}
	}
}

func handleGetUserOrders(ctx context.Context, r *bufio.Reader, serv *Service) {

	fmt.Print("Provide user id: ")
	idStr, _ := r.ReadString('\n')
	idStr = strings.TrimSpace(idStr)
	userId, _ := strconv.Atoi(idStr)

	orders, err := serv.GetUserOrders(ctx, userId)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(orders) == 0 {
		fmt.Println("No orders found.")
		return
	}

	fmt.Printf("\nFound %d orders for user %d:\n", len(orders), userId)
	for _, o := range orders {
		fmt.Printf("\nOrder ID:  %d\n", o.ID)
		fmt.Printf("Date:      %s\n", o.CreatedAt)
		fmt.Printf("Status:    %s\n", o.Status)
		fmt.Printf("Items:\n")
		fmt.Println(strings.Repeat("-", 45))
	}
}

func handleGetOrdersWithDetails(ctx context.Context, r *bufio.Reader, serv *Service) {
	fmt.Print("Provide order id: ")
	idStr, _ := r.ReadString('\n')
	idStr = strings.TrimSpace(idStr)
	orderId, _ := strconv.Atoi(idStr)

	orders, err := serv.GetOrdersWithDetails(ctx, orderId)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(orders) == 0 {
		fmt.Println("No orders found.")
		return
	}

	for _, o := range orders {
		fmt.Printf("\nOrder ID:  %d\n", o.ID)
		fmt.Printf("User ID:   %d\n", o.UserID)
		fmt.Printf("Date:      %s\n", o.CreatedAt)
		fmt.Printf("Status:    %s\n", o.Status)
		fmt.Printf("Items Details:\n")
		var orderTotal float64
		for _, item := range o.Items {
			lineTotal := float64(item.Quantity) * item.Price
			orderTotal += lineTotal
			productName := item.ProductDetails.Name
			if productName == "" {
				productName = "Unknown Product"
			}
			fmt.Printf("  - %-20s | Qty: %-2d | Price: %-8.2f | Total: %.2f\n",
				productName, item.Quantity, item.Price, lineTotal)
		}
		fmt.Printf("Total Amount: %.2f\n", orderTotal)
		fmt.Println(strings.Repeat("-", 55))
	}
}

func handleGetTopProductsByRevenue(ctx context.Context, r *bufio.Reader, serv *Service) {
	fmt.Print("Provide limit (default is 10): ")
	limitStr, _ := r.ReadString('\n')
	limitStr = strings.TrimSpace(limitStr)
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 {
		limit = 10
	}

	products, err := serv.GetTopProductsByRevenue(ctx, limit)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(products) == 0 {
		fmt.Println("No products found.")
		return
	}

	fmt.Printf("\n=== Top %d Products by Revenue ===\n", limit)
	fmt.Printf("%-5s | %-23s | %-12s | %s\n", "ID", "Name", "Revenue", "Quantity Sold")
	fmt.Println(strings.Repeat("-", 55))

	for _, p := range products {
		productID := p["productId"]
		name := p["name"]
		revenue := p["revenue"]
		quantity := p["quantitySold"]

		fmt.Printf("%-5v | %-23s | %-12v | %v\n",
			productID, name, revenue, quantity)
	}
}
