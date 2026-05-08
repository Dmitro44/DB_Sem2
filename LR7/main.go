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

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	uri := "mongodb://admin:password@localhost:27017/lr7_db?authSource=admin"
	mongoClient, _ := mongo.Connect(options.Client().ApplyURI(uri))

	runMenu(mongoClient)
}

func runMenu(mcl *mongo.Client) {
	mdb := mcl.Database("lr7_db")
	serv := Service{
		mdb: mdb,
	}
	ctx := context.Background()

	r := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("\n========== Mongo Lab 7 ==========")
		fmt.Println("1. Flush DB and migrate")
		fmt.Println("2. Monthly Revenue by Category")
		fmt.Println("3. Market Basket Analysis")
		fmt.Println("4. RFM Analysis")
		fmt.Println("5. Materialized View Performance Comparison")
		fmt.Println("6. Complex Process: Stock Transfer")
		fmt.Println("7. Isolation: Race Condition Simulation")
		fmt.Println("0. Exit")
		fmt.Print("\nChoose option: ")

		choice, _ := r.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			flushAndMigrate(mdb)
		case "2":
			handleMonthlyRevenueByCategory(ctx, &serv)
		case "3":
			handleMarketBasketAnalysis(ctx, &serv)
		case "4":
			handleRFMAnalysis(ctx, &serv)
		case "5":
			handlePerformanceComparison(ctx, &serv)
		case "6":
			handleStockTransfer(ctx, &serv)
		case "7":
			handleIsolationTest(ctx, &serv)
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
			{Key: "foreignField", Value: "_id"},
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

func generatePerformanceData(ctx context.Context, mdb *mongo.Database) {
	fmt.Println("Generating performance test data (50,000 orders)...")

	coll := mdb.Collection("orders")

	_ = coll.Drop(ctx)

	var docs []any
	batchSize := 2000
	totalOrders := 50000

	now := time.Now()

	for i := 1; i <= totalOrders; i++ {
		userID := (i % 100) + 1
		createdTime := now.Add(-time.Duration(i) * time.Minute)

		doc := bson.D{
			{Key: "_id", Value: i},
			{Key: "userId", Value: userID},
			{Key: "createdAt", Value: createdTime.Format(time.RFC3339)}, // format like in csv
			{Key: "status", Value: "completed"},
		}

		docs = append(docs, doc)

		// Insert batches instead of one by one
		if len(docs) >= batchSize {
			_, err := coll.InsertMany(ctx, docs)
			if err != nil {
				log.Printf("Error inserting batch: %v", err)
			}
			docs = []any{} // free up slice

			if i%10000 == 0 {
				fmt.Printf("  Generated %d / %d\n", i, totalOrders)
			}
		}
	}

	if len(docs) > 0 {
		_, _ = coll.InsertMany(ctx, docs)
	}

	fmt.Println("Generation complete.")
}

func handlePerformanceTest(ctx context.Context, mdb *mongo.Database) {
	fmt.Println("\n=== Performance Test (Large Data) ===")

	mdb.Drop(ctx)

	generatePerformanceData(ctx, mdb)

	collName := "orders"

	filter := bson.D{{Key: "userId", Value: 1}}
	sort := bson.D{{Key: "createdAt", Value: -1}}

	fmt.Println("\n--- WITHOUT INDEX ---")
	printStats(ctx, mdb, collName, filter, sort, nil)

	fmt.Println("\nCreating index: { userId: 1, createdAt: -1 } ...")
	mdb.Collection(collName).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "userId", Value: 1}, {Key: "createdAt", Value: -1}},
	})
	fmt.Println("Index created.")

	fmt.Println("\n--- WITH INDEX ---")
	hint := bson.D{{Key: "userId", Value: 1}, {Key: "createdAt", Value: -1}}
	printStats(ctx, mdb, collName, filter, sort, hint)
}

func toMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	if d, ok := v.(bson.D); ok {
		m := make(map[string]any)
		for _, e := range d {
			m[e.Key] = e.Value
		}
		return m
	}
	return make(map[string]any)
}

func printStats(ctx context.Context, db *mongo.Database, collName string, filter, sort, hint bson.D) {
	findCmd := bson.D{
		{Key: "find", Value: collName},
		{Key: "filter", Value: filter},
		{Key: "sort", Value: sort},
	}
	if hint != nil {
		findCmd = append(findCmd, bson.E{Key: "hint", Value: hint})
	}

	var res bson.M
	err := db.RunCommand(ctx, bson.D{
		{Key: "explain", Value: findCmd},
		{Key: "verbosity", Value: "executionStats"},
	}).Decode(&res)

	if err != nil {
		fmt.Printf("Error running explain: %v\n", err)
		return
	}

	execStats := toMap(res["executionStats"])
	queryPlanner := toMap(res["queryPlanner"])
	winningPlan := toMap(queryPlanner["winningPlan"])

	stage, _ := winningPlan["stage"].(string)

	getNum := func(k string) int64 {
		switch v := execStats[k].(type) {
		case int32:
			return int64(v)
		case int64:
			return v
		case float64:
			return int64(v)
		default:
			return 0
		}
	}

	fmt.Printf("Stage: %s\n", stage)
	fmt.Printf("Docs Examined: %d\n", getNum("totalDocsExamined"))
	fmt.Printf("Execution Time (ms): %d\n", getNum("executionTimeMillis"))
	fmt.Println("----------------------------------")
}

func handleMonthlyRevenueByCategory(ctx context.Context, serv *Service) {

	results, err := serv.GetMonthlyRevenueByCategory(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("\n=== Monthly Revenue by Category ===")
	for _, res := range results {
		fmt.Printf("Month: %s | Category: %s | Revenue: %.2f\n",
			res["month"], res["category"], res["revenue"])
	}
}

func handleMarketBasketAnalysis(ctx context.Context, serv *Service) {
	results, err := serv.GetMarketBasketAnalysis(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("\n=== Market Basket Analysis (Top Pairs) ===")
	for _, res := range results {
		fmt.Printf("Products: [%v, %v] | Times bought together: %v\n",
			res["productA"], res["productB"], res["count"])
	}
}

func handleRFMAnalysis(ctx context.Context, serv *Service) {
	results, err := serv.GetRFMAnalysis(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("\n=== RFM Analysis ===")
	fmt.Printf("%-10s | %-19s | %-10s | %-10s\n", "User ID", "Recency", "Frequency", "Monetary")
	for _, res := range results {
		fmt.Printf("%-10v | %-8v | %-10v | %-10.2f\n",
			res["userId"], res["recency"], res["frequency"], res["monetary"])
	}
}

func handlePerformanceComparison(ctx context.Context, serv *Service) {
	fmt.Println("\n=== Performance Comparison: Live vs Materialized ===")

	startLive := time.Now()
	_, err := serv.GetMonthlyRevenueByCategory(ctx)
	durationLive := time.Since(startLive)
	if err != nil {
		fmt.Printf("Live Error: %v\n", err)
		return
	}
	fmt.Printf("Live Aggregation Time: %v\n", durationLive)

	err = serv.CreateMonthlyRevenueMaterializedView(ctx)
	if err != nil {
		fmt.Printf("MV Creation Error: %v\n", err)
		return
	}
	fmt.Println("Materialized View Created ($merge)")

	startMV := time.Now()
	_, err = serv.GetMonthlyRevenueFromMV(ctx)
	durationMV := time.Since(startMV)
	if err != nil {
		fmt.Printf("MV Read Error: %v\n", err)
		return
	}
	fmt.Printf("Materialized View Read Time: %v\n", durationMV)
	fmt.Printf("Speedup: %.2fx\n", float64(durationLive)/float64(durationMV))
}

func handleStockTransfer(ctx context.Context, serv *Service) {
	fmt.Println("\n=== Complex Business Process: Stock Transfer ===")
	productID := 1
	fromWarehouse := "Main"
	toWarehouse := "Secondary"
	qty := 5

	fmt.Printf("Transferring %d units of Product %d from %s to %s...\n",
		qty, productID, fromWarehouse, toWarehouse)

	err := serv.TransferStock(ctx, productID, fromWarehouse, toWarehouse, qty)
	if err != nil {
		fmt.Printf("Transfer Error: %v\n", err)
		return
	}
	fmt.Println("Transfer successful. Inventory updated and history record created.")
}

func handleIsolationTest(ctx context.Context, serv *Service) {
	fmt.Println("\n=== Isolation Test: Race Condition Simulation ===")
	productID := 99
	initialStock := 2

	serv.mdb.Collection("inventory").UpdateOne(ctx,
		bson.D{{Key: "productId", Value: productID}, {Key: "warehouse", Value: "Shop"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "quantity", Value: initialStock}}}},
		options.UpdateOne().SetUpsert(true),
	)

	fmt.Printf("Initial stock for Product %d: %d\n", productID, initialStock)
	fmt.Println("Simulating two users trying to buy the last item simultaneously...")

	done := make(chan string, 3)

	buy := func(userName string) {
		err := serv.BuyLastItem(ctx, productID, userName)
		if err != nil {
			done <- fmt.Sprintf("User %s: Failed - %v", userName, err)
		} else {
			done <- fmt.Sprintf("User %s: Success!", userName)
		}
	}

	go buy("Alice")
	go buy("Bob")
	go buy("Diam")

	fmt.Println(<-done)
	fmt.Println(<-done)
	fmt.Println(<-done)

	var finalDoc bson.M
	serv.mdb.Collection("inventory").FindOne(ctx, bson.D{{Key: "productId", Value: productID}}).Decode(&finalDoc)
	fmt.Printf("Final stock: %v\n", finalDoc["quantity"])
}
