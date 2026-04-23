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
	uri := "mongodb://admin:password@localhost:27017/lr6_db"
	mongoClient, _ := mongo.Connect(options.Client().ApplyURI(uri))

	runMenu(mongoClient)
}

func runMenu(mcl *mongo.Client) {
	mdb := mcl.Database("lr6_db")

	r := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("\n========== Mongo Lab 6 ==========")
		fmt.Println("1. Flush DB and migrate")
		fmt.Println("2. GetRecentOrders for user")
		fmt.Println("3. GetTopProducts")
		fmt.Println("4. GetProductsByCategory")
		fmt.Println("5. FilterProductsByPrice")
		fmt.Println("6. GetRecommendations for user")
		fmt.Println("7. VerifyRecommendations (debug)")
		fmt.Println("8. Exit")
		fmt.Print("\nChoose option: ")

		choice, _ := r.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			flushAndMigrate(mdb)
		case "2":
			runIntegrityChecks(mdb)
		case "3":
			handleGetRecentOrders(r, &serv)
		case "3":
			handleGetTopProducts(r, &serv)
		case "4":
			handleGetProductsByCategory(r, &serv)
		case "5":
			handleGetProductsByPrice(r, &serv)
		case "6":
			handleGetRecommendations(r, &serv)
		case "7":
			handleVerifyRecommendations(r, &serv)
		case "8":
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
	migrateOrdersAndItems(ctx, mdb)
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
		userID := row[0]

		user := bson.D{
			{"userId", userID},
			{"name", row[1]},
			{"email", row[2]},
			{"createdAt", row[3]},
		}

		_, err := collection.InsertOne(ctx, user)
		if err != nil {
			log.Printf("Error adding user:%s: %v", userID, err)
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
		categoryID := row[0]

		category := bson.D{
			{"categoryId", categoryID},
			{"name", row[1]},
		}

		_, err := collection.InsertOne(ctx, category)
		if err != nil {
			log.Printf("Error adding category:%s: %v", categoryID, err)
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
		productID := row[0]
		price, _ := strconv.ParseFloat(row[3], 64)

		product := bson.D{
			{"productId", productID},
			{"name", row[1]},
			{"categoryId", row[2]},
			{"price", price},
		}

		_, err := collection.InsertOne(ctx, product)
		if err != nil {
			log.Printf("Error adding product:%s: %v", productID, err)
		}
		count++
	}
	fmt.Printf("    Loaded products: %d\n", count)
}

func migrateOrdersAndItems(ctx context.Context, mdb *mongo.Database) {
	//Items
	file, err := os.Open("Data_csv/orderitemid-orderid-productid-quantity-price.csv")
	if err != nil {
		log.Fatal(err)
	}

	reader := csv.NewReader(file)
	itemRecords, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	file.Close()

	items := make(map[string][]bson.D, len(itemRecords))
	for _, row := range itemRecords[1:] {
		orderID := row[1]
		quantity, _ := strconv.ParseInt(row[3], 10, 64)
		price, _ := strconv.ParseFloat(row[4], 64)

		items[orderID] = append(items[orderID], bson.D{
			{"orderItemId", row[0]},
			{"productId", row[2]},
			{"quantity", quantity},
			{"price", price},
		})
	}

	//Orders
	file, err = os.Open("Data_csv/orderid-userid-createdat-status.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader = csv.NewReader(file)
	itemRecords, err = reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	collection := mdb.Collection("orders")

	count := 0
	for _, row := range itemRecords[1:] {
		orderID := row[0]

		itemsForOrder := items[orderID]
		if itemsForOrder == nil {
			itemsForOrder = []bson.D{}
		}

		order := bson.D{
			{"orderId", orderID},
			{"userId", row[1]},
			{"createdAt", row[2]},
			{"status", row[3]},
			{"items", itemsForOrder},
		}
		_, err := collection.InsertOne(ctx, order)
		if err != nil {
			log.Printf("Error adding order:%s: %v", orderID, err)
		}
		count++
	}
	fmt.Printf("    Loaded orders: %d\n", count)
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
			{Key: "foreignField", Value: "userId"},
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
