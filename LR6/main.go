package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
	uri := "mongodb://admin:password@localhost:27017/lr6_db"
	mongoClient, _ := mongo.Connect(options.Client().ApplyURI(uri))

}

func runMenu(mdb *mongo.Client) {

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

func flushAndMigrate(mdb *mongo.Client) {
	ctx := context.Background()
	dbName := ""
	err := mdb.Database(dbName).Drop(ctx)
	if err != nil {
		log.Fatalf("failed to drop database: %v", err)
	}
	fmt.Println("Database flushed")

}
