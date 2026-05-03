package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SQLService struct {
	pool *pgxpool.Pool
}

func (s *SQLService) MigrateAndLoadCSV(ctx context.Context) error {
	fmt.Println("Flushing and migrating SQL database...")

	tables := []string{
		"order_items",
		"orders",
		"products",
		"categories",
		"users",
	}

	for _, table := range tables {
		_, err := s.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", table))
		if err != nil {
			return fmt.Errorf("failed to drop table %s: %w", table, err)
		}
	}

	createTables := `
	CREATE TABLE users (
		user_id INT PRIMARY KEY,
		name TEXT,
		email TEXT,
		created_at TIMESTAMP
	);

	CREATE TABLE categories (
		category_id INT PRIMARY KEY,
		name TEXT
	);

	CREATE TABLE products (
		product_id INT PRIMARY KEY,
		name TEXT,
		category_id INT REFERENCES categories(category_id),
		price DECIMAL(10, 2)
	);

	CREATE TABLE orders (
		order_id INT PRIMARY KEY,
		user_id INT REFERENCES users(user_id),
		created_at TIMESTAMP,
		status TEXT
	);

	CREATE TABLE order_items (
		order_item_id INT PRIMARY KEY,
		order_id INT REFERENCES orders(order_id),
		product_id INT REFERENCES products(product_id),
		quantity INT,
		price DECIMAL(10, 2)
	);
	`
	_, err := s.pool.Exec(ctx, createTables)
	if err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// Load Users
	fmt.Println("  > SQL: Loading Users...")
	if err := s.loadUsers(ctx); err != nil {
		return err
	}

	// Load Categories
	fmt.Println("  > SQL: Loading Categories...")
	if err := s.loadCategories(ctx); err != nil {
		return err
	}

	// Load Products
	fmt.Println("  > SQL: Loading Products...")
	if err := s.loadProducts(ctx); err != nil {
		return err
	}

	// Load Orders
	fmt.Println("  > SQL: Loading Orders...")
	if err := s.loadOrders(ctx); err != nil {
		return err
	}

	// Load Order Items
	fmt.Println("  > SQL: Loading Order Items...")
	if err := s.loadOrderItems(ctx); err != nil {
		return err
	}

	fmt.Println("SQL migration completed successfully!")
	return nil
}

func (s *SQLService) loadUsers(ctx context.Context) error {
	file, err := os.Open("Data_csv/userid-name-email-createdat.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	for _, row := range records[1:] {
		userID, _ := strconv.Atoi(row[0])
		_, err := s.pool.Exec(ctx, "INSERT INTO users (user_id, name, email, created_at) VALUES ($1, $2, $3, $4)",
			userID, row[1], row[2], row[3])
		if err != nil {
			log.Printf("Error adding SQL user %d: %v", userID, err)
		}
	}
	return nil
}

func (s *SQLService) loadCategories(ctx context.Context) error {
	file, err := os.Open("Data_csv/categoryid-name.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	for _, row := range records[1:] {
		categoryID, _ := strconv.Atoi(row[0])
		_, err := s.pool.Exec(ctx, "INSERT INTO categories (category_id, name) VALUES ($1, $2)",
			categoryID, row[1])
		if err != nil {
			log.Printf("Error adding SQL category %d: %v", categoryID, err)
		}
	}
	return nil
}

func (s *SQLService) loadProducts(ctx context.Context) error {
	file, err := os.Open("Data_csv/productid-name-categoryid-price.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	for _, row := range records[1:] {
		productID, _ := strconv.Atoi(row[0])
		categoryID, _ := strconv.Atoi(row[2])
		price, _ := strconv.ParseFloat(row[3], 64)
		_, err := s.pool.Exec(ctx, "INSERT INTO products (product_id, name, category_id, price) VALUES ($1, $2, $3, $4)",
			productID, row[1], categoryID, price)
		if err != nil {
			log.Printf("Error adding SQL product %d: %v", productID, err)
		}
	}
	return nil
}

func (s *SQLService) loadOrders(ctx context.Context) error {
	file, err := os.Open("Data_csv/orderid-userid-createdat-status.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	for _, row := range records[1:] {
		orderID, _ := strconv.Atoi(row[0])
		userID, _ := strconv.Atoi(row[1])
		_, err := s.pool.Exec(ctx, "INSERT INTO orders (order_id, user_id, created_at, status) VALUES ($1, $2, $3, $4)",
			orderID, userID, row[2], row[3])
		if err != nil {
			log.Printf("Error adding SQL order %d: %v", orderID, err)
		}
	}
	return nil
}

func (s *SQLService) loadOrderItems(ctx context.Context) error {
	file, err := os.Open("Data_csv/orderitemid-orderid-productid-quantity-price.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	for _, row := range records[1:] {
		orderItemID, _ := strconv.Atoi(row[0])
		orderID, _ := strconv.Atoi(row[1])
		productID, _ := strconv.Atoi(row[2])
		quantity, _ := strconv.Atoi(row[3])
		price, _ := strconv.ParseFloat(row[4], 64)
		_, err := s.pool.Exec(ctx, "INSERT INTO order_items (order_item_id, order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4, $5)",
			orderItemID, orderID, productID, quantity, price)
		if err != nil {
			log.Printf("Error adding SQL order_item %d: %v", orderItemID, err)
		}
	}
	return nil
}

func (s *SQLService) GetRecentOrders(ctx context.Context, userID int, limit int64) ([]OrderWithItems, error) {
	query := `
		SELECT order_id, user_id, TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS'), status
		FROM orders
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT $2
	`
	rows, err := s.pool.Query(ctx, query, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orders []OrderWithItems
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.OrderId, &o.UserId, &o.CreatedAt, &o.Status); err != nil {
			return nil, err
		}

		itemsQuery := `
			SELECT oi.order_item_id, oi.order_id, oi.product_id, oi.quantity, oi.price, p.name, p.price
			FROM order_items oi
			JOIN products p ON oi.product_id = p.product_id
			WHERE oi.order_id = $1
		`
		itemRows, err := s.pool.Query(ctx, itemsQuery, o.OrderId)
		if err != nil {
			return nil, err
		}

		var items []OrderItemWithProduct
		for itemRows.Next() {
			var i OrderItemWithProduct
			if err := itemRows.Scan(&i.OrderItemId, &i.OrderId, &i.ProductId, &i.Quantity, &i.OrderItem.Price, &i.ProductName, &i.ProductPrice); err != nil {
				itemRows.Close()
				return nil, err
			}
			items = append(items, i)
		}
		itemRows.Close()

		orders = append(orders, OrderWithItems{
			Order: o,
			Items: items,
		})
	}
	return orders, nil
}

func (s *SQLService) GetTopProducts(ctx context.Context, n int64, sortBy string) ([]ProductStat, error) {
	var query string
	if sortBy == "revenue" {
		query = `
			SELECT p.product_id, p.name, p.category_id, p.price, COALESCE(SUM(oi.quantity * oi.price), 0) as score
			FROM products p
			LEFT JOIN order_items oi ON p.product_id = oi.product_id
			GROUP BY p.product_id
            ORDER BY score DESC, price DESC
    		LIMIT $1
		`
	} else {
		query = `
			SELECT p.product_id, p.name, p.category_id, p.price, COALESCE(SUM(oi.quantity), 0) as score
			FROM products p
			LEFT JOIN order_items oi ON p.product_id = oi.product_id
			GROUP BY p.product_id
            ORDER BY score DESC, price DESC
    		LIMIT $1
		`
	}

	rows, err := s.pool.Query(ctx, query, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []ProductStat
	for rows.Next() {
		var p ProductStat
		if err := rows.Scan(&p.Product.ProductID, &p.Product.Name, &p.Product.CategoryID, &p.Product.Price, &p.Score); err != nil {
			return nil, err
		}
		stats = append(stats, p)
	}
	return stats, nil
}

func (s *SQLService) GetProductsByCategory(ctx context.Context, categoryID int, minPrice, maxPrice float64) ([]Product, error) {
	query := `
		SELECT product_id, name, category_id, price
		FROM products
		WHERE category_id = $1
	`
	var args []interface{}
	args = append(args, categoryID)
	paramIdx := 2

	if minPrice > 0 {
		query += fmt.Sprintf(" AND price >= $%d", paramIdx)
		args = append(args, minPrice)
		paramIdx++
	}
	if maxPrice > 0 {
		query += fmt.Sprintf(" AND price <= $%d", paramIdx)
		args = append(args, maxPrice)
		paramIdx++
	}
	query += " ORDER BY price ASC"

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ProductID, &p.Name, &p.CategoryID, &p.Price); err != nil {
			return nil, err
		}
		products = append(products, p)
	}
	return products, nil
}

func (s *SQLService) GetProductsByPriceRange(ctx context.Context, minPrice, maxPrice float64) ([]Product, error) {
	query := `
		SELECT product_id, name, category_id, price
		FROM products
		WHERE 1=1
	`
	var args []interface{}
	paramIdx := 1

	if minPrice > 0 {
		query += fmt.Sprintf(" AND price >= $%d", paramIdx)
		args = append(args, minPrice)
		paramIdx++
	}
	if maxPrice > 0 {
		query += fmt.Sprintf(" AND price <= $%d", paramIdx)
		args = append(args, maxPrice)
		paramIdx++
	}
	query += " ORDER BY price ASC"

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ProductID, &p.Name, &p.CategoryID, &p.Price); err != nil {
			return nil, err
		}
		products = append(products, p)
	}
	return products, nil
}

func (s *SQLService) GetRecommendations(ctx context.Context, userID int, limit int64) ([]Product, error) {
	query := `
		WITH target_user_products AS (
			SELECT DISTINCT product_id
			FROM order_items oi
			JOIN orders o ON oi.order_id = o.order_id
			WHERE o.user_id = $1
		),
		similar_users AS (
			SELECT DISTINCT o.user_id
			FROM orders o
			JOIN order_items oi ON o.order_id = oi.order_id
			WHERE oi.product_id IN (SELECT product_id FROM target_user_products)
			  AND o.user_id != $1
		),
		recommended_products AS (
			SELECT DISTINCT oi.product_id
			FROM order_items oi
			JOIN orders o ON oi.order_id = o.order_id
			WHERE o.user_id IN (SELECT user_id FROM similar_users)
			  AND oi.product_id NOT IN (SELECT product_id FROM target_user_products)
		)
		SELECT p.product_id, p.name, p.category_id, p.price
		FROM products p
		JOIN recommended_products rp ON p.product_id = rp.product_id
		ORDER BY p.product_id ASC
		LIMIT $2
	`
	rows, err := s.pool.Query(ctx, query, userID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ProductID, &p.Name, &p.CategoryID, &p.Price); err != nil {
			return nil, err
		}
		products = append(products, p)
	}
	return products, nil
}
