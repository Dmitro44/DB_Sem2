# LR4: Миграция SQL в Redis

## 1. Основные Use Cases

### Use Case 1: Получение данных пользователя
**SQL операции:** `SELECT`, `WHERE`
```sql
SELECT user_id, name, email, created_at 
FROM users 
WHERE user_id = ?
```

### Use Case 2: Последние заказы
**SQL операции:** `SELECT`, `ORDER BY`, `LIMIT`
```sql
SELECT order_id, user_id, created_at, status 
FROM orders 
ORDER BY created_at DESC 
LIMIT 10
```

### Use Case 3: Товары по категории
**SQL операции:** `SELECT`, `JOIN`, `WHERE`
```sql
SELECT p.product_id, p.name, p.price 
FROM products p 
JOIN categories c ON p.category_id = c.category_id 
WHERE c.category_id = ?
```

### Use Case 4: Топ товаров по объему продаж
**SQL операции:** `SELECT`, `JOIN`, `GROUP BY`, `ORDER BY`, `LIMIT`
```sql
SELECT p.product_id, p.name, SUM(oi.quantity) as total_sold 
FROM products p 
JOIN order_items oi ON p.product_id = oi.product_id 
GROUP BY p.product_id, p.name 
ORDER BY total_sold DESC 
LIMIT 10
```

### Use Case 5: Топ товаров по выручке
**SQL операции:** `SELECT`, `JOIN`, `GROUP BY`, `ORDER BY`, `LIMIT`
```sql
SELECT p.product_id, p.name, SUM(oi.quantity * oi.price) as revenue 
FROM products p 
JOIN order_items oi ON p.product_id = oi.product_id 
GROUP BY p.product_id, p.name 
ORDER BY revenue DESC 
LIMIT 10
```

### Use Case 6: История заказов пользователя
**SQL операции:** `SELECT`, `WHERE`, `ORDER BY`
```sql
SELECT order_id, created_at, status 
FROM orders 
WHERE user_id = ? 
ORDER BY created_at DESC
```

### Use Case 7: Детали заказа с позициями
**SQL операции:** `SELECT`, `JOIN`, `WHERE`
```sql
SELECT o.order_id, o.status, oi.product_id, oi.quantity, oi.price 
FROM orders o 
JOIN order_items oi ON o.order_id = oi.order_id 
WHERE o.order_id = ?
```

---

## 2. Структура данных Redis

### Спроектированные ключи и типы данных

| Ключ | Тип | Назначение |
|------|-----|------------|
| `user:{id}` | **Hash** | Данные пользователя (id, name, email, created_at) |
| `user:{id}:orders` | **List** | Заказы пользователя (последние в начале) |
| `category:{id}` | **Hash** | Данные категории (id, name) |
| `category:{id}:products` | **Set** | ID товаров в категории |
| `product:{id}` | **Hash** | Данные товара (id, name, category_id, price) |
| `products:by_price` | **Sorted Set** | Товары, отсортированные по цене |
| `products:by_sales` | **Sorted Set** | Товары, отсортированные по объему продаж |
| `products:by_revenue` | **Sorted Set** | Товары, отсортированные по выручке |
| `order:{id}` | **Hash** | Данные заказа (id, user_id, created_at, status) |
| `order:{id}:items` | **Set** | ID позиций в заказе |
| `orders:by_time` | **Sorted Set** | Заказы, отсортированные по времени создания |
| `order_item:{id}` | **Hash** | Данные позиции заказа (id, order_id, product_id, quantity, price) |

---

## 3. Таблица соответствий SQL → Redis

| SQL запрос/таблица | Структура Redis | Ключ(и) | Тип данных |
|--------------------|-----------------|---------|------------|
| **users** | Хранение сущностей | `user:{id}` | Hash |
| `SELECT * FROM users WHERE user_id = ?` | Прямой доступ | `user:{id}` | Hash |
| **categories** | Хранение сущностей | `category:{id}` | Hash |
| **products** | Хранение сущностей | `product:{id}` | Hash |
| `SELECT * FROM products WHERE category_id = ?` | Индекс по категориям | `category:{id}:products` → `product:{id}` | Set + Hash |
| `SELECT * FROM products ORDER BY price` | Индекс по цене | `products:by_price` (ZRANGE) | Sorted Set |
| **orders** | Хранение сущностей | `order:{id}` | Hash |
| `SELECT * FROM orders WHERE user_id = ?` | Связь user-orders | `user:{id}:orders` → `order:{id}` | List + Hash |
| `SELECT * FROM orders ORDER BY created_at DESC LIMIT N` | Индекс по времени | `orders:by_time` (ZREVRANGE) | Sorted Set |
| **order_items** | Хранение сущностей | `order_item:{id}` | Hash |
| `SELECT * FROM order_items WHERE order_id = ?` | Связь order-items | `order:{id}:items` → `order_item:{id}` | Set + Hash |
| `SELECT product_id, SUM(quantity) ... GROUP BY ... ORDER BY ... DESC` | Агрегат по продажам | `products:by_sales` (ZREVRANGE) | Sorted Set |
| `SELECT product_id, SUM(quantity * price) ... GROUP BY ... ORDER BY ... DESC` | Агрегат по выручке | `products:by_revenue` (ZREVRANGE) | Sorted Set |

### Паттерны трансляции запросов

**SQL JOIN + WHERE:**
```
SQL:   JOIN products p ON c.category_id = p.category_id WHERE c.category_id = 5
Redis: SMEMBERS category:5:products → HGETALL product:{id} для каждого
```

**SQL GROUP BY + ORDER BY:**
```
SQL:   GROUP BY product_id ORDER BY SUM(quantity) DESC LIMIT 10
Redis: ZREVRANGE products:by_sales 0 9 WITHSCORES
```

**SQL ORDER BY + LIMIT:**
```
SQL:   ORDER BY created_at DESC LIMIT 5
Redis: ZREVRANGE orders:by_time 0 4
```

**SQL вложенный запрос:**
```
SQL:   SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC
Redis: LRANGE user:{id}:orders 0 -1 → HGETALL order:{id} для каждого
```
