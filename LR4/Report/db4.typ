#import "lib/stp2024.typ"
#show: stp2024.template

#include "lab_title.typ"

#pagebreak()

= Индивидуальное задание

+ #[Реализовать миграцию данных из формата #emph("CSV") в хранилище #emph("Redis"). Необходимо спроектировать структуру ключей и выбрать подходящие типы данных для эффективного доступа.]
+ #[Реализовать бизнес-логику для получения последних заказов пользователя с отображением связанных позиций заказа и данных о товарах.]
+ #[Реализовать получение топа товаров по объему продаж и по выручке. Внедрить механизм кэширования результатов с заданным временем жизни (#emph("TTL")).]
+ #[Реализовать фильтрацию товаров по категориям и диапазону цен с использованием упорядоченных множеств (#emph("Sorted Sets")).]
+ #[Разработать систему рекомендаций на основе поиска похожих пользователей, используя теоретико-множественные операции (#emph("SINTER"), #emph("SUNION"), #emph("SDIFF")).]
+ #[Реализовать аналогичные запросы в реляционной СУБД (#emph("PostgreSQL")) для демонстрации различий между подходами к обработке данных.]

= Краткие теоретические сведения

== Хранилище Redis

#emph("Redis") (#emph("Remote Dictionary Server")) представляет собой высокопроизводительную нереляционную систему управления базами данных класса #emph("Key-Value"), работающую в оперативной памяти. Основной особенностью системы является поддержка сложных структур данных и атомарность операций над ними.

== Типы данных

В ходе работы использовались следующие типы данных:

- #emph("Hash") — ассоциативный массив, применяемый для хранения объектов (пользователи, товары, заказы). Позволяет обращаться к отдельным полям объекта без необходимости чтения всей структуры;
- #emph("Set") — неупорядоченная коллекция уникальных строк. Эффективно используется для хранения связей (например, позиции в заказе) и выполнения операций пересечения или объединения;
- #emph("Sorted Set") (#emph("ZSet")) — коллекция уникальных элементов, где каждый элемент ассоциирован с числовым весом (#emph("score")). Позволяет выполнять выборку по диапазону весов и получать элементы в отсортированном виде;
- #emph("List") — упорядоченный список строк. Используется для хранения хронологических данных, таких как история заказов пользователя.

== Стратегии кэширования

Кэширование применяется для снижения нагрузки на систему при выполнении ресурсоемких агрегационных запросов. В работе реализована стратегия #emph("Cache Aside"), где данные сначала запрашиваются из кэша, а при их отсутствии — вычисляются и сохраняются в кэш с установкой времени актуальности (#emph("TTL")).

= Выполнение работы

== Соглашение об именовании ключей

Для обеспечения структурированности данных в плоском пространстве ключей использован префиксный подход с разделением двоеточиями:

- #emph("user:{id}") — основные данные пользователя;
- #emph("user:{id}:orders") — список идентификаторов заказов пользователя;
- #emph("product:{id}") — параметры товара;
- #emph("order:{id}") — заголовок заказа;
- #emph("order:{id}:items") — множество позиций в заказе.

== Процесс миграции данных

Миграция осуществлялась путем последовательного чтения файлов #emph("CSV") и записи данных в соответствующие структуры #emph("Redis"). Для ускорения доступа к агрегатам при загрузке данных формировались дополнительные индексы в виде упорядоченных множеств (например, #emph("products:by_price")).

#stp2024.listing[Миграция данных о товарах и создание индексов по цене][
  ```
  // Создание Hash для товара
  rdb.HSet(ctx, fmt.Sprintf("product:%s", productID), map[string]any{
      "product_id":  productID,
      "name":        row[1],
      "category_id": categoryID,
      "price":       price,
  })

  // Добавление в индекс по цене (Sorted Set)
  rdb.ZAdd(ctx, "products:by_price", redis.Z{
      Score:  priceFloat,
      Member: productID,
  })
  ```
]

== Получение последних заказов

Метод #emph("GetRecentOrders") выполняет сопоставление связанных сущностей. Сначала извлекаются идентификаторы заказов из списка пользователя (#emph("List")), затем для каждого заказа запрашиваются его атрибуты из #emph("Hash"), позиции из множества (#emph("Set")) и детальная информация о товарах.

#stp2024.listing[GetRecentOrders в Redis][
  ```
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
  ```
]

Аналогичный запрос в PostgreSQL:

#stp2024.listing[GetRecentOrders в PostgreSQL][
  ```
  SELECT order_id, user_id, created_at, status
  FROM orders
  WHERE user_id = $1
  ORDER BY created_at DESC
  LIMIT $2
  ```
]

== Кэширование популярных товаров

Метод #emph("GetTopProducts") возвращает список наиболее продаваемых товаров. Для оптимизации используется строковый ключ с параметрами запроса. Если данные присутствуют в кэше, они возвращаются немедленно. В противном случае выполняется выборка из #emph("ZSet") (#emph("ZREVRANGE")), данные сериализуются в #emph("JSON") и сохраняются в кэш на 60 секунд.

#stp2024.listing[Реализация кэширования с TTL][
  ```
  cached, err := s.rdb.Get(ctx, cacheKey).Result()
  if err == nil {
      s.CacheHits++
      json.Unmarshal([]byte(cached), &stats)
      return stats, nil
  }

  // Выборка из индекса при промахе кэша
  results, _ := s.rdb.ZRevRangeWithScores(ctx, zsetKey, 0, n-1).Result()
  // ... формирование результата ...

  data, _ := json.Marshal(stats)
  s.rdb.Set(ctx, cacheKey, data, 60*time.Second)
  ```
]

== Фильтрация товаров по категориям

Для реализации функции #emph("GetProductsByCategory") использовались упорядоченные множества, где ключом выступает идентификатор категории, а весом — цена товара. Это позволяет эффективно ограничивать выборку диапазоном цен с помощью команды #emph("ZRANGEBYSCORE").

#stp2024.listing[GetProductsByCategory в Redis][
  ```
  productIDs, _ := s.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
      Key:     fmt.Sprintf("category:%d:products", categoryID),
      ByScore: true,
      Start:   minPrice,
      Stop:    maxPrice,
  }).Result()
  ```
]

Аналогичный запрос в PostgreSQL:

#stp2024.listing[GetProductsByCategory в PostgreSQL][
  ```
  SELECT product_id, name, category_id, price
  FROM products
  WHERE category_id = $1
    AND price >= $2
    AND price <= $3
  ORDER BY price ASC
  ```
]

== Система рекомендаций похожим пользователям

Алгоритм рекомендаций основан на анализе покупок. Процесс включает следующие этапы:

+ #[Поиск множества товаров, купленных текущим пользователем.]
+ #[Поиск других пользователей, имеющих пересечение по купленным товарам (#emph("SINTER")).]
+ #[Объединение всех товаров, купленных этими пользователями (#emph("SUNION")).]
+ #[Исключение товаров, которые текущий пользователь уже приобрел (#emph("SDIFF")).]

#stp2024.listing[Выполнение теоретико-множественных операций для рекомендаций][
  ```
  // Поиск пересечения (похожие пользователи)
  commonProducts, _ := s.rdb.SInter(ctx, userPurchasedKey, otherUserKey).Result()

  // Объединение товаров от похожих пользователей
  allSimilarProducts, _ := s.rdb.SUnion(ctx, similarUserKeys...).Result()

  // Вычитание уже купленных товаров
  recommendations, _ := s.rdb.SDiff(ctx, tempKey, userPurchasedKey).Result()
  ```
]

Аналогичный запрос в PostgreSQL с использованием CTE:

#stp2024.listing[Рекомендации в PostgreSQL][
  ```
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
  LIMIT $2
  ```
]

#pagebreak()

#stp2024.heading_unnumbered[Вывод]

В ходе выполнения лабораторной работы были изучены принципы работы с высокопроизводительным хранилищем данных #emph("Redis"). Были приобретены следующие навыки:

- моделирование данных с использованием различных структур #emph("Redis") (#emph("Hash"), #emph("Set"), #emph("Sorted Set"), #emph("List")) для эффективного решения бизнес-задач;
- проектирование систем индексации в нереляционных базах данных для поддержки сложных выборок и фильтрации;
- реализация механизмов кэширования с управлением временем жизни данных для оптимизации производительности приложения;
- применение теоретико-множественных операций для построения аналитических систем и механизмов рекомендаций.

Разработанное решение демонстрирует преимущества использования #emph("In-Memory") хранилищ для задач, требующих минимального времени отклика и высокой масштабируемости при обработке типичных паттернов доступа к данным.
