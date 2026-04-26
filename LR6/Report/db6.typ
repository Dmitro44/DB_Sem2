#import "lib/stp2024.typ"
#show: stp2024.template

#include "lab_title.typ"

#pagebreak()

= Индивидуальное задание

+ #[Реализовать миграцию данных из формата #emph("CSV") в хранилище #emph("MongoDB"). Спроектировать структуру коллекций и выбрать подходящие типы данных.]
+ #[Реализовать проверку целостности данных с использованием агрегационных #emph("pipeline").]
+ #[Реализовать получение заказов пользователя.]
+ #[Реализовать получение детальной информации о заказе с использованием #emph("$lookup") для соединения коллекций.]
+ #[Реализовать получение топа товаров по выручке с использованием группировки и сортировки.]
+ #[Провести проверку производительности запросов до и после создания индексов.]

= Краткие теоретические сведения

== Хранилище MongoDB

#emph("MongoDB") представляет собой документоориентированную систему управления базами данных класса #emph("NoSQL"). Основной особенностью является хранение данных в виде JSON-подобных документов в формате #emph("BSON"), что обеспечивает гибкость схемы и высокую масштабируемость.

== Агрегационные Pipeline

Для выполнения сложных запросов в MongoDB используется фреймворк агрегации. Основные стадии:

- #emph("$match") -- фильтрация документов (аналог WHERE в SQL);
- #emph("$group") -- группировка документов по ключу (аналог GROUP BY);
- #emph("$sort") -- сортировка результатов (аналог ORDER BY);
- #emph("$limit") -- ограничение количества результатов;
- #emph("$lookup") -- соединение с другой коллекцией (аналог JOIN);
- #emph("$unwind") -- разворачивание массива в отдельные документы;
- #emph("$project") -- выбор конкретных полей (аналог SELECT).

== Индексы

Индексы в MongoDB позволяют ускорить поиск документов. Применяются составные индексы, покрывающие несколько полей. Для анализа эффективности запросов используется метод #emph("explain()") с параметром #emph("executionStats").

= Выполнение работы

== Проектирование схемы данных

Для хранения данных использовались следующие коллекции с соответствующими полями:

- #emph("users") -- { \_id, name, email, createdAt };
- #emph("categories") -- { \_id, name };
- #emph("products") -- { \_id, name, categoryId, price };
- #emph("orders") -- { \_id, userId, createdAt, status };
- #emph("orderItems") -- { \_id, orderId, productId, quantity, price }.

Первичные ключи хранятся в поле #emph("_id") для оптимизации доступа.

== Миграция данных из CSV

Миграция осуществлялась путем последовательного чтения файлов CSV и записи документов в соответствующие коллекции. Числовые поля преобразовывались из строк в соответствующие типы (int, float64). Каждый документ содержит уникальный идентификатор в поле `_id`, который используется в качестве первичного ключа для быстрого доступа.

Ниже приведены примеры документов, которые создаются в коллекциях orders и orderItems в результате миграции.

#stp2024.listing[Пример миграции заказов][
  ```javascript
  db.orders.insertMany([
    { _id: 1, userId: 1, createdAt: "2024-01-15", status: "completed" },
    { _id: 2, userId: 1, createdAt: "2024-01-20", status: "pending" },
    { _id: 3, userId: 2, createdAt: "2024-01-22", status: "completed" }
  ])
  ```
]

#stp2024.listing[Пример миграции позиций заказов][
  ```javascript
  db.orderItems.insertMany([
    { _id: 1, orderId: 1, productId: 5, quantity: 2, price: 1000.00 },
    { _id: 2, orderId: 1, productId: 3, quantity: 1, price: 500.00 },
    { _id: 3, orderId: 2, productId: 7, quantity: 3, price: 300.00 }
  ])
  ```
]

== Проверка целостности данных

Для проверки ссылочной целостности используется агрегация с оператором #emph("$lookup"). Этот запрос позволяет найти заказы, в которых поле userId ссылается на несуществующего пользователя. Результат показывает количество заказов с невалидными ссылками на пользователей.

#stp2024.listing[Проверка userId в заказах][
  ```javascript
  db.orders.aggregate([
    { $lookup: {
        from: "users",
        localField: "userId",
        foreignField: "_id",
        as: "user_details"
      }},
      { $match: { user_details: { $size: 0 } }},
      { $count: "invalid_orders_count" }
    ])
  ```
]

Результат показывает количество заказов с невалидными ссылками на пользователей.

#stp2024.listing[Проверка общей стоимости заказа][
  ```javascript
  db.orders.aggregate([
    { $addFields: {
      calculated_total: {
        $sum: {
          $map: {
            input: "$items",
            as: "item",
            in: { $multiply: ["$$item.quantity", "$$item.price"] }
          }
        }
      }
    }},
    { $match: { calculated_total: { $gt: 1000 } }},
    { $count: "expensive_orders_count" }
  ])
  ```
]

== Получение заказов пользователя

Простой запрос для получения всех заказов конкретного пользователя. Фильтрация выполняется по полю userId, которое содержит идентификатор пользователя. Результаты не сортируются, порядок определяется внутренней структурой индекса.

#stp2024.listing[GetUserOrders - простой Find][
  ```javascript
  db.orders.find({ userId: 1 })
  ```
]

== Получение детальной информации о заказе

Для получения полной информации о заказе используется трехэтапное соединение коллекций. Сначала выбирается заказ, затем к нему присоединяются позиции заказа, и наконец к каждой позиции присоединяются данные о товаре. Операция #emph("$unwind") используется для разворачивания массива в отдельные документы, а #emph("$group") собирает результат обратно в один документ.

#stp2024.listing[GetOrdersWithDetails - 3-way JOIN][
  ```javascript
  db.orders.aggregate([
    // Этап 1: выбор заказа по ID
    { $match: { _id: 1 }},

    // Этап 2: JOIN с orderItems
    { $lookup: {
      from: "orderItems",
      localField: "_id",
      foreignField: "orderId",
      as: "items"
    }},

    // Этап 3: разворачивание массива
    { $unwind: { path: "$items", preserveNullAndEmptyArrays: true }},

    // Этап 4: JOIN с products
    { $lookup: {
      from: "products",
      localField: "items.productId",
      foreignField: "_id",
      as: "items.product_details"
    }},

    // Этап 5: разворачивание деталей продукта
    { $unwind: { path: "$items.product_details", preserveNullAndEmptyArrays: true }},

    // Этап 6: группировка обратно
    { $group: {
      _id: "$_id",
      userId: { $first: "$userId" },
      status: { $first: "$status" },
      createdAt: { $first: "$createdAt" },
      items: { $push: "$items" }
    }}
  ])
  ```
]

== Получение топа товаров по выручке

Этот запрос вычисляет общую выручку для каждого товара. Сначала выполняется группировка по productId с вычислением суммы quantity \* price. Затем результаты сортируются по убыванию выручки и ограничиваются топ-N позициями. Операция #emph("$lookup") присоединяет названия товаров из коллекции products.
\
\
\
#stp2024.listing[GetTopProductsByRevenue - GROUP BY][
  ```javascript
  db.orderItems.aggregate([
    // GROUP BY productId, SUM(quantity * price)
    { $group: {
      _id: "$productId",
      totalRevenue: { $sum: { $multiply: ["$quantity", "$price"] }},
      totalQuantity: { $sum: "$quantity" }
    }},

    // ORDER BY totalRevenue DESC
    { $sort: { totalRevenue: -1 }},

    // LIMIT N
    { $limit: 10},

    // JOIN для получения названий товаров
    { $lookup: {
      from: "products",
      localField: "_id",
      foreignField: "_id",
      as: "product_info"
    }},

    { $unwind: "$product_info" },

    // SELECT productId, name, revenue
    { $project: {
      productId: "$_id",
      name: "$product_info.name",
      revenue: "$totalRevenue",
      quantitySold: "$totalQuantity"
    }}
  ])
  ```
]

== Проверка производительности

Для демонстрации влияния индексов на производительность выполнялось сравнение времени выполнения запроса до и после создания индекса с использованием метода #emph("explain()").

#stp2024.listing[Создание составного индекса][
  ```javascript
  db.orders.createIndex({ userId: 1, createdAt: -1 })
  ```
]

#stp2024.listing[Получение статистики выполнения (MongoDB runCommand)][
  ```javascript
  db.runCommand({
    explain: {
      find: "orders",
      filter: { userId: 1 },
      sort: { createdAt: -1 }
    },
    verbosity: "executionStats"
  })
  ```
]

Результат содержит поля:
- #emph("totalDocsExamined") -- количество просмотренных документов;
- #emph("nReturned") -- количество возвращенных документов;
- #emph("executionTimeMillis") -- время выполнения в миллисекундах.

#pagebreak()

#stp2024.heading_unnumbered[Вывод]

В ходе выполнения лабораторной работы были изучены принципы работы с документоориентированной СУБД #emph("MongoDB"). Были приобретены следующие навыки:

- проектирование схемы данныx с использованием коллекций и полей #emph("_id") в качестве первичных ключей;
- реализация сложных запросов с использованием агрегационных #emph("pipeline") с операторами #emph("$lookup"), #emph("$group"), #emph("$unwind");
- выполнение соединений коллекций через #emph("$lookup") для имитации реляционных JOIN;
- создание индексов для оптимизации запросов и измерение производительности.

Разработанное решение демонстрирует возможности NoSQL-хранилища MongoDB для обработки сложных аналитических запросов с использованием агрегационных пайплайнов.
