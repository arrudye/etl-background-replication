-- отключает вывод сообщений
SET client_min_messages TO warning;

CREATE TABLE customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    deleted_at  TIMESTAMP NULL
);

CREATE TABLE orders (
    id           SERIAL PRIMARY KEY,
    customer_id  INT REFERENCES customers(id) ON DELETE CASCADE,
    status       VARCHAR(50) DEFAULT 'pending',
    created_at   TIMESTAMP DEFAULT NOW(),
    updated_at   TIMESTAMP DEFAULT NOW(),
    deleted_at   TIMESTAMP NULL
);

-- таблицы для доп задания
CREATE TABLE products (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    price       NUMERIC(10, 2) NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    deleted_at  TIMESTAMP NULL
);

CREATE TABLE order_items (
    id          SERIAL PRIMARY KEY,
    order_id    INT REFERENCES orders(id) ON DELETE CASCADE,
    product_id  INT REFERENCES products(id) ON DELETE CASCADE,
    quantity    INT NOT NULL DEFAULT 1,
    price_at_time NUMERIC(10, 2) NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW(),
    deleted_at  TIMESTAMP NULL
);

CREATE INDEX idx_orders_updated ON orders(updated_at);
CREATE INDEX idx_customers_created ON customers(created_at);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_customers_deleted ON customers(deleted_at);
CREATE INDEX idx_orders_deleted ON orders(deleted_at);
CREATE INDEX idx_products_deleted ON products(deleted_at);

-- 1000 покупателей
INSERT INTO customers (name, email)
SELECT 
    'Customer_' || generate_series,
    'email_' || generate_series || '@email.com'
FROM generate_series(1, 1000);

-- 100 продуктов
INSERT INTO products (name, price)
SELECT 
    'Product_' || generate_series,
    (random() * 1000)::numeric(10,2)
FROM generate_series(1, 100);

-- по 300 заказов на каждого покупателя
INSERT INTO orders (customer_id, status)
SELECT 
    c.id,
    (ARRAY['pending', 'processing', 'shipped', 'completed', 'cancelled'])[1 + floor(random() * 5)::int]
FROM customers c
CROSS JOIN generate_series(1, 300);

-- от 1 до 5 товаров в каждом заказе
INSERT INTO order_items (order_id, product_id, quantity, price_at_time)
SELECT 
    o.id,
    p.id,
    (1 + floor(random() * 5))::int,
    p.price
FROM orders o
CROSS JOIN LATERAL (
    SELECT id, price 
    FROM products 
    ORDER BY random() 
    LIMIT (1 + floor(random() * 5)::int)
) p;

-- растягиваем время создания и обновления на последние 30 дней, чтобы не всё было NOW
UPDATE customers SET created_at = NOW() - (random() * interval '30 days');
UPDATE products SET created_at = NOW() - (random() * interval '30 days');
UPDATE orders SET 
    created_at = NOW() - (random() * interval '30 days'),
    updated_at = NOW() - (random() * interval '30 days');
UPDATE order_items SET 
    created_at = NOW() - (random() * interval '30 days');

-- помечаем некоторые записи как удаленные
UPDATE customers SET deleted_at = NOW() - (random() * interval '10 days') WHERE id % 10 = 0;
UPDATE products SET deleted_at = NOW() - (random() * interval '10 days') WHERE id % 8 = 0;
UPDATE orders SET deleted_at = NOW() - (random() * interval '10 days') WHERE id % 5 = 0;
UPDATE order_items SET deleted_at = NOW() - (random() * interval '10 days') WHERE id % 7 = 0;