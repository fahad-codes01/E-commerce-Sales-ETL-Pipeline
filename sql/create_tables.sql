-- =============================================
-- E-commerce Sales Data Pipeline
-- Create orders table for cleaned data
-- =============================================

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id        VARCHAR(20)     PRIMARY KEY,
    order_date      DATE            NOT NULL,
    customer_id     VARCHAR(20)     NOT NULL,
    product         VARCHAR(100)    NOT NULL,
    category        VARCHAR(50)     NOT NULL,
    price           NUMERIC(10, 2)  NOT NULL,
    quantity        INTEGER         NOT NULL,
    total_price     NUMERIC(12, 2)  NOT NULL
);

-- Index for common analytical queries
CREATE INDEX idx_orders_date ON orders (order_date);
CREATE INDEX idx_orders_category ON orders (category);
CREATE INDEX idx_orders_customer ON orders (customer_id);
