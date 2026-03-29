-- =============================================
-- E-commerce Sales Data Pipeline
-- Analytical Queries
-- =============================================

-- 1. Total Revenue
SELECT
    COUNT(*)            AS total_orders,
    SUM(total_price)    AS total_revenue,
    ROUND(AVG(total_price), 2) AS avg_order_value
FROM orders;

-- 2. Top 10 Products by Revenue
SELECT
    product,
    COUNT(*)            AS times_sold,
    SUM(quantity)        AS total_quantity,
    SUM(total_price)    AS total_revenue
FROM orders
GROUP BY product
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Monthly Revenue Trend
SELECT
    TO_CHAR(order_date, 'YYYY-MM')  AS month,
    COUNT(*)                        AS total_orders,
    SUM(total_price)                AS monthly_revenue
FROM orders
GROUP BY TO_CHAR(order_date, 'YYYY-MM')
ORDER BY month;

-- 4. Revenue by Category
SELECT
    category,
    COUNT(*)            AS total_orders,
    SUM(total_price)    AS total_revenue,
    ROUND(AVG(price), 2) AS avg_price
FROM orders
GROUP BY category
ORDER BY total_revenue DESC;

-- 5. Top 10 Customers by Spending
SELECT
    customer_id,
    COUNT(*)            AS total_orders,
    SUM(total_price)    AS total_spent
FROM orders
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;

-- 6. Daily Order Count (Last 30 Days)
SELECT
    order_date,
    COUNT(*)        AS daily_orders,
    SUM(total_price) AS daily_revenue
FROM orders
WHERE order_date >= (SELECT MAX(order_date) - INTERVAL '30 days' FROM orders)
GROUP BY order_date
ORDER BY order_date;
