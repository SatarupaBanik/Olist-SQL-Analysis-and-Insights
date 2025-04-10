use e_commerce_olist;

-- Total monthly revenue:
SELECT 
  DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS month,
  ROUND(SUM(p.payment_value), 2) AS total_revenue
FROM olist_orders_dataset o
JOIN olist_order_payments_dataset p ON o.order_id = p.order_id
GROUP BY month
ORDER BY month;

-- Product categories Which generate the most revenue.
SELECT 
  t.product_category_name_english,
  ROUND(SUM(oi.price), 2) AS revenue
FROM olist_order_items_dataset oi
JOIN olist_products_dataset p ON oi.product_id = p.product_id
JOIN cleaned_product_category_data t ON p.product_category_name = t.product_category_name
GROUP BY t.product_category_name_english
ORDER BY revenue DESC
LIMIT 10;

-- Top 5 highest paying customer
SELECT 
  c.customer_unique_id,
  ROUND(SUM(pay.payment_value), 2) AS total_spent
FROM olist_orders_dataset o
JOIN olist_order_payments_dataset pay ON o.order_id = pay.order_id
JOIN olist_customers_dataset c ON o.customer_id = c.customer_id
GROUP BY c.customer_unique_id
ORDER BY total_spent DESC
LIMIT 5;

-- States with most customer
SELECT 
  customer_state,
  COUNT(*) AS total_customers
FROM olist_customers_dataset
GROUP BY customer_state
ORDER BY total_customers DESC;

-- Top 10 most sold product categories
SELECT 
  t.product_category_name_english,
  COUNT(*) AS total_sold
FROM olist_order_items_dataset oi
JOIN olist_products_dataset p ON oi.product_id = p.product_id
JOIN cleaned_product_category_data t ON p.product_category_name = t.product_category_name
GROUP BY t.product_category_name_english
ORDER BY total_sold DESC
LIMIT 10;

-- Sellers with most orders
SELECT 
  seller_id,
  COUNT(DISTINCT order_id) AS total_orders
FROM olist_order_items_dataset
GROUP BY seller_id
ORDER BY total_orders DESC
LIMIT 10;

-- customers who pays more than the average
SELECT 
  customer_id,
  total_spent
FROM (
    SELECT 
      o.customer_id,
      SUM(p.payment_value) AS total_spent
    FROM olist_orders_dataset o
    JOIN olist_order_payments_dataset p ON o.order_id = p.order_id
    GROUP BY o.customer_id
) AS customer_totals
WHERE total_spent > (
    SELECT AVG(payment_value)
    FROM olist_order_payments_dataset
);

-- Number of products, avg price, min/max per category
SELECT 
  p.product_category_name,
  COUNT(*) AS total_products,
  ROUND(AVG(oi.price), 2) AS avg_price,
  MIN(oi.price) AS min_price,
  MAX(oi.price) AS max_price
FROM olist_order_items_dataset oi
JOIN olist_products_dataset p ON oi.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY avg_price DESC;

-- Create View for Total Sales by Seller

CREATE OR REPLACE VIEW seller_sales AS
SELECT 
  s.seller_id,
  s.seller_city,
  s.seller_state,
  COUNT(oi.order_id) AS total_orders,
  ROUND(SUM(oi.price), 2) AS total_revenue
FROM olist_order_items_dataset oi
JOIN olist_sellers_dataset s ON oi.seller_id = s.seller_id
GROUP BY s.seller_id, s.seller_city, s.seller_state;

-- Top 10 sellers
SELECT * FROM seller_sales
ORDER BY total_revenue DESC
LIMIT 10;

-- Sellers with more than 10 orders
SELECT * FROM seller_sales
WHERE total_orders > 100
ORDER BY total_orders DESC;



