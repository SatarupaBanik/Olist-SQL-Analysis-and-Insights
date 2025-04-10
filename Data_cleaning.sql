use e_commerce_olist;

-- Data cleaning: 

select * from olist_customers_dataset;

-- Checking for duplicates dor all columns

with duplicate_cte as(
select *, row_number() over ( partition by customer_id, customer_unique_id, customer_zip_code_prefix, customer_city,
customer_state) as row_num from olist_customers_dataset)

select * from duplicate_cte where row_num > 1;

-- Result: No duplicate dataset for Table: olist_customers_dataset

select * from olist_order_items_dataset;

with duplicate_cte_1 as(
select *, row_number() over ( partition by order_id, order_item_id, product_id, seller_id,
shipping_limit_date,price,freight_value) as row_num from olist_order_items_dataset)

select * from duplicate_cte_1 where row_num > 1;

-- Result: No duplicate values

select * from olist_order_payments_dataset;

with duplicate_cte_2 as(
select *, row_number() over ( partition by order_id, payment_sequential,payment_type,payment_installments ,
payment_value) as row_numb from olist_order_payments_dataset)

select * from  duplicate_cte_2 where row_numb > 1;

-- No duplicates.

select * from olist_order_reviews_dataset_cleaned;

 -- No duplicates
 
 
select * from cleaned_product_category_data;

-- No duplicates can be observed after checking the table manually as dataset is small.

select * from olist_sellers_dataset;

with duplicate_cte_4 as(
select *, row_number() over ( partition by seller_id, seller_zip_code_prefix,seller_city ,
seller_state) as row_numb from olist_sellers_dataset)

select * from  duplicate_cte_4 where row_numb > 1;

-- No duplicates

with duplicate_cte_5 as(
select *, row_number() over ( partition by product_id, product_category_name,product_name_lenght ,product_description_lenght,
product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm) as row_numb from olist_products_dataset)

select * from  duplicate_cte_5 where row_numb > 1;

-- No duplicates


select * from  olist_orders_dataset;


WITH duplicate_cte_6 AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, customer_id, order_status,
                            order_purchase_timestamp, order_approved_at,
                            order_delivered_carrier_date, order_delivered_customer_date,
                            order_estimated_delivery_date
               ORDER BY order_id
           ) AS row_num
    FROM olist_orders_dataset
)
SELECT * FROM duplicate_cte_6
WHERE row_num > 1 ;

-- No duplicates

-- We have all the tables and all the columns, and no duplicate value has been observed.


show tables from e_commerce_olist;

-- Standardise data
select * from olist_customers_dataset;
select * from olist_order_items_dataset;
select * from olist_order_payments_dataset;
select * from olist_order_reviews_dataset_cleaned;
select * from olist_orders_dataset;
select * from olist_products_dataset;
select * from olist_sellers_dataset;
select * from cleaned_product_category_data;


-- Updating customer_city (capitalize first letter only) from table olist_customers_dataset

UPDATE olist_customers_dataset
SET customer_city = CONCAT(UCASE(LEFT(customer_city, 1)), LCASE(SUBSTRING(customer_city, 2)));

-- Changing the data types of olist_order_items_dataset
ALTER TABLE olist_order_items_dataset
MODIFY COLUMN order_id TEXT,
MODIFY COLUMN order_item_id INT,
MODIFY COLUMN product_id TEXT,
MODIFY COLUMN seller_id TEXT,
MODIFY COLUMN shipping_limit_date DATETIME,
MODIFY COLUMN price DOUBLE,
MODIFY COLUMN freight_value DOUBLE;

SELECT DISTINCT shipping_limit_date
FROM olist_order_items_dataset
WHERE STR_TO_DATE(shipping_limit_date, '%Y-%m-%d %H:%i:%s') IS NULL;

-- MySQL doesn't auto-convert string to DATETIME on MODIFY COLUMN. 
-- So we need to create a new column, copy and convert the values, then drop the old one.

-- Step 1: Add a new column for cleaned datetime
ALTER TABLE olist_order_items_dataset
ADD COLUMN shipping_limit_date_cleaned DATETIME;

-- Step 2: Populate it with converted values
UPDATE olist_order_items_dataset
SET shipping_limit_date_cleaned = STR_TO_DATE(shipping_limit_date, '%Y-%m-%d %H:%i:%s');

-- Step 3: Drop the old column
ALTER TABLE olist_order_items_dataset
DROP COLUMN shipping_limit_date;

-- Step 4: Rename new column to original name
ALTER TABLE olist_order_items_dataset
CHANGE shipping_limit_date_cleaned shipping_limit_date DATETIME;


-- Capitalise first letter from product_category_name in table olist_products_dataset
UPDATE olist_products_dataset
SET product_category_name = CONCAT(
    UPPER(LEFT(product_category_name, 1)),
    LOWER(SUBSTRING(product_category_name, 2))
);

-- Capitalise first letter from seller_city in table olist_sellers_dataset
UPDATE olist_sellers_dataset
SET seller_city = CONCAT(
    UPPER(LEFT(seller_city, 1)),
    LOWER(SUBSTRING(seller_city, 2))
);

-- Capitalise first letter from product_category_name,product_category_name_english in table cleaned_product_category_data
UPDATE cleaned_product_category_data
SET 
    product_category_name = CONCAT(
        UPPER(LEFT(product_category_name, 1)),
        LOWER(SUBSTRING(product_category_name, 2))
    ),
    product_category_name_english = CONCAT(
        UPPER(LEFT(product_category_name_english, 1)),
        LOWER(SUBSTRING(product_category_name_english, 2))
    );
-- Standardisation is being completed, now checking for null values:

-- Olist Customer Dataset
SELECT 
  COUNT(*) AS total_rows,
  SUM(customer_id IS NULL) AS null_customer_id,
  SUM(customer_unique_id IS NULL) AS null_customer_unique_id,
  SUM(customer_zip_code_prefix IS NULL) AS null_zip,
  SUM(customer_city IS NULL) AS null_city,
  SUM(customer_state IS NULL) AS null_state
FROM olist_customers_dataset;
-- Result : No null

-- olist_orders_dataset
SELECT 
  COUNT(*) AS total_rows,
  SUM(order_id IS NULL) AS null_order_id,
  SUM(customer_id IS NULL) AS null_customer_id,
  SUM(order_status IS NULL) AS null_order_status,
  SUM(order_purchase_timestamp IS NULL) AS null_purchase_time,
  SUM(order_approved_at IS NULL) AS null_approved_at,
  SUM(order_delivered_carrier_date IS NULL) AS null_delivered_carrier_date,
  SUM(order_delivered_customer_date IS NULL) AS null_delivered_customer_date,
  SUM(order_estimated_delivery_date IS NULL) AS null_estimated_delivery_date
FROM olist_orders_dataset;
-- Result : No null

-- olist_order_items_dataset
SELECT 
  COUNT(*) AS total_rows,
  SUM(order_id IS NULL) AS null_order_id,
  SUM(order_item_id IS NULL) AS null_item_id,
  SUM(product_id IS NULL) AS null_product_id,
  SUM(seller_id IS NULL) AS null_seller_id,
  SUM(shipping_limit_date IS NULL) AS null_shipping_limit_date,
  SUM(price IS NULL) AS null_price,
  SUM(freight_value IS NULL) AS null_freight
FROM olist_order_items_dataset;
-- Result : No null

-- olist_products_dataset

SELECT 
  COUNT(*) AS total_rows,
  SUM(product_id IS NULL) AS null_product_id,
  SUM(product_category_name IS NULL) AS null_category,
  SUM(product_name_lenght IS NULL) AS null_name_length,
  SUM(product_description_lenght IS NULL) AS null_desc_length,
  SUM(product_photos_qty IS NULL) AS null_photos,
  SUM(product_weight_g IS NULL) AS null_weight,
  SUM(product_length_cm IS NULL) AS null_length,
  SUM(product_height_cm IS NULL) AS null_height,
  SUM(product_width_cm IS NULL) AS null_width
FROM olist_products_dataset;
 -- result : No null
 
 -- olist_sellers_dataset
 SELECT 
  COUNT(*) AS total_rows,
  SUM(seller_id IS NULL) AS null_seller_id,
  SUM(seller_zip_code_prefix IS NULL) AS null_zip,
  SUM(seller_city IS NULL) AS null_city,
  SUM(seller_state IS NULL) AS null_state
FROM olist_sellers_dataset;
 -- result : No null
 
-- olist_order_reviews_dataset
SELECT 
  COUNT(*) AS total_rows,
  SUM(review_id IS NULL) AS null_review_id,
  SUM(order_id IS NULL) AS null_order_id,
  SUM(review_score IS NULL) AS null_score,
  SUM(review_comment_title IS NULL) AS null_title,
  SUM(review_comment_message IS NULL) AS null_message,
  SUM(review_creation_date IS NULL) AS null_created,
  SUM(review_answer_timestamp IS NULL) AS null_answered
FROM olist_order_reviews_dataset_cleaned;
 -- result : No null
 
 -- olist_order_payments_dataset

SELECT 
  COUNT(*) AS total_rows,
  SUM(order_id IS NULL) AS null_order_id,
  SUM(payment_sequential IS NULL) AS null_seq,
  SUM(payment_type IS NULL) AS null_type,
  SUM(payment_installments IS NULL) AS null_installments,
  SUM(payment_value IS NULL) AS null_value
FROM olist_order_payments_dataset;
 -- result : No null
 
 -- cleaned_product_category_data has no null value as well

-- data cleaning has been successfully done.
-- Outcomes: No duplicates and null observed
-- Some columns in 8 tables has been successfully standardized.

