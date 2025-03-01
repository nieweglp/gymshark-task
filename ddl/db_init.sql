CREATE SCHEMA IF NOT EXISTS `gymshark.data_us`
OPTIONS(location="US");

-- Orders Table
CREATE TABLE `gymshark.data_us.order` (
    order_id STRING,  -- UUID, PK
    customer_id STRING,  -- UUID
    order_date DATE,
    status STRING,  -- ENUM(pending, processing, shipped, delivered)
    total_amount FLOAT64,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    batch_id STRING
)
PARTITION BY DATE(order_date)
CLUSTER BY customer_id;

-- Items Table
CREATE TABLE `gymshark.data_us.item` (
    item_id STRING, -- UUID, PK
    product_id STRING,  -- UUID
    order_id STRING,  -- UUID, orders FK
    product_name STRING,
    quantity INT64,
    price FLOAT64,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    batch_id STRING
)
CLUSTER BY order_id;

-- Shipping Address Table
CREATE TABLE `gymshark.data_us.shipping_address` (
    shipping_address_id STRING, -- UUID, PK
    order_id STRING,  -- UUID, orders FK
    street STRING,
    city STRING,
    country STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    batch_id STRING
)
CLUSTER BY order_id;

-- Inventory Table
CREATE TABLE `gymshark.data_us.inventory` (
    inventory_id STRING,  -- UUID, PK
    product_id STRING,  -- UUID, items FK
    warehouse_id STRING,  -- UUID
    quantity_change INT64,
    reason STRING,  -- ENUM(restock, sale, return, damage)
    timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    batch_id STRING
)
PARTITION BY DATE(timestamp)
CLUSTER BY product_id, warehouse_id;

-- User Activity Table
CREATE TABLE `gymshark.data_us.user_activity` (
    user_activity_id STRING, -- UUID, PK
    user_id STRING,  -- UUID
    session_id STRING,  -- UUID
    activity_type STRING,  -- ENUM(login, logout, view_product, add_to_cart, remove_from_cart)
    ip_address STRING,
    user_agent STRING,
    platform STRING,  -- ENUM(web, mobile, tablet)
    timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    batch_id STRING
)
PARTITION BY DATE(timestamp)
CLUSTER BY user_id, session_id;

-- User Customer bridge table
CREATE TABLE `gymshark.data_us.user_customer_bridge` (
    user_customer_bridge_id STRING, -- UUID, PK
    user_id STRING, -- UUID
    customer_id STRING, -- UUID
)
CLUSTER BY user_id, customer_id;
