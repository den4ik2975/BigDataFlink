DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_product_category CASCADE;
DROP TABLE IF EXISTS dim_pet_category CASCADE;
DROP TABLE IF EXISTS dim_city CASCADE;
DROP TABLE IF EXISTS dim_postal_area CASCADE;
DROP TABLE IF EXISTS dim_country CASCADE;

CREATE TABLE dim_country (
    country_key TEXT PRIMARY KEY,
    country_name TEXT NOT NULL
);

CREATE TABLE dim_postal_area (
    postal_area_key TEXT PRIMARY KEY,
    country_key TEXT REFERENCES dim_country(country_key),
    postal_code TEXT
);

CREATE TABLE dim_city (
    city_key TEXT PRIMARY KEY,
    city_name TEXT,
    state_name TEXT,
    country_key TEXT REFERENCES dim_country(country_key)
);

CREATE TABLE dim_product_category (
    product_category_key TEXT PRIMARY KEY,
    product_category_name TEXT NOT NULL
);

CREATE TABLE dim_pet_category (
    pet_category_key TEXT PRIMARY KEY,
    pet_category_name TEXT NOT NULL
);

CREATE TABLE dim_customer (
    customer_key TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    postal_area_key TEXT REFERENCES dim_postal_area(postal_area_key),
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE dim_seller (
    seller_key TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    postal_area_key TEXT REFERENCES dim_postal_area(postal_area_key)
);

CREATE TABLE dim_supplier (
    supplier_key TEXT PRIMARY KEY,
    supplier_name TEXT,
    contact_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city_key TEXT REFERENCES dim_city(city_key)
);

CREATE TABLE dim_store (
    store_key TEXT PRIMARY KEY,
    store_name TEXT,
    store_location TEXT,
    city_key TEXT REFERENCES dim_city(city_key),
    phone TEXT,
    email TEXT
);

CREATE TABLE dim_product (
    product_key TEXT PRIMARY KEY,
    product_name TEXT,
    product_category_key TEXT REFERENCES dim_product_category(product_category_key),
    pet_category_key TEXT REFERENCES dim_pet_category(pet_category_key),
    supplier_key TEXT REFERENCES dim_supplier(supplier_key),
    product_price NUMERIC(12, 2),
    stock_quantity INTEGER,
    product_weight NUMERIC(10, 2),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(3, 2),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE
);

CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year_number INTEGER NOT NULL,
    quarter_number INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    day_number INTEGER NOT NULL
);

CREATE TABLE fact_sales (
    source_raw_id BIGINT PRIMARY KEY,
    source_file TEXT,
    source_file_number INTEGER,
    source_sale_id BIGINT,
    source_customer_id BIGINT,
    source_seller_id BIGINT,
    source_product_id BIGINT,
    customer_key TEXT REFERENCES dim_customer(customer_key),
    seller_key TEXT REFERENCES dim_seller(seller_key),
    product_key TEXT REFERENCES dim_product(product_key),
    store_key TEXT REFERENCES dim_store(store_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    sale_quantity INTEGER,
    sale_total_price NUMERIC(14, 2),
    unit_product_price NUMERIC(12, 2)
);

CREATE INDEX idx_fact_sales_date_key ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_product_key ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer_key ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_store_key ON fact_sales(store_key);
CREATE INDEX idx_fact_sales_seller_key ON fact_sales(seller_key);
