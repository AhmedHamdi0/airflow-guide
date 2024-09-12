-- CREATE TABLE IF NOT EXISTS clean_store_transactions(STORE_ID varchar(50), STORE_LOCATION varchar(50), PRODUCT_CATEGORY varchar(50), PRODUCT_ID int, MRP float, CP float, DISCOUNT float, SP float, DATE date);

CREATE TABLE IF NOT EXISTS clean_store_transactions (
    STORE_ID VARCHAR(50),
    STORE_LOCATION VARCHAR(50),
    PRODUCT_CATEGORY VARCHAR(50),
    PRODUCT_ID INT,
    MRP NUMERIC,
    CP NUMERIC,
    DISCOUNT NUMERIC,
    SP NUMERIC,
    DATE DATE
);
