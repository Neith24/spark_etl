
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(255) PRIMARY KEY,
    favourite_product VARCHAR(255),
    longest_streak INTEGER
);