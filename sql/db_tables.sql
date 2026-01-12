CREATE DATABASE IF NOT EXISTS merchant_intelligence;

CREATE TABLE IF NOT EXISTS merchant_intelligence.events (
    date DATE,
    product_id INT PRIMARY KEY,
    addtocart INT,
    transaction INT,
    view INT,
    impressions INT,
    clicks INT,
    ctr FLOAT
);