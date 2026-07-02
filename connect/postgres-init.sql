-- Seed data for the Lab 4 JDBC source connector (connect profile).
-- The connector reads the `orders` table incrementally using id + updated_at.
CREATE TABLE IF NOT EXISTS orders (
    id         SERIAL PRIMARY KEY,
    customer   TEXT           NOT NULL,
    amount     NUMERIC(10,2)  NOT NULL,
    updated_at TIMESTAMP      NOT NULL DEFAULT now()
);

INSERT INTO orders (customer, amount) VALUES
    ('alice', 99.99),
    ('bob',   249.50),
    ('carol', 19.99),
    ('dave',  5.00);
