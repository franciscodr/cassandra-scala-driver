CREATE KEYSPACE customer WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE customer.transaction (
account_id uuid,
order_at timestamp,
transaction_id uuid,
amount decimal,
payment_method text,
PRIMARY KEY (account_id, order_at, transaction_id)
) WITH CLUSTERING ORDER BY (order_at DESC, transaction_id ASC)