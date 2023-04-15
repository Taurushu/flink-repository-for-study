CREATE TABLE orders
(
    order_id      INT,
    order_date    TIMESTAMP(0),
    customer_name STRING,
    price         DECIMAL(10, 5),
    product_id    INT,
    order_status  BOOLEAN,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
      'connector' = 'mysql-cdc',
      'hostname' = '192.168.32.151',
      'port' = '3306',
      'username' = 'root',
      'password' = 'shujie',
      'database-name' = 'mydb',
      'table-name' = 'orders_1'
      );
