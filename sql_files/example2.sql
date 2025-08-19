SELECT u.id, u.name, (
    SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id AND o.status = 'completed'
) AS completed_orders
FROM users u
WHERE u.signup_date > '2025-01-01';

SELECT user_id, order_id, order_date,
       SUM(quantity) OVER (PARTITION BY user_id ORDER BY order_date) AS running_total
FROM orders;

WITH recent_orders AS (
    SELECT * FROM orders WHERE order_date > '2025-07-01'
)
SELECT u.name, ro.order_id, p.product_name
FROM users u
JOIN recent_orders ro ON u.id = ro.user_id
JOIN products p ON ro.product_id = p.product_id;

SELECT * FROM users WHERE active = 1;

SELECT COUNT(*) FROM orders WHERE status = 'pending';