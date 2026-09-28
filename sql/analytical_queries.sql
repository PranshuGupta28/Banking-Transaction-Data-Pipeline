-- =====================================================================
-- analytical_queries.sql
-- SQL-based banking analytics on the `banking_analytics` database.
-- Every query is preceded by a plain-language explanation of the
-- business question it answers.
--
-- Uses MySQL 8.0+ features: CTEs, window functions (RANK, ROW_NUMBER,
-- LAG, SUM/AVG OVER), CASE, GROUP BY, HAVING, subqueries.
-- =====================================================================

USE banking_analytics;

-- ---------------------------------------------------------------------
-- Q1. How many transactions are there in total?
-- A simple overall volume check on the fact table.
-- ---------------------------------------------------------------------
SELECT COUNT(*) AS total_transactions
FROM transactions;

-- ---------------------------------------------------------------------
-- Q2. What is the total value of all transactions?
-- Sums the amount column to show overall money movement.
-- ---------------------------------------------------------------------
SELECT SUM(amount) AS total_transaction_value
FROM transactions;

-- ---------------------------------------------------------------------
-- Q3. What is the average transaction amount?
-- ---------------------------------------------------------------------
SELECT AVG(amount) AS average_transaction_amount
FROM transactions;

-- ---------------------------------------------------------------------
-- Q4. How many transactions succeeded?
-- ---------------------------------------------------------------------
SELECT COUNT(*) AS successful_transactions
FROM transactions
WHERE transaction_status = 'Success';

-- ---------------------------------------------------------------------
-- Q5. How many transactions failed?
-- ---------------------------------------------------------------------
SELECT COUNT(*) AS failed_transactions
FROM transactions
WHERE transaction_status = 'Failed';

-- ---------------------------------------------------------------------
-- Q6. What is the overall transaction success rate?
-- Conditional aggregation computes each status share in one pass.
-- ---------------------------------------------------------------------
SELECT
    COUNT(*)                                                  AS total_transactions,
    SUM(CASE WHEN transaction_status = 'Success' THEN 1 ELSE 0 END) AS successful,
    SUM(CASE WHEN transaction_status = 'Failed'  THEN 1 ELSE 0 END) AS failed,
    ROUND(SUM(CASE WHEN transaction_status = 'Success' THEN 1 ELSE 0 END) * 100.0
          / COUNT(*), 2)                                       AS success_rate_pct
FROM transactions;

-- ---------------------------------------------------------------------
-- Q7. How many transactions happened per month?
-- Monthly volume trend (GROUP BY on the derived year/month columns).
-- ---------------------------------------------------------------------
SELECT
    transaction_year,
    transaction_month,
    COUNT(*) AS monthly_transactions
FROM transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year, transaction_month;

-- ---------------------------------------------------------------------
-- Q8. What is the total transaction value per month?
-- ---------------------------------------------------------------------
SELECT
    transaction_year,
    transaction_month,
    ROUND(SUM(amount), 2) AS monthly_transaction_value
FROM transactions
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year, transaction_month;

-- ---------------------------------------------------------------------
-- Q9. Which are the top 10 customers by total transaction value?
-- Joins to customers only to enrich the output (age/gender/context).
-- ---------------------------------------------------------------------
SELECT
    t.customer_id,
    c.customer_age,
    c.customer_gender,
    ROUND(SUM(t.amount), 2) AS total_value
FROM transactions t
JOIN customers c ON c.customer_id = t.customer_id
GROUP BY t.customer_id, c.customer_age, c.customer_gender
ORDER BY total_value DESC
LIMIT 10;

-- ---------------------------------------------------------------------
-- Q10. Which are the top 10 customers by transaction frequency?
-- ---------------------------------------------------------------------
SELECT
    customer_id,
    COUNT(*) AS transaction_count
FROM transactions
GROUP BY customer_id
ORDER BY transaction_count DESC
LIMIT 10;

-- ---------------------------------------------------------------------
-- Q11. Which branches process the most transaction value? (Top 10)
-- ---------------------------------------------------------------------
SELECT
    branch,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_value
FROM transactions
GROUP BY branch
ORDER BY total_value DESC
LIMIT 10;

-- ---------------------------------------------------------------------
-- Q12. Which payment methods are used the most?
-- ---------------------------------------------------------------------
SELECT
    payment_method,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_value
FROM transactions
GROUP BY payment_method
ORDER BY transaction_count DESC;

-- ---------------------------------------------------------------------
-- Q13. Which transaction channels are used the most?
-- ---------------------------------------------------------------------
SELECT
    channel,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_value,
    ROUND(SUM(CASE WHEN transaction_status = 'Failed' THEN 1 ELSE 0 END) * 100.0
          / COUNT(*), 2) AS failure_rate_pct
FROM transactions
GROUP BY channel
ORDER BY transaction_count DESC;

-- ---------------------------------------------------------------------
-- Q14. How does money flowing in (Credit) compare to money flowing out
-- (Debit/Withdrawal/Transfer)?
-- ---------------------------------------------------------------------
SELECT
    CASE
        WHEN transaction_type = 'Credit' THEN 'Money In (Credit)'
        ELSE 'Money Out (Debit/Withdrawal/Transfer)'
    END                              AS direction,
    transaction_type,
    COUNT(*)                         AS transaction_count,
    ROUND(SUM(amount), 2)            AS total_value
FROM transactions
GROUP BY direction, transaction_type
ORDER BY direction, total_value DESC;

-- ---------------------------------------------------------------------
-- Q15. Which cities generate the most transaction value?
-- ---------------------------------------------------------------------
SELECT
    city,
    COUNT(*)            AS transaction_count,
    ROUND(SUM(amount),2) AS total_value
FROM transactions
GROUP BY city
ORDER BY total_value DESC
LIMIT 10;

-- ---------------------------------------------------------------------
-- Q16. How do Savings vs Current accounts compare on volume and value?
-- ---------------------------------------------------------------------
SELECT
    account_type,
    COUNT(*)              AS transaction_count,
    ROUND(SUM(amount),2)  AS total_value,
    ROUND(AVG(amount),2)  AS avg_amount
FROM transactions
GROUP BY account_type;

-- ---------------------------------------------------------------------
-- Q17. What is the average transaction amount per payment method,
-- and which methods are only used for large payments (HAVING filter)?
-- ---------------------------------------------------------------------
SELECT
    payment_method,
    COUNT(*)             AS transaction_count,
    ROUND(AVG(amount),2) AS avg_amount
FROM transactions
GROUP BY payment_method
HAVING COUNT(*) > 100
ORDER BY avg_amount DESC;

-- ---------------------------------------------------------------------
-- Q18. Which customers have unusually high single transactions?
-- Flags transactions larger than 3x that customer's own average.
-- ---------------------------------------------------------------------
WITH customer_avg AS (
    SELECT customer_id, AVG(amount) AS avg_amount
    FROM transactions
    GROUP BY customer_id
)
SELECT
    t.transaction_id,
    t.customer_id,
    t.amount,
    ROUND(ca.avg_amount, 2)                          AS customer_avg_amount,
    ROUND(t.amount / ca.avg_amount, 2)               AS amount_vs_avg_ratio
FROM transactions t
JOIN customer_avg ca ON ca.customer_id = t.customer_id
WHERE t.amount > 3 * ca.avg_amount
ORDER BY amount_vs_avg_ratio DESC
LIMIT 50;

-- ---------------------------------------------------------------------
-- Q19. Which customers have the highest counts of failed transactions?
-- ---------------------------------------------------------------------
SELECT
    customer_id,
    COUNT(*)                                                       AS total_transactions,
    SUM(CASE WHEN transaction_status = 'Failed' THEN 1 ELSE 0 END) AS failed_transactions
FROM transactions
GROUP BY customer_id
HAVING failed_transactions >= 3
ORDER BY failed_transactions DESC
LIMIT 20;

-- ---------------------------------------------------------------------
-- Q20. How is monthly transaction value growing month over month?
-- LAG() compares each month's value to the previous month's value.
-- ---------------------------------------------------------------------
WITH monthly AS (
    SELECT
        transaction_year,
        transaction_month,
        SUM(amount) AS monthly_value
    FROM transactions
    GROUP BY transaction_year, transaction_month
)
SELECT
    transaction_year,
    transaction_month,
    ROUND(monthly_value, 2)                                                   AS monthly_value,
    ROUND(LAG(monthly_value) OVER (ORDER BY transaction_year, transaction_month), 2) AS prev_month_value,
    ROUND(
        (monthly_value - LAG(monthly_value) OVER (ORDER BY transaction_year, transaction_month))
        * 100.0 / LAG(monthly_value) OVER (ORDER BY transaction_year, transaction_month),
        2
    )                                                                          AS growth_pct
FROM monthly
ORDER BY transaction_year, transaction_month;

-- ---------------------------------------------------------------------
-- Q21. Rank branches by transaction value within each state.
-- RANK() gives the position of each branch inside its own state.
-- ---------------------------------------------------------------------
SELECT
    state,
    branch,
    total_value,
    RANK() OVER (PARTITION BY state ORDER BY total_value DESC) AS rank_in_state
FROM (
    SELECT state, branch, SUM(amount) AS total_value
    FROM transactions
    GROUP BY state, branch
) branch_totals;

-- ---------------------------------------------------------------------
-- Q22. Number each customer's transactions chronologically (ROW_NUMBER).
-- Useful for "first transaction of the day" or sequence analysis.
-- ---------------------------------------------------------------------
SELECT
    transaction_id,
    customer_id,
    transaction_date,
    transaction_time,
    amount,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date, transaction_time
    ) AS customer_txn_sequence
FROM transactions
ORDER BY customer_id, customer_txn_sequence
LIMIT 50;

-- ---------------------------------------------------------------------
-- Q23. Running total of transaction value per customer.
-- SUM() OVER with ORDER builds a cumulative balance of spend.
-- ---------------------------------------------------------------------
SELECT
    customer_id,
    transaction_date,
    amount,
    ROUND(SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date, transaction_time
    ), 2) AS running_total_value
FROM transactions
ORDER BY customer_id, transaction_date
LIMIT 50;

-- ---------------------------------------------------------------------
-- Q24. Compare each transaction to the running average for that customer.
-- AVG() OVER shows whether individual transactions are above/below the
-- customer's own typical behaviour at that point in time.
-- ---------------------------------------------------------------------
SELECT
    customer_id,
    transaction_id,
    amount,
    ROUND(AVG(amount) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date, transaction_time
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_avg_last_10_txns
FROM transactions
ORDER BY customer_id, transaction_date
LIMIT 50;

-- ---------------------------------------------------------------------
-- Q25. Which channels have a failure rate above the overall average?
-- Subquery computes the global rate once, outer query filters channels.
-- ---------------------------------------------------------------------
SELECT
    channel,
    ROUND(SUM(CASE WHEN transaction_status = 'Failed' THEN 1 ELSE 0 END) * 100.0
          / COUNT(*), 2) AS channel_failure_rate_pct
FROM transactions
GROUP BY channel
HAVING channel_failure_rate_pct >
       (SELECT ROUND(SUM(CASE WHEN transaction_status = 'Failed' THEN 1 ELSE 0 END) * 100.0
                     / COUNT(*), 2)
        FROM transactions)
ORDER BY channel_failure_rate_pct DESC;
