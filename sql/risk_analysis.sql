-- =====================================================================
-- risk_analysis.sql
-- Rule-based risk analytics for the Banking Transaction Data Pipeline
-- project.
--
-- IMPORTANT DISCLAIMER:
--   This is a SYNTHETIC, rule-based risk analysis. It does NOT detect
--   real financial fraud. It produces *analytical risk indicators* -
--   transactions that match simple, explainable business rules that in
--   a real system would be sent for human review.
--
-- RISK RULES (each rule contributes points to a total risk_score):
--   Rule 1 - Very high amount:        amount > 5x customer's own
--            average transaction amount                    -> +40 pts
--   Rule 2 - Multiple failures:       transaction failed AND the
--            customer has >= 3 failed transactions overall  -> +20 pts
--   Rule 3 - Unusual international:   international transaction
--            with amount > 3x customer's average           -> +30 pts
--   Rule 4 - Rapid consecutive:       amount > 2x the customer's
--            immediately previous transaction amount       -> +10 pts
--   Rule 5 - High daily activity:     customer made >= 5 transactions
--            on that same day                               -> +15 pts
--   Rule 6 - Above average amount:    amount > 2x customer's
--            average                                       -> +10 pts
--
-- RISK LEVELS (based on total score):
--   score = 0           -> LOW
--   score 10-39         -> MEDIUM
--   score >= 40         -> HIGH
--
-- These thresholds are intentionally simple and transparent so the
-- logic can be explained end-to-end in an interview.
-- =====================================================================

USE banking_analytics;

-- ---------------------------------------------------------------------
-- STEP 1. Build supporting aggregates (customer average, daily counts,
-- customer failure totals) as CTEs so each rule stays readable.
-- ---------------------------------------------------------------------

-- 1a. Per-customer average transaction amount (baseline for rules 1,3,4,6)
-- (embedded in the main query below via CTE)

-- ---------------------------------------------------------------------
-- STEP 2. Score every transaction and load transaction_risk table.
-- ---------------------------------------------------------------------

TRUNCATE TABLE transaction_risk;

INSERT INTO transaction_risk (
    transaction_id, customer_id, transaction_date, amount,
    customer_avg_amount,
    is_high_amount, has_multiple_failures, is_unusual_international,
    is_rapid_consecutive, is_high_daily_activity, above_customer_avg,
    risk_score, risk_level
)
WITH customer_avg AS (
    SELECT customer_id, AVG(amount) AS avg_amount
    FROM transactions
    GROUP BY customer_id
),
daily_counts AS (
    SELECT customer_id, transaction_date, COUNT(*) AS txns_that_day
    FROM transactions
    GROUP BY customer_id, transaction_date
),
customer_failures AS (
    SELECT customer_id,
           SUM(CASE WHEN transaction_status = 'Failed' THEN 1 ELSE 0 END) AS total_failures
    FROM transactions
    GROUP BY customer_id
),
scored AS (
    SELECT
        t.transaction_id,
        t.customer_id,
        t.transaction_date,
        t.amount,
        ca.avg_amount                                   AS customer_avg_amount,

        -- Rule 1: amount > 5x customer's average
        CASE WHEN t.amount > 5 * ca.avg_amount THEN 1 ELSE 0 END AS is_high_amount,

        -- Rule 2: this txn failed AND customer has >=3 total failures
        CASE WHEN t.transaction_status = 'Failed' AND cf.total_failures >= 3
             THEN 1 ELSE 0 END                              AS has_multiple_failures,

        -- Rule 3: international txn with amount > 3x customer avg
        CASE WHEN t.is_international = 1 AND t.amount > 3 * ca.avg_amount
             THEN 1 ELSE 0 END                              AS is_unusual_international,

        -- Rule 4: amount > 2x the customer's previous transaction amount
        CASE WHEN t.previous_transaction_amount IS NOT NULL
                  AND t.amount > 2 * t.previous_transaction_amount
             THEN 1 ELSE 0 END                              AS is_rapid_consecutive,

        -- Rule 5: customer made >=5 transactions that day
        CASE WHEN dc.txns_that_day >= 5 THEN 1 ELSE 0 END   AS is_high_daily_activity,

        -- Rule 6: amount > 2x customer's average
        CASE WHEN t.amount > 2 * ca.avg_amount THEN 1 ELSE 0 END AS above_customer_avg

    FROM transactions t
    JOIN customer_avg       ca ON ca.customer_id = t.customer_id
    JOIN daily_counts       dc ON dc.customer_id = t.customer_id
                              AND dc.transaction_date = t.transaction_date
    JOIN customer_failures  cf ON cf.customer_id = t.customer_id
)
SELECT
    transaction_id,
    customer_id,
    transaction_date,
    amount,
    customer_avg_amount,
    is_high_amount,
    has_multiple_failures,
    is_unusual_international,
    is_rapid_consecutive,
    is_high_daily_activity,
    above_customer_avg,
    (is_high_amount          * 40 +
     has_multiple_failures   * 20 +
     is_unusual_international* 30 +
     is_rapid_consecutive    * 10 +
     is_high_daily_activity  * 15 +
     above_customer_avg      * 10)                        AS risk_score,
    CASE
        WHEN (is_high_amount          * 40 +
              has_multiple_failures   * 20 +
              is_unusual_international* 30 +
              is_rapid_consecutive    * 10 +
              is_high_daily_activity  * 15 +
              above_customer_avg      * 10) >= 40 THEN 'HIGH'
        WHEN (is_high_amount          * 40 +
              has_multiple_failures   * 20 +
              is_unusual_international* 30 +
              is_rapid_consecutive    * 10 +
              is_high_daily_activity  * 15 +
              above_customer_avg      * 10) >= 10 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                  AS risk_level
FROM scored;

-- ---------------------------------------------------------------------
-- STEP 3. Summary analyses on the risk table
-- ---------------------------------------------------------------------

-- R1. How many transactions fall into each risk level?
SELECT
    risk_level,
    COUNT(*)                                   AS transaction_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM transaction_risk
GROUP BY risk_level
ORDER BY risk_level;

-- R2. Which rules trigger most often?
SELECT
    'Rule1_high_amount'            AS rule_name, SUM(is_high_amount)            AS hits
FROM transaction_risk
UNION ALL
SELECT 'Rule2_multiple_failures',  SUM(has_multiple_failures)      FROM transaction_risk
UNION ALL
SELECT 'Rule3_unusual_international', SUM(is_unusual_international) FROM transaction_risk
UNION ALL
SELECT 'Rule4_rapid_consecutive',  SUM(is_rapid_consecutive)       FROM transaction_risk
UNION ALL
SELECT 'Rule5_high_daily_activity', SUM(is_high_daily_activity)    FROM transaction_risk
UNION ALL
SELECT 'Rule6_above_customer_avg', SUM(above_customer_avg)         FROM transaction_risk
ORDER BY hits DESC;

-- R3. Sample of HIGH-risk transactions for manual review
SELECT *
FROM transaction_risk
WHERE risk_level = 'HIGH'
ORDER BY risk_score DESC, amount DESC
LIMIT 50;

-- R4. Customer-level risk profile:
-- aggregates each customer's transactional + risk behaviour into one
-- table so analysts can find high-value, highly active, failure-prone
-- or unusual-behaviour customers.
DROP TABLE IF EXISTS customer_risk_profile;
CREATE TABLE customer_risk_profile AS
SELECT
    t.customer_id,
    COUNT(*)                                                    AS total_transactions,
    ROUND(SUM(t.amount), 2)                                     AS total_transaction_amount,
    ROUND(AVG(t.amount), 2)                                     AS avg_transaction_amount,
    ROUND(MAX(t.amount), 2)                                     AS max_transaction_amount,
    SUM(CASE WHEN t.transaction_status = 'Failed' THEN 1 ELSE 0 END) AS failed_transactions,
    SUM(t.is_international)                                     AS international_transactions,
    SUM(CASE WHEN tr.risk_level = 'HIGH' THEN 1 ELSE 0 END)     AS high_risk_transactions,
    SUM(CASE WHEN tr.risk_level = 'MEDIUM' THEN 1 ELSE 0 END)   AS medium_risk_transactions,
    ROUND(AVG(tr.risk_score), 2)                                AS avg_risk_score,
    MAX(tr.risk_score)                                          AS max_risk_score
FROM transactions t
JOIN transaction_risk tr ON tr.transaction_id = t.transaction_id
GROUP BY t.customer_id;

ALTER TABLE customer_risk_profile
    ADD PRIMARY KEY (customer_id),
    ADD INDEX idx_crp_max_risk (max_risk_score);

-- R5. High-value customers (top 20 by total spend)
SELECT *
FROM customer_risk_profile
ORDER BY total_transaction_amount DESC
LIMIT 20;

-- R6. Highly active customers (top 20 by transaction count)
SELECT *
FROM customer_risk_profile
ORDER BY total_transactions DESC
LIMIT 20;

-- R7. Customers with frequent failures (>=3 failed transactions)
SELECT *
FROM customer_risk_profile
WHERE failed_transactions >= 3
ORDER BY failed_transactions DESC
LIMIT 20;

-- R8. Customers with unusual behaviour / highest risk scores
SELECT *
FROM customer_risk_profile
ORDER BY max_risk_score DESC, high_risk_transactions DESC
LIMIT 20;

-- R9. Overall risk distribution by month
SELECT
    transaction_year,
    transaction_month,
    COUNT(*)                                        AS transactions,
    SUM(CASE WHEN tr.risk_level = 'HIGH' THEN 1 ELSE 0 END)  AS high_risk,
    SUM(CASE WHEN tr.risk_level = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_risk
FROM transactions t
JOIN transaction_risk tr ON tr.transaction_id = t.transaction_id
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year, transaction_month;
