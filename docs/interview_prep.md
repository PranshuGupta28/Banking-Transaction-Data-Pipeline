# Interview Preparation - Banking Transaction Data Pipeline & Risk Analytics

## A. 2-Minute Project Explanation

> "I built an end-to-end banking transaction data pipeline entirely on my
> local machine, simulating what a data engineer does at a bank. First, I
> generated ~55,000 realistic synthetic transactions for 4,000 customers
> over one year - and deliberately injected the kind of defects you'd see
> in a real raw extract: missing values, duplicate transaction IDs,
> negative amounts, inconsistent capitalization like 'UPI' vs 'upi', and
> invalid dates.
>
> Then I built the pipeline in layers: an automated data-quality report
> that quantifies every defect; a Pandas cleaning pipeline that
> standardizes categories, fixes missing values and derives fields like
> hour-of-day and amount buckets; a PySpark ETL that re-runs the same
> ingest-validate-clean-transform-aggregate flow with an explicit schema;
> and a MySQL relational model - customers, accounts, transactions,
> transaction_risk and a monthly summary table - loaded via JDBC with
> credentials passed as environment variables.
>
> On top of that I wrote 25+ SQL analytical queries using CTEs and window
> functions, and a fully explainable, rule-based risk scoring layer: six
> transparent rules like 'amount > 5x the customer's own average' or
> 'more than 5 transactions in one day' produce a risk_score and a
> LOW/MEDIUM/HIGH level. Finally, a script auto-generates the insights
> report so every number is real and reproducible. The whole thing is
> seeded and rerunnable end-to-end."

## B. 5-Minute Detailed Project Explanation

Walk through the pipeline stages as in the README architecture diagram:

1. **Data generation** - 4,000 customers, 5,176 accounts, ~55k
   transactions. Field relationships are enforced (ATM -> Cash +
   Withdrawal, international -> foreign country, etc.). ~1-2% defects
   injected deliberately so downstream stages have real problems to fix.
   Fixed seed = reproducibility.

2. **EDA** - notebook answers business questions: amount distribution is
   right-skewed; Mobile Banking is the dominant channel; ATM has the
   highest failure rate; success rate ~90%; monthly volume trends.

3. **Data quality** - `data_quality.py` outputs a metric/value report:
   total records, duplicates, missing values per column, invalid
   amounts/dates/categories, impossible ages, valid vs rejected rows.
   Important design choice: the report measures; it does not mutate data.

4. **Cleaning** - `data_cleaning.py`: drop full duplicates and duplicate
   IDs, standardize categories via case-insensitive mapping, fill missing
   categoricals with 'Unknown' and age with the median, drop unparsable
   dates and non-positive amounts, clip ages, recalculate balance_after,
   derive transaction_year/month/day/hour, amount buckets, age groups.

5. **PySpark ETL** - `banking_etl.py`: explicit StructType schema (not
   inferSchema - explain why), dropDuplicates, categorical normalization
   via `when`/`otherwise`, to_date parsing, aggregations into
   customer/monthly/branch/payment/risk summaries, JDBC load into MySQL
   gated by environment variables.

6. **MySQL + data model** - `customers` 1:N `accounts` 1:N
   `transactions`; `transaction_risk` keyed on transaction_id;
   `monthly_transaction_summary` as a rollup. Indexes on the hot join/
   filter columns. In warehouse terms, transactions = fact table,
   customers/accounts = dimensions.

7. **SQL analytics** - 25 queries; highlight LAG for month-over-month
   growth, RANK over partition for branch rankings, ROW_NUMBER for
   per-customer sequencing, conditional aggregation for success rate.

8. **Risk analytics** - six weighted rules summed into a score, bucketed
   LOW/MEDIUM/HIGH; mirrored identically in SQL and Pandas. Customer
   risk profile aggregates behavior per customer. Emphasize: indicators
   for human review, NOT fraud detection.

9. **Insights** - `generate_insights.py` computes every figure from the
   actual CSVs, so the report is always consistent with the data.

## C. 20 Technical Interview Questions

### Python / Pandas / EDA

1. **Why generate synthetic data instead of downloading a dataset?**
   Control: I needed ~55k rows, exact column names, one-year coverage,
   realistic relationships (ATM -> Cash), controlled defect injection,
   and full reproducibility via a seed - no public dataset gives all that
   legally and cleanly.

2. **How did you make customer IDs repeat realistically?**
   I generated a customer master (4,000 customers) and an account master
   (~5,176 accounts, each mapped to one customer, customers can have 2
   accounts), then sampled accounts for each transaction - so the same
   customers/accounts naturally appear many times.

3. **Why does EDA run on the RAW file rather than the cleaned one?**
   Because part of EDA's job is quantifying data-quality problems. If I
   explored only cleaned data, missing values and duplicates would be
   invisible and the cleaning decisions couldn't be justified.

4. **How did you detect outliers in transaction amount?**
   IQR method: Q1 - 1.5xIQR to Q3 + 1.5xIQR, plus a log-scale boxplot.
   I kept legitimate high-value transactions (banking data is naturally
   right-skewed) and only removed genuinely invalid amounts (<= 0).

5. **What vectorized Pandas operations did you use, and why avoid loops?**
   `groupby().agg()`, `transform()`, `pd.cut`, `np.where`, boolean masks.
   Vectorized ops run in C-level loops, are ~100x faster than Python
   `for` loops at this scale, and are more readable.

### Data Quality / Cleaning

6. **Which missing-value strategies did you use and why?**
   Categoricals -> "Unknown" (retains the row, makes the gap visible);
   customer_age -> median of valid ages (robust to the impossible values
   like 150); balance_after -> recalculated from balance_before +/-
   amount (deterministic rule, not imputation).

7. **What's the difference between dropping a row and flagging it?**
   Dropping loses data forever; flagging (e.g., "Unknown" categories)
   preserves the record for volume analysis while marking uncertainty.
   I only dropped rows where the defect breaks core logic - unparsable
   dates, non-positive amounts, duplicate IDs.

8. **Why dedupe on transaction_id in addition to drop_duplicates()?**
   The raw extract contains rows that differ in content but share a
   transaction_id (simulating a source-system ID bug). `drop_duplicates()`
   wouldn't catch those because the rows aren't identical.

### PySpark

9. **Why PySpark when the data fits in Pandas?**
   Same code scales to billions of rows on a cluster; explicit schema
   enforcement; lazy evaluation builds an optimized plan; and it's the
   industry-standard engine for banking-scale transaction pipelines.
   The project deliberately demonstrates distributed-style ETL, not
   because 55k rows require it - and I say that honestly in the script's
   docstring.

10. **Why define an explicit schema instead of inferSchema?**
    inferSchema does an extra pass over the data, is slow at scale, and
    can silently coerce messy values to the wrong type or null - I want
    raw strings first, then parse/validate deliberately with clear rules.

11. **Explain lazy evaluation in your ETL.**
    `withColumn`/`filter`/`groupBy` build a logical plan - nothing runs
    until an action (`count`, `write`, `first`) is called. That's why I
    could chain the whole cleaning+transform pipeline and Spark
    optimized it as a single job. It's also why a bad `to_timestamp`
    deep in the transform stage only surfaced when a later `agg` ran.

12. **How does dropDuplicates differ from drop_duplicates in Pandas?**
    Pandas is single-machine and dedupes the loaded DataFrame; Spark's
    `dropDuplicates` shuffles data across executors to dedupe
    distributedly - same API intent, distributed mechanics.

13. **How do you load Spark output into MySQL?**
    `df.write.jdbc(url, table, mode, properties)` with the MySQL
    Connector/J jar on the classpath; credentials come from environment
    variables (`MYSQL_PASSWORD`, etc.) - never hardcoded or committed.

### SQL / MySQL

14. **Explain your schema - why fact vs dimension tables?**
    `transactions` is the fact table (one row per measurable event);
    `customers` and `accounts` are dimensions (descriptive context).
    Separating them avoids repeating customer attributes on all ~55k
    transaction rows and makes analytical joins clean. Foreign keys keep
    referential integrity; indexes on join/filter columns keep queries
    fast.

15. **Explain LAG() in your month-over-month growth query.**
    `LAG(monthly_value) OVER (ORDER BY year, month)` returns the previous
    row's value inside an ordered window, letting me compute
    `(current - previous) / previous * 100` without a self-join. March
    vs February becomes a one-pass window calculation.

16. **HAVING vs WHERE - where did you use each?**
    `WHERE` filters rows before aggregation (e.g., `amount > 0`);
    `HAVING` filters groups after aggregation (e.g., customers with
    `failed_transactions >= 3`, or payment methods with >100 txns).

17. **CTE vs subquery - why did you prefer CTEs?**
    Readability and reuse: `customer_avg` is consumed by multiple risk
    rules; a named CTE documents intent and can be joined multiple times
    in the same statement, whereas a nested subquery would be buried
    inside the SELECT/WHERE clause.

18. **How does SUM(...) OVER() differ from SUM(...) with GROUP BY?**
    GROUP BY collapses rows into groups; `SUM() OVER (PARTITION BY ...)`
    keeps every row and adds an aggregated value per partition - e.g.,
    a running total per customer or comparing each row to its group
    percentage without losing the row grain.

### Data Modeling / Risk

19. **Walk me through the risk scoring logic.**
    Six binary rules, each with a documented weight: amount > 5x
    customer's own avg (+40), unusual international (+30), failed txn by
    a customer with >=3 total failures (+20), >=5 same-day txns (+15),
    amount > 2x previous txn (+10), amount > 2x own avg (+10). Sum ->
    risk_score; >=40 HIGH, >=10 MEDIUM, else LOW. Fully explainable: any
    flagged transaction can list exactly which rules fired and why.

20. **Why rules instead of machine learning?**
    Three honest reasons: (a) with synthetic data there are no real
    fraud labels to train on - an ML model would be learning my own
    rules back at me; (b) rule-based systems are auditable and
    explainable, which regulators and reviewers actually need; (c) a
    fresher project should demonstrate correct foundations before
    advanced techniques. ML would be a natural next step with labeled
    real data.

## D. Additional Explainer Answers

**Why Python?** It's the lingua franca of data engineering - Pandas for
transformation, NumPy for numerics, huge ecosystem (PySpark, SQLAlchemy,
MySQL connector), readable and fast to iterate on. Every layer of this
project shares one language, so the pipeline stays coherent.

**Why PySpark?** See Q9 - distributed-style ETL skills, explicit schemas,
lazy optimized plans, and the same code pattern used by real banking
pipelines at scale.

**Why MySQL?** Industry-standard, free, runs locally, supports everything
the project needs (foreign keys, indexes, CTEs, window functions in
MySQL 8+), and it forces real-world concerns like schema design,
credentials management and JDBC loading - things SQLite glosses over.

**Pandas vs PySpark?** Pandas: single-machine, eager evaluation, data in
RAM, great for exploration up to a few GB. PySpark: distributed engine,
lazy evaluation, scales horizontally, required for data larger than
memory and for cluster production jobs - at the cost of more setup
(JVM, Spark session) and less interactive ergonomics.

**The ETL pipeline in one breath:** Extract the raw CSV with an explicit
schema -> validate (parsable dates, positive amounts, valid categories)
-> clean (dedupe, normalize, fill/recalculate) -> transform (derived
time/bucket fields) -> aggregate (customer/monthly/branch/payment/risk
summaries) -> load to CSV and, when configured, MySQL via JDBC.

**The schema in one breath:** customers (PK) -> accounts (FK
customer_id) -> transactions (FK account_id, the fact table) ->
transaction_risk (per-transaction score) + monthly_transaction_summary
(pre-aggregated rollup); indexes on customer_id, account_id, date, type,
status.

**The risk logic in one breath:** Each transaction is tested against six
documented rules comparing it to the customer's own baseline and daily
activity; weighted hits sum to risk_score, bucketed into
LOW/MEDIUM/HIGH - producing review indicators, not fraud verdicts.
