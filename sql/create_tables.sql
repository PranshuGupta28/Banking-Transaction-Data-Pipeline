-- =====================================================================
-- create_tables.sql
-- Creates the relational schema for the Banking Transaction Data
-- Pipeline & Risk Analytics project.
--
-- Primary model (operational/relational style):
--   customers                - one row per customer
--   accounts                 - one row per account (FK -> customers)
--   transactions             - one row per transaction (FK -> accounts)
--   transaction_risk         - rule-based risk score per transaction
--   monthly_transaction_summary - pre-aggregated monthly rollup
--
-- Indexes are created on the most commonly filtered/joined columns
-- (customer_id, account_id, transaction_date, transaction_type,
-- transaction_status) to keep analytical queries fast.
--
-- Usage:
--   mysql -u <user> -p banking_analytics < sql/create_tables.sql
-- =====================================================================

USE banking_analytics;

-- ---------------------------------------------------------------------
-- 1. customers - dimension-style table, one row per customer
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS transaction_risk;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS monthly_transaction_summary;

CREATE TABLE customers (
    customer_id           VARCHAR(12)  NOT NULL,
    customer_age          INT          NULL,
    customer_gender       VARCHAR(10)  NULL,
    customer_risk_score   DECIMAL(5,2) NULL,
    customer_age_group    VARCHAR(10)  NULL,
    PRIMARY KEY (customer_id)
) ENGINE = InnoDB;

-- ---------------------------------------------------------------------
-- 2. accounts - one row per account; each account belongs to a customer
-- ---------------------------------------------------------------------
CREATE TABLE accounts (
    account_id        VARCHAR(12) NOT NULL,
    customer_id       VARCHAR(12) NOT NULL,
    account_type      VARCHAR(20) NOT NULL,
    city              VARCHAR(50) NULL,
    state             VARCHAR(50) NULL,
    branch            VARCHAR(20) NULL,
    PRIMARY KEY (account_id),
    CONSTRAINT fk_accounts_customer
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
    INDEX idx_accounts_customer_id (customer_id),
    INDEX idx_accounts_branch (branch)
) ENGINE = InnoDB;

-- ---------------------------------------------------------------------
-- 3. transactions - fact-style table, one row per transaction
-- ---------------------------------------------------------------------
CREATE TABLE transactions (
    transaction_id               VARCHAR(12)    NOT NULL,
    customer_id                  VARCHAR(12)    NOT NULL,
    account_id                   VARCHAR(12)    NOT NULL,
    transaction_date             DATE           NOT NULL,
    transaction_time             TIME           NULL,
    transaction_type             VARCHAR(20)    NOT NULL,
    amount                       DECIMAL(15,2)  NOT NULL,
    balance_before               DECIMAL(15,2)  NULL,
    balance_after                DECIMAL(15,2)  NULL,
    account_type                 VARCHAR(20)    NULL,
    city                         VARCHAR(50)    NULL,
    state                        VARCHAR(50)    NULL,
    branch                       VARCHAR(20)    NULL,
    channel                      VARCHAR(30)    NULL,
    merchant_category            VARCHAR(50)    NULL,
    payment_method               VARCHAR(30)    NULL,
    transaction_status           VARCHAR(15)    NOT NULL,
    device_type                  VARCHAR(30)    NULL,
    is_international             TINYINT(1)     DEFAULT 0,
    country                      VARCHAR(40)    NULL,
    failed_attempts              INT            DEFAULT 0,
    previous_transaction_amount  DECIMAL(15,2)  NULL,
    transaction_year             INT            NULL,
    transaction_month            INT            NULL,
    transaction_day              INT            NULL,
    transaction_hour             INT            NULL,
    transaction_amount_bucket    VARCHAR(30)    NULL,
    PRIMARY KEY (transaction_id),
    CONSTRAINT fk_txn_account
        FOREIGN KEY (account_id) REFERENCES accounts (account_id),
    INDEX idx_txn_customer_id (customer_id),
    INDEX idx_txn_account_id (account_id),
    INDEX idx_txn_date (transaction_date),
    INDEX idx_txn_type (transaction_type),
    INDEX idx_txn_status (transaction_status),
    INDEX idx_txn_year_month (transaction_year, transaction_month)
) ENGINE = InnoDB;

-- ---------------------------------------------------------------------
-- 4. transaction_risk - explainable, rule-based risk scoring output.
--    Populated by sql/risk_analysis.sql. NOTE: these are analytical
--    indicators on synthetic data, NOT confirmed fraud flags.
-- ---------------------------------------------------------------------
CREATE TABLE transaction_risk (
    transaction_id            VARCHAR(12) NOT NULL,
    customer_id               VARCHAR(12) NOT NULL,
    transaction_date          DATE        NULL,
    amount                    DECIMAL(15,2) NULL,
    customer_avg_amount       DECIMAL(15,2) NULL,
    is_high_amount            TINYINT(1)  DEFAULT 0,   -- Rule 1
    has_multiple_failures     TINYINT(1)  DEFAULT 0,   -- Rule 2
    is_unusual_international  TINYINT(1)  DEFAULT 0,   -- Rule 3
    is_rapid_consecutive      TINYINT(1)  DEFAULT 0,   -- Rule 4
    is_high_daily_activity    TINYINT(1)  DEFAULT 0,   -- Rule 5
    above_customer_avg        TINYINT(1)  DEFAULT 0,   -- Rule 6
    risk_score                INT         DEFAULT 0,
    risk_level                VARCHAR(10) DEFAULT 'LOW',
    PRIMARY KEY (transaction_id),
    INDEX idx_risk_customer (customer_id),
    INDEX idx_risk_level (risk_level),
    INDEX idx_risk_score (risk_score)
) ENGINE = InnoDB;

-- ---------------------------------------------------------------------
-- 5. monthly_transaction_summary - pre-aggregated rollup table
-- ---------------------------------------------------------------------
CREATE TABLE monthly_transaction_summary (
    summary_id              INT AUTO_INCREMENT PRIMARY KEY,
    transaction_year        INT           NOT NULL,
    transaction_month       INT           NOT NULL,
    total_transactions      INT           NULL,
    total_amount            DECIMAL(18,2) NULL,
    avg_amount              DECIMAL(15,2) NULL,
    successful_transactions INT           NULL,
    failed_transactions     INT           NULL,
    success_rate_pct        DECIMAL(6,2)  NULL,
    UNIQUE KEY uq_year_month (transaction_year, transaction_month)
) ENGINE = InnoDB;

-- ---------------------------------------------------------------------
-- (Optional) Customer-level rollup tables produced by the PySpark ETL.
-- Created here so banking_etl.py's MySQL load step has targets.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer_summary (
    customer_id               VARCHAR(12) PRIMARY KEY,
    total_transactions        INT NULL,
    total_amount              DECIMAL(18,2) NULL,
    avg_amount                DECIMAL(15,2) NULL,
    max_amount                DECIMAL(15,2) NULL,
    failed_transactions       INT NULL,
    international_transactions INT NULL
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS transaction_risk_summary (
    customer_id                 VARCHAR(12) PRIMARY KEY,
    total_transactions          INT NULL,
    high_value_outlier_count    INT NULL,
    failed_transactions         INT NULL,
    international_transactions  INT NULL,
    customer_risk_score         DECIMAL(5,2) NULL
) ENGINE = InnoDB;
