"""
data_quality.py
-----------------
Runs a set of explainable, rule-based data-quality checks against the
raw banking transaction extract and produces a data-quality report.

Checks performed:
    - Missing values (per column and overall)
    - Fully duplicated records
    - Duplicate transaction_id values
    - Invalid transaction amounts (<= 0)
    - Invalid transaction dates (unparsable / out-of-range)
    - Invalid categorical values (transaction_type, account_type,
      transaction_status, channel, payment_method) once case/whitespace
      is normalised
    - Impossible customer ages (< 15 or > 100)

Output:
    outputs/reports/data_quality_report.csv

This script does NOT modify the raw data. Cleaning is handled
separately in data_cleaning.py so that the data-quality report always
reflects the true condition of the raw extract.
"""

import os
import pandas as pd
import numpy as np

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_PATH = os.path.join(BASE_DIR, "data", "raw", "banking_transactions_raw.csv")
REPORT_PATH = os.path.join(BASE_DIR, "outputs", "reports", "data_quality_report.csv")

VALID_TRANSACTION_TYPES = {"debit", "credit", "transfer", "withdrawal"}
VALID_ACCOUNT_TYPES = {"savings", "current"}
VALID_STATUSES = {"success", "failed", "pending"}
VALID_CHANNELS = {"atm", "mobile banking", "internet banking", "branch", "pos"}
VALID_PAYMENT_METHODS = {"upi", "debit card", "credit card", "neft", "imps", "rtgs", "cash"}


def normalize(series: pd.Series) -> pd.Series:
    """Lowercase + strip whitespace, used only for validity comparisons."""
    return series.astype(str).str.strip().str.lower()


def check_missing_values(df: pd.DataFrame) -> pd.Series:
    return df.isna().sum()


def check_duplicate_records(df: pd.DataFrame) -> int:
    return int(df.duplicated().sum())


def check_duplicate_transaction_ids(df: pd.DataFrame) -> int:
    return int(df["transaction_id"].duplicated().sum())


def check_invalid_amounts(df: pd.DataFrame) -> int:
    return int((df["amount"] <= 0).sum())


def check_invalid_dates(df: pd.DataFrame) -> int:
    parsed = pd.to_datetime(df["transaction_date"], errors="coerce", format="%Y-%m-%d")
    return int(parsed.isna().sum())


def check_invalid_categories(df: pd.DataFrame) -> dict:
    results = {}
    results["invalid_transaction_type"] = int((~normalize(df["transaction_type"]).isin(VALID_TRANSACTION_TYPES)).sum())
    results["invalid_account_type"] = int((~normalize(df["account_type"]).isin(VALID_ACCOUNT_TYPES)).sum())
    results["invalid_transaction_status"] = int((~normalize(df["transaction_status"]).isin(VALID_STATUSES)).sum())
    results["invalid_channel"] = int((~normalize(df["channel"]).isin(VALID_CHANNELS)).sum())
    results["invalid_payment_method"] = int((~normalize(df["payment_method"].fillna("")).isin(VALID_PAYMENT_METHODS)).sum())
    return results


def check_impossible_ages(df: pd.DataFrame) -> int:
    """Counts ages that are present but out of a realistic human range.
    Missing ages are excluded here since they are already reported
    separately under 'missing values'."""
    ages = pd.to_numeric(df["customer_age"], errors="coerce")
    return int((ages.notna() & ~ages.between(15, 100)).sum())


def run_data_quality_checks(df: pd.DataFrame) -> pd.DataFrame:
    total_records = len(df)
    missing = check_missing_values(df)
    total_missing = int(missing.sum())
    duplicate_records = check_duplicate_records(df)
    duplicate_ids = check_duplicate_transaction_ids(df)
    invalid_amounts = check_invalid_amounts(df)
    invalid_dates = check_invalid_dates(df)
    invalid_categories = check_invalid_categories(df)
    total_invalid_categories = sum(invalid_categories.values())
    impossible_ages = check_impossible_ages(df)

    # A record is "invalid" if it has ANY problem (used for a rough
    # valid/rejected split - actual cleaning fixes many of these rather
    # than dropping them, see data_cleaning.py)
    invalid_row_mask = (
        df.duplicated()
        | df["transaction_id"].duplicated()
        | (df["amount"] <= 0)
        | pd.to_datetime(df["transaction_date"], errors="coerce", format="%Y-%m-%d").isna()
        | (
            pd.to_numeric(df["customer_age"], errors="coerce").notna()
            & ~pd.to_numeric(df["customer_age"], errors="coerce").between(15, 100)
        )
    )
    rejected_records = int(invalid_row_mask.sum())
    valid_records = total_records - rejected_records

    summary_rows = [
        ("Total records", total_records),
        ("Duplicate records (full row)", duplicate_records),
        ("Duplicate transaction_id values", duplicate_ids),
        ("Total missing values (all columns)", total_missing),
        ("Missing: city", int(missing.get("city", 0))),
        ("Missing: customer_age", int(missing.get("customer_age", 0))),
        ("Missing: merchant_category", int(missing.get("merchant_category", 0))),
        ("Missing: payment_method", int(missing.get("payment_method", 0))),
        ("Missing: balance_after", int(missing.get("balance_after", 0))),
        ("Invalid amounts (<= 0)", invalid_amounts),
        ("Invalid transaction dates", invalid_dates),
        ("Invalid transaction_type values", invalid_categories["invalid_transaction_type"]),
        ("Invalid account_type values", invalid_categories["invalid_account_type"]),
        ("Invalid transaction_status values", invalid_categories["invalid_transaction_status"]),
        ("Invalid channel values", invalid_categories["invalid_channel"]),
        ("Invalid payment_method values", invalid_categories["invalid_payment_method"]),
        ("Total invalid categorical values", total_invalid_categories),
        ("Impossible customer ages (<15 or >100)", impossible_ages),
        ("Valid records (no critical issue)", valid_records),
        ("Rejected records (>=1 critical issue)", rejected_records),
    ]
    return pd.DataFrame(summary_rows, columns=["metric", "value"])


def main():
    df = pd.read_csv(RAW_PATH)
    report = run_data_quality_checks(df)

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    report.to_csv(REPORT_PATH, index=False)

    print("Data Quality Report")
    print("=" * 50)
    print(report.to_string(index=False))
    print(f"\nSaved report -> {REPORT_PATH}")


if __name__ == "__main__":
    main()
