"""
data_cleaning.py
-----------------
Cleans and validates the raw banking transaction extract, producing an
analysis-ready dataset.

Steps performed:
    1. Remove fully duplicated records.
    2. Remove rows with duplicate transaction_id (keep first occurrence).
    3. Standardize column names (snake_case, already the case here).
    4. Standardize categorical values (case, whitespace).
    5. Handle missing values:
        - city / merchant_category / payment_method -> "Unknown"
        - customer_age -> filled with median age
        - balance_after -> recalculated from balance_before +/- amount
    6. Convert transaction_date/time to proper datetime; drop rows with
       unparsable dates (kept as a rejected list for transparency).
    7. Convert numeric fields to numeric dtype.
    8. Flag and remove invalid transaction amounts (<= 0).
    9. Validate & clip customer age to a realistic [15, 100] range.
    10. Validate transaction_status / transaction_type / payment_method
        against allowed value sets (after normalisation).
    11. Recalculate balance_after where it was missing or inconsistent.
    12. Create derived fields:
        - transaction_month, transaction_year, transaction_day, transaction_hour
        - transaction_amount_bucket
        - customer_age_group

Output:
    data/processed/banking_transactions_cleaned.csv
"""

import os
import pandas as pd
import numpy as np

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
RAW_PATH = os.path.join(BASE_DIR, "data", "raw", "banking_transactions_raw.csv")
CLEANED_PATH = os.path.join(BASE_DIR, "data", "processed", "banking_transactions_cleaned.csv")

VALID_TRANSACTION_TYPES = ["Debit", "Credit", "Transfer", "Withdrawal"]
VALID_ACCOUNT_TYPES = ["Savings", "Current"]
VALID_STATUSES = ["Success", "Failed", "Pending"]
VALID_PAYMENT_METHODS = ["UPI", "Debit Card", "Credit Card", "NEFT", "IMPS", "RTGS", "Cash"]

AGE_BINS = [0, 25, 35, 45, 55, 65, 120]
AGE_LABELS = ["18-25", "26-35", "36-45", "46-55", "56-65", "66+"]

AMOUNT_BINS = [-np.inf, 1000, 5000, 20000, 50000, np.inf]
AMOUNT_LABELS = ["Very Low (<1K)", "Low (1K-5K)", "Medium (5K-20K)", "High (20K-50K)", "Very High (50K+)"]


def standardize_categorical(series: pd.Series, valid_values: list) -> pd.Series:
    """Maps messy category strings (varied case/whitespace) back onto the
    canonical value from valid_values, leaving unmatched values as NaN."""
    lookup = {v.lower(): v for v in valid_values}
    cleaned = series.astype(str).str.strip().str.lower().map(lookup)
    return cleaned


def load_raw(path: str = RAW_PATH) -> pd.DataFrame:
    return pd.read_csv(path)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    initial_rows = len(df)

    # 1. Remove fully duplicated rows
    df = df.drop_duplicates()

    # 2. Remove rows with duplicate transaction_id (keep first)
    df = df.drop_duplicates(subset="transaction_id", keep="first")

    # 3. Column names already snake_case - nothing to rename here, but
    #    normalise just in case of stray whitespace in headers.
    df.columns = [c.strip().lower() for c in df.columns]

    # 4/10. Standardize categorical values
    df["transaction_type"] = standardize_categorical(df["transaction_type"], VALID_TRANSACTION_TYPES)
    df["account_type"] = standardize_categorical(df["account_type"], VALID_ACCOUNT_TYPES)
    df["transaction_status"] = standardize_categorical(df["transaction_status"], VALID_STATUSES)
    df["payment_method"] = standardize_categorical(df["payment_method"].fillna(""), VALID_PAYMENT_METHODS)

    gender_map = {"male": "Male", "m": "Male", "female": "Female", "f": "Female"}
    df["customer_gender"] = df["customer_gender"].astype(str).str.strip().str.lower().map(gender_map)

    # Drop rows where core categorical fields could not be standardized
    # (transaction_type/status/account_type are critical for analysis)
    df = df.dropna(subset=["transaction_type", "account_type", "transaction_status"])

    # 5. Handle missing values
    df["city"] = df["city"].fillna("Unknown")
    df["merchant_category"] = df["merchant_category"].fillna("Unknown")
    df["payment_method"] = df["payment_method"].fillna("Unknown")
    df["customer_gender"] = df["customer_gender"].fillna("Unknown")

    df["customer_age"] = pd.to_numeric(df["customer_age"], errors="coerce")
    median_age = df.loc[df["customer_age"].between(15, 100), "customer_age"].median()
    df["customer_age"] = df["customer_age"].fillna(median_age)

    # 6. Parse dates; drop rows with unparsable transaction_date
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce", format="%Y-%m-%d")
    df = df.dropna(subset=["transaction_date"])

    df["transaction_time"] = pd.to_datetime(df["transaction_time"], errors="coerce", format="%H:%M:%S").dt.time

    # 7. Convert numeric fields
    numeric_cols = ["amount", "balance_before", "balance_after", "customer_age",
                     "failed_attempts", "previous_transaction_amount", "customer_risk_score"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 8. Remove invalid transaction amounts (<= 0)
    df = df[df["amount"] > 0]

    # 9. Validate & clip customer age to realistic range
    df["customer_age"] = df["customer_age"].clip(lower=15, upper=100)

    # 11. Recalculate/repair balance_after where missing or inconsistent
    expected_balance_after = np.where(
        df["transaction_type"] == "Credit",
        df["balance_before"] + df["amount"],
        df["balance_before"] - df["amount"],
    )
    missing_balance = df["balance_after"].isna()
    df.loc[missing_balance, "balance_after"] = expected_balance_after[missing_balance.values]
    df["balance_after"] = df["balance_after"].clip(lower=0)

    # previous_transaction_amount: fill any remaining nulls with current amount
    df["previous_transaction_amount"] = df["previous_transaction_amount"].fillna(df["amount"])

    # 12. Derived fields
    df["transaction_year"] = df["transaction_date"].dt.year
    df["transaction_month"] = df["transaction_date"].dt.month
    df["transaction_day"] = df["transaction_date"].dt.day
    df["transaction_hour"] = pd.to_datetime(
        df["transaction_time"].astype(str), errors="coerce", format="%H:%M:%S"
    ).dt.hour

    df["transaction_amount_bucket"] = pd.cut(df["amount"], bins=AMOUNT_BINS, labels=AMOUNT_LABELS)
    df["customer_age_group"] = pd.cut(df["customer_age"], bins=AGE_BINS, labels=AGE_LABELS)

    df = df.reset_index(drop=True)

    removed = initial_rows - len(df)
    print(f"Initial rows: {initial_rows}")
    print(f"Rows removed during cleaning: {removed}")
    print(f"Final cleaned rows: {len(df)}")

    return df


def main():
    df_raw = load_raw()
    df_clean = clean_data(df_raw)

    os.makedirs(os.path.dirname(CLEANED_PATH), exist_ok=True)
    df_clean.to_csv(CLEANED_PATH, index=False)
    print(f"Saved cleaned dataset -> {CLEANED_PATH}")


if __name__ == "__main__":
    main()
