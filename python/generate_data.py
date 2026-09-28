"""
generate_data.py
-----------------
Generates a realistic SYNTHETIC banking transaction dataset for the
"Banking Transaction Data Pipeline & Risk Analytics" project.

This script creates:
    data/raw/banking_transactions_raw.csv

The dataset simulates ~1 year of banking transactions across many
customers/accounts/branches, with realistic field relationships
(e.g. ATM transactions -> ATM channel, international transactions ->
foreign country, failed transactions -> failed_attempts > 0, etc.)

It then DELIBERATELY injects realistic data-quality issues so that the
rest of the pipeline (data_quality.py, data_cleaning.py, PySpark ETL)
has real problems to detect and fix. This mirrors what a fresher would
encounter with real-world raw banking data extracts.

INTENTIONALLY INTRODUCED DATA QUALITY ISSUES (documented):
    1. ~1.5% missing values scattered across: city, customer_age,
       merchant_category, payment_method, balance_after.
    2. ~0.5% duplicate full rows (same transaction repeated in extract).
    3. ~0.3% duplicate transaction_id values assigned to different rows
       (simulates an ID-generation bug in the source system).
    4. ~0.5% negative or zero transaction amounts (invalid amounts).
    5. Inconsistent capitalization / spelling in categorical columns:
       payment_method ("upi", "UPI", "Upi"), customer_gender
       ("Male", "male", "M", "Female", "female", "F"), transaction_type
       ("debit", "Debit", "DEBIT").
    6. ~0.2% invalid/garbled transaction_date values (e.g. "31-13-2025",
       empty string) simulating extraction/export errors.
    7. ~0.1% impossible customer ages (e.g. 4, 150) simulating manual
       data-entry errors.

A fixed random seed (42) is used throughout so the dataset is fully
reproducible.
"""

import numpy as np
import pandas as pd
import os
import string

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
SEED = 42
N_RECORDS = 55000  # a little above 50,000 so that after removing
                    # injected duplicates we still comfortably exceed 50k
N_CUSTOMERS = 4000
N_ACCOUNTS = 5200

rng = np.random.default_rng(SEED)
np.random.seed(SEED)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "banking_transactions_raw.csv")

# ----------------------------------------------------------------------
# REFERENCE / LOOKUP DATA
# ----------------------------------------------------------------------
TRANSACTION_TYPES = ["Debit", "Credit", "Transfer", "Withdrawal"]
TRANSACTION_TYPE_WEIGHTS = [0.35, 0.25, 0.20, 0.20]

CHANNELS = ["ATM", "Mobile Banking", "Internet Banking", "Branch", "POS"]
CHANNEL_WEIGHTS = [0.20, 0.30, 0.20, 0.10, 0.20]

PAYMENT_METHODS = ["UPI", "Debit Card", "Credit Card", "NEFT", "IMPS", "RTGS", "Cash"]
PAYMENT_METHOD_WEIGHTS = [0.30, 0.20, 0.15, 0.10, 0.12, 0.05, 0.08]

TRANSACTION_STATUSES = ["Success", "Failed", "Pending"]
STATUS_WEIGHTS = [0.90, 0.07, 0.03]

ACCOUNT_TYPES = ["Savings", "Current"]
ACCOUNT_TYPE_WEIGHTS = [0.75, 0.25]

DEVICE_TYPES = ["Android", "iOS", "Web", "ATM Machine", "POS Terminal"]

MERCHANT_CATEGORIES = [
    "Grocery", "Fuel", "Utilities", "E-commerce", "Restaurant",
    "Travel", "Healthcare", "Education", "Entertainment", "ATM Cash",
    "Salary Credit", "Rent", "Insurance", "Other",
]

CITIES_STATES = {
    "Mumbai": "Maharashtra", "Pune": "Maharashtra", "Delhi": "Delhi",
    "Bengaluru": "Karnataka", "Chennai": "Tamil Nadu", "Hyderabad": "Telangana",
    "Kolkata": "West Bengal", "Ahmedabad": "Gujarat", "Jaipur": "Rajasthan",
    "Lucknow": "Uttar Pradesh", "Chandigarh": "Punjab", "Kochi": "Kerala",
    "Bhopal": "Madhya Pradesh", "Patna": "Bihar", "Surat": "Gujarat",
}
CITIES = list(CITIES_STATES.keys())

BRANCHES = [f"Branch-{code}" for code in
            ["MUM01", "MUM02", "PUN01", "DEL01", "DEL02", "BLR01", "BLR02",
             "CHE01", "HYD01", "KOL01", "AHM01", "JAI01", "LKO01", "CHD01",
             "KOC01"]]

FOREIGN_COUNTRIES = ["USA", "UK", "UAE", "Singapore", "Germany", "Australia", "Canada"]

# ----------------------------------------------------------------------
# STEP 1: Build the customer & account master (so IDs repeat realistically)
# ----------------------------------------------------------------------
customer_ids = [f"CUST{str(i).zfill(6)}" for i in range(1, N_CUSTOMERS + 1)]
customer_age = rng.integers(18, 75, size=N_CUSTOMERS)
customer_gender = rng.choice(["Male", "Female"], size=N_CUSTOMERS, p=[0.55, 0.45])
customer_risk_base = np.clip(rng.normal(30, 15, size=N_CUSTOMERS), 0, 100).round(1)

customer_master = pd.DataFrame({
    "customer_id": customer_ids,
    "customer_age": customer_age,
    "customer_gender": customer_gender,
    "customer_risk_score": customer_risk_base,
})

# Each account belongs to exactly one customer; a customer can have 1-2 accounts
account_rows = []
account_counter = 1
for cid in customer_ids:
    n_acc = rng.choice([1, 2], p=[0.7, 0.3])
    for _ in range(n_acc):
        if account_counter > N_ACCOUNTS:
            break
        account_rows.append((f"ACC{str(account_counter).zfill(6)}", cid))
        account_counter += 1
account_master = pd.DataFrame(account_rows, columns=["account_id", "customer_id"])
account_master["account_type"] = rng.choice(
    ACCOUNT_TYPES, size=len(account_master), p=ACCOUNT_TYPE_WEIGHTS
)
account_master["city"] = rng.choice(CITIES, size=len(account_master))
account_master["state"] = account_master["city"].map(CITIES_STATES)
account_master["branch"] = rng.choice(BRANCHES, size=len(account_master))

print(f"Customers: {len(customer_master)}  Accounts: {len(account_master)}")

# ----------------------------------------------------------------------
# STEP 2: Generate transactions by sampling accounts (so IDs repeat)
# ----------------------------------------------------------------------
acc_sample_idx = rng.integers(0, len(account_master), size=N_RECORDS)
txn_accounts = account_master.iloc[acc_sample_idx].reset_index(drop=True)
txn_customers = txn_accounts.merge(customer_master, on="customer_id", how="left")

df = pd.DataFrame()
df["transaction_id"] = [f"TXN{str(i).zfill(8)}" for i in range(1, N_RECORDS + 1)]
df["customer_id"] = txn_customers["customer_id"]
df["account_id"] = txn_customers["account_id"]
df["account_type"] = txn_customers["account_type"]
df["city"] = txn_customers["city"]
df["state"] = txn_customers["state"]
df["branch"] = txn_customers["branch"]
df["customer_age"] = txn_customers["customer_age"]
df["customer_gender"] = txn_customers["customer_gender"]
df["customer_risk_score"] = txn_customers["customer_risk_score"]

# transaction_date/time spanning ~1 year (2025-01-01 to 2025-12-31)
start_date = pd.Timestamp("2025-01-01")
day_offsets = rng.integers(0, 365, size=N_RECORDS)
df["transaction_date"] = (start_date + pd.to_timedelta(day_offsets, unit="D")).strftime("%Y-%m-%d")

seconds_in_day = rng.integers(0, 24 * 3600, size=N_RECORDS)
df["transaction_time"] = pd.to_timedelta(seconds_in_day, unit="s").astype(str).str.slice(-8)

df["transaction_type"] = rng.choice(TRANSACTION_TYPES, size=N_RECORDS, p=TRANSACTION_TYPE_WEIGHTS)
df["channel"] = rng.choice(CHANNELS, size=N_RECORDS, p=CHANNEL_WEIGHTS)
df["payment_method"] = rng.choice(PAYMENT_METHODS, size=N_RECORDS, p=PAYMENT_METHOD_WEIGHTS)
df["transaction_status"] = rng.choice(TRANSACTION_STATUSES, size=N_RECORDS, p=STATUS_WEIGHTS)
df["merchant_category"] = rng.choice(MERCHANT_CATEGORIES, size=N_RECORDS)
df["device_type"] = rng.choice(DEVICE_TYPES, size=N_RECORDS)

# ---- Realistic relationships between fields -------------------------
# ATM transactions -> channel ATM, device ATM Machine, withdrawal-heavy
atm_mask = df["channel"] == "ATM"
df.loc[atm_mask, "device_type"] = "ATM Machine"
df.loc[atm_mask, "transaction_type"] = rng.choice(
    ["Withdrawal", "Debit"], size=atm_mask.sum(), p=[0.85, 0.15]
)
df.loc[atm_mask, "payment_method"] = "Cash"

# POS channel -> card payments, device POS Terminal
pos_mask = df["channel"] == "POS"
df.loc[pos_mask, "device_type"] = "POS Terminal"
df.loc[pos_mask, "payment_method"] = rng.choice(
    ["Debit Card", "Credit Card"], size=pos_mask.sum(), p=[0.6, 0.4]
)
df.loc[pos_mask, "transaction_type"] = "Debit"

# Mobile/Internet banking -> Android/iOS/Web devices, UPI/NEFT/IMPS
mobile_mask = df["channel"] == "Mobile Banking"
df.loc[mobile_mask, "device_type"] = rng.choice(["Android", "iOS"], size=mobile_mask.sum(), p=[0.6, 0.4])
df.loc[mobile_mask, "payment_method"] = rng.choice(
    ["UPI", "IMPS", "NEFT"], size=mobile_mask.sum(), p=[0.6, 0.25, 0.15]
)

internet_mask = df["channel"] == "Internet Banking"
df.loc[internet_mask, "device_type"] = "Web"
df.loc[internet_mask, "payment_method"] = rng.choice(
    ["NEFT", "RTGS", "IMPS"], size=internet_mask.sum(), p=[0.5, 0.25, 0.25]
)

# Branch channel -> Cash/RTGS, no device
branch_mask = df["channel"] == "Branch"
df.loc[branch_mask, "device_type"] = "Web"
df.loc[branch_mask, "payment_method"] = rng.choice(
    ["Cash", "RTGS", "NEFT"], size=branch_mask.sum(), p=[0.5, 0.3, 0.2]
)

# ---- International transactions --------------------------------------
df["is_international"] = rng.choice([0, 1], size=N_RECORDS, p=[0.95, 0.05])
df["country"] = "India"
intl_mask = df["is_international"] == 1
df.loc[intl_mask, "country"] = rng.choice(FOREIGN_COUNTRIES, size=intl_mask.sum())
# international transactions only make sense via card/internet banking
df.loc[intl_mask, "channel"] = rng.choice(
    ["Internet Banking", "Mobile Banking"], size=intl_mask.sum(), p=[0.6, 0.4]
)
df.loc[intl_mask, "payment_method"] = rng.choice(
    ["Credit Card", "Debit Card", "RTGS"], size=intl_mask.sum(), p=[0.5, 0.3, 0.2]
)

# ---- Transaction amount depends on transaction_type ------------------
amount = np.zeros(N_RECORDS)
for t_type, (mean, sigma, low, high) in {
    "Debit": (2500, 3500, 50, 60000),
    "Credit": (5000, 6000, 100, 150000),
    "Transfer": (8000, 9000, 100, 250000),
    "Withdrawal": (3000, 2500, 100, 40000),
}.items():
    mask = df["transaction_type"] == t_type
    n = mask.sum()
    vals = np.clip(rng.lognormal(mean=np.log(mean), sigma=0.8, size=n), low, high)
    amount[mask.values] = vals
# international transactions tend to be larger
amount[intl_mask.values] *= rng.uniform(1.5, 3.0, size=intl_mask.sum())
df["amount"] = amount.round(2)

# ---- Balances ----------------------------------------------------------
df["balance_before"] = np.clip(rng.normal(50000, 40000, size=N_RECORDS), 500, 2_000_000).round(2)
credit_like = df["transaction_type"].isin(["Credit"])
balance_after = np.where(
    credit_like,
    df["balance_before"] + df["amount"],
    df["balance_before"] - df["amount"],
)
df["balance_after"] = np.clip(balance_after, 0, None).round(2)

# ---- Failed attempts: correlated with Failed status ---------------------
df["failed_attempts"] = 0
failed_mask = df["transaction_status"] == "Failed"
df.loc[failed_mask, "failed_attempts"] = rng.integers(1, 5, size=failed_mask.sum())
# a small fraction of "Success"/"Pending" also show minor retry attempts
some_success = (~failed_mask) & (rng.random(N_RECORDS) < 0.03)
df.loc[some_success, "failed_attempts"] = rng.integers(1, 2, size=some_success.sum())

# ---- previous_transaction_amount: shuffle amount within customer history ---
df = df.sort_values(["customer_id", "transaction_date", "transaction_time"]).reset_index(drop=True)
df["previous_transaction_amount"] = df.groupby("customer_id")["amount"].shift(1)
df["previous_transaction_amount"] = df["previous_transaction_amount"].fillna(df["amount"] * 0.9).round(2)

# Recompute transaction_id after sort so it stays unique & sequential-looking
df["transaction_id"] = [f"TXN{str(i).zfill(8)}" for i in range(1, len(df) + 1)]

# ---- Rapid-fire transaction bursts -------------------------------------
# A few customers make many transactions on the SAME day within minutes
# of each other - realistic behaviour that also exercises Rule 5
# (>=5 txns/customer/day) of the risk-scoring logic downstream.
burst_rows = []
burst_customers = rng.choice(customer_ids, size=25, replace=False)
for cid in burst_customers:
    cust_accounts = account_master.loc[account_master["customer_id"] == cid]
    acc_row = cust_accounts.sample(1, random_state=SEED).iloc[0]
    burst_date = start_date + pd.to_timedelta(int(rng.integers(0, 365)), unit="D")
    n_burst = int(rng.integers(5, 9))
    start_sec = int(rng.integers(0, 18 * 3600))
    for k in range(n_burst):
        amt = float(np.clip(rng.lognormal(mean=np.log(4000), sigma=0.6), 200, 50000))
        balance_before = float(rng.uniform(5000, 100000))
        t_sec = start_sec + int(rng.integers(60, 600)) * k  # spaced minutes apart
        burst_rows.append({
            "transaction_id": "PENDING",
            "customer_id": cid,
            "account_id": acc_row["account_id"],
            "account_type": acc_row["account_type"],
            "city": acc_row["city"],
            "state": acc_row["state"],
            "branch": acc_row["branch"],
            "customer_age": int(customer_master.loc[customer_master["customer_id"] == cid, "customer_age"].iloc[0]),
            "customer_gender": customer_master.loc[customer_master["customer_id"] == cid, "customer_gender"].iloc[0],
            "customer_risk_score": float(customer_master.loc[customer_master["customer_id"] == cid, "customer_risk_score"].iloc[0]),
            "transaction_date": burst_date.strftime("%Y-%m-%d"),
            "transaction_time": str(pd.to_timedelta(t_sec, unit="s"))[-8:],
            "transaction_type": rng.choice(["Debit", "Transfer"]),
            "channel": rng.choice(["Mobile Banking", "Internet Banking"]),
            "payment_method": rng.choice(["UPI", "IMPS"]),
            "transaction_status": "Success",
            "merchant_category": rng.choice(MERCHANT_CATEGORIES),
            "device_type": rng.choice(["Android", "iOS"]),
            "is_international": 0,
            "country": "India",
            "failed_attempts": 0,
            "previous_transaction_amount": amt * 0.9,
            "amount": round(amt, 2),
            "balance_before": round(balance_before, 2),
            "balance_after": round(balance_before - amt, 2),
        })
burst_df = pd.DataFrame(burst_rows)
df = pd.concat([df, burst_df[df.columns]], ignore_index=True)
df = df.sort_values(["customer_id", "transaction_date", "transaction_time"]).reset_index(drop=True)
df["transaction_id"] = [f"TXN{str(i).zfill(8)}" for i in range(1, len(df) + 1)]
print(f"Rapid-fire burst transactions injected: {len(burst_df)}")

print(f"Base transactions generated: {len(df)}")

# ----------------------------------------------------------------------
# STEP 3: INTENTIONALLY INJECT DATA QUALITY ISSUES
# ----------------------------------------------------------------------
n = len(df)

# --- Issue 1: Missing values (~1.5%) in several columns ---
for col, frac in [
    ("city", 0.02), ("customer_age", 0.01), ("merchant_category", 0.015),
    ("payment_method", 0.01), ("balance_after", 0.01),
]:
    idx = rng.choice(n, size=int(n * frac), replace=False)
    df.loc[idx, col] = np.nan

# --- Issue 2: Inconsistent capitalization in categorical fields ---
def scramble_case(series, variants_map, frac=0.15):
    idx = rng.choice(len(series), size=int(len(series) * frac), replace=False)
    s = series.copy()
    for i in idx:
        val = s.iloc[i]
        if val in variants_map:
            s.iloc[i] = rng.choice(variants_map[val])
    return s

payment_variants = {
    "UPI": ["upi", "Upi", "UPI"],
    "Debit Card": ["debit card", "DEBIT CARD", "Debit card"],
    "Credit Card": ["credit card", "CREDIT CARD", "Credit card"],
    "Cash": ["cash", "CASH"],
}
df["payment_method"] = scramble_case(df["payment_method"], payment_variants, frac=0.15)

gender_variants = {"Male": ["Male", "male", "M"], "Female": ["Female", "female", "F"]}
df["customer_gender"] = scramble_case(df["customer_gender"], gender_variants, frac=0.20)

type_variants = {
    "Debit": ["debit", "Debit", "DEBIT"],
    "Credit": ["credit", "Credit", "CREDIT"],
    "Transfer": ["transfer", "Transfer"],
    "Withdrawal": ["withdrawal", "Withdrawal"],
}
df["transaction_type"] = scramble_case(df["transaction_type"], type_variants, frac=0.10)

# --- Issue 3: Invalid transaction amounts (negative/zero) (~0.5%) ---
idx = rng.choice(n, size=int(n * 0.005), replace=False)
df.loc[idx, "amount"] = -df.loc[idx, "amount"].abs()

# --- Issue 4: Invalid / garbled transaction dates (~0.2%) ---
idx = rng.choice(n, size=int(n * 0.002), replace=False)
bad_dates = ["31-13-2025", "0000-00-00", "", "2025/02/30", "NaT"]
df.loc[idx, "transaction_date"] = rng.choice(bad_dates, size=len(idx))

# --- Issue 5: Impossible customer ages (~0.1%) ---
idx = rng.choice(n, size=int(n * 0.001), replace=False)
bad_ages = [4, 150, -5, 200]
df.loc[idx, "customer_age"] = rng.choice(bad_ages, size=len(idx))

# --- Issue 6: Duplicate transaction_id values (~0.3%) reused from other rows ---
idx = rng.choice(n, size=int(n * 0.003), replace=False)
donor_idx = rng.choice(n, size=len(idx), replace=False)
df.loc[idx, "transaction_id"] = df.loc[donor_idx, "transaction_id"].values

# --- Issue 7: Duplicate full rows (~0.5%) appended at the end ---
dup_rows = df.sample(n=int(n * 0.005), random_state=SEED)
df = pd.concat([df, dup_rows], ignore_index=True)

# Shuffle final dataset so duplicates aren't all at the tail (more realistic)
df = df.sample(frac=1, random_state=SEED).reset_index(drop=True)

# ----------------------------------------------------------------------
# STEP 4: Reorder columns to match spec & save
# ----------------------------------------------------------------------
COLUMN_ORDER = [
    "transaction_id", "customer_id", "account_id", "transaction_date",
    "transaction_time", "transaction_type", "amount", "balance_before",
    "balance_after", "account_type", "customer_age", "customer_gender",
    "city", "state", "branch", "channel", "merchant_category",
    "payment_method", "transaction_status", "device_type",
    "is_international", "country", "failed_attempts",
    "previous_transaction_amount", "customer_risk_score",
]
df = df[COLUMN_ORDER]

os.makedirs(OUTPUT_DIR, exist_ok=True)
df.to_csv(OUTPUT_PATH, index=False)

print(f"\nSaved raw dataset with {len(df)} rows -> {OUTPUT_PATH}")
print("\n--- Data quality issues intentionally injected ---")
print(f"Missing values:           spread across city/age/merchant_category/payment_method/balance_after")
print(f"Duplicate transaction_id: ~{int(n*0.003)} rows")
print(f"Duplicate full rows:      {len(dup_rows)} rows")
print(f"Invalid amounts (<=0):    ~{int(n*0.005)} rows")
print(f"Invalid dates:            ~{int(n*0.002)} rows")
print(f"Impossible ages:          ~{int(n*0.001)} rows")
print(f"Inconsistent capitalization introduced in payment_method, customer_gender, transaction_type")
