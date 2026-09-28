"""
risk_analysis.py
------------------
Implements the SAME explainable, rule-based risk scoring logic as
sql/risk_analysis.sql, but in Pandas, so the pipeline can produce a
risk-scored dataset locally even without a running MySQL server.

DISCLAIMER: This is a SYNTHETIC, rule-based risk analysis. It does NOT
detect real financial fraud - it flags transactions matching simple,
transparent business rules for review.

RISK RULES (each rule contributes points to risk_score):
    Rule 1 - Very high amount:          amount > 5x customer's own
                                        average amount            -> +40
    Rule 2 - Multiple failures:         this txn failed AND the
                                        customer has >=3 total
                                        failed transactions       -> +20
    Rule 3 - Unusual international:     international txn with
                                        amount > 3x customer avg  -> +30
    Rule 4 - Rapid consecutive:         amount > 2x the customer's
                                        previous transaction amt  -> +10
    Rule 5 - High daily activity:       customer made >=5 txns on
                                        the same day              -> +15
    Rule 6 - Above customer average:    amount > 2x customer avg  -> +10

RISK LEVELS:
    score == 0          -> LOW
    score 1-39          -> MEDIUM
    score >= 40         -> HIGH

Outputs:
    data/processed/risk_transactions.csv          (transaction-level risk)
    data/processed/customer_risk_profile.csv      (customer-level profile)
    outputs/reports/risk_summary_report.csv       (overall risk summary)
"""

import os
import pandas as pd
import numpy as np

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
CLEANED_PATH = os.path.join(BASE_DIR, "data", "processed", "banking_transactions_cleaned.csv")
RISK_TXN_PATH = os.path.join(BASE_DIR, "data", "processed", "risk_transactions.csv")
RISK_CUST_PATH = os.path.join(BASE_DIR, "data", "processed", "customer_risk_profile.csv")
REPORT_PATH = os.path.join(BASE_DIR, "outputs", "reports", "risk_summary_report.csv")

RULE_WEIGHTS = {
    "is_high_amount": 40,
    "has_multiple_failures": 20,
    "is_unusual_international": 30,
    "is_rapid_consecutive": 10,
    "is_high_daily_activity": 15,
    "above_customer_avg": 10,
}


def score_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])

    # Baselines used by multiple rules
    customer_avg = df.groupby("customer_id")["amount"].transform("mean")
    daily_counts = df.groupby(["customer_id", "transaction_date"])["transaction_id"].transform("count")
    customer_failures = (df["transaction_status"] == "Failed").groupby(df["customer_id"]).transform("sum")

    # --- Rules ---
    df["customer_avg_amount"] = customer_avg.round(2)
    df["is_high_amount"] = (df["amount"] > 5 * customer_avg).astype(int)
    df["has_multiple_failures"] = ((df["transaction_status"] == "Failed") & (customer_failures >= 3)).astype(int)
    df["is_unusual_international"] = ((df["is_international"] == 1) & (df["amount"] > 3 * customer_avg)).astype(int)
    df["is_rapid_consecutive"] = (df["amount"] > 2 * df["previous_transaction_amount"]).astype(int)
    df["is_high_daily_activity"] = (daily_counts >= 5).astype(int)
    df["above_customer_avg"] = (df["amount"] > 2 * customer_avg).astype(int)

    # --- Total score & level ---
    score = sum(df[rule] * w for rule, w in RULE_WEIGHTS.items())
    df["risk_score"] = score.astype(int)
    df["risk_level"] = np.select(
        [df["risk_score"] >= 40, df["risk_score"] >= 10],
        ["HIGH", "MEDIUM"],
        default="LOW",
    )
    return df


def build_customer_risk_profile(df: pd.DataFrame, scored: pd.DataFrame) -> pd.DataFrame:
    profile = scored.groupby("customer_id").agg(
        total_transactions=("transaction_id", "count"),
        total_transaction_amount=("amount", "sum"),
        avg_transaction_amount=("amount", "mean"),
        max_transaction_amount=("amount", "max"),
        failed_transactions=("transaction_status", lambda s: (s == "Failed").sum()),
        international_transactions=("is_international", "sum"),
        high_risk_transactions=("risk_level", lambda s: (s == "HIGH").sum()),
        medium_risk_transactions=("risk_level", lambda s: (s == "MEDIUM").sum()),
        avg_risk_score=("risk_score", "mean"),
        max_risk_score=("risk_score", "max"),
    ).reset_index()

    for col in ["total_transaction_amount", "avg_transaction_amount", "max_transaction_amount", "avg_risk_score"]:
        profile[col] = profile[col].round(2)
    return profile


def main():
    df = pd.read_csv(CLEANED_PATH)
    scored = score_transactions(df)
    profile = build_customer_risk_profile(df, scored)

    os.makedirs(os.path.dirname(RISK_TXN_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    scored.to_csv(RISK_TXN_PATH, index=False)
    profile.to_csv(RISK_CUST_PATH, index=False)

    level_dist = scored["risk_level"].value_counts().rename("count")
    level_dist_pct = (level_dist / len(scored) * 100).round(2)
    rule_hits = {rule: int(scored[rule].sum()) for rule in RULE_WEIGHTS}

    report = pd.DataFrame(
        {
            "metric": [
                "Total scored transactions",
                "LOW risk transactions", "LOW risk %",
                "MEDIUM risk transactions", "MEDIUM risk %",
                "HIGH risk transactions", "HIGH risk %",
                *[f"Rule hits: {r}" for r in RULE_WEIGHTS],
                "Customers with >=1 HIGH risk txn",
            ],
            "value": [
                len(scored),
                int(level_dist.get("LOW", 0)), level_dist_pct.get("LOW", 0),
                int(level_dist.get("MEDIUM", 0)), level_dist_pct.get("MEDIUM", 0),
                int(level_dist.get("HIGH", 0)), level_dist_pct.get("HIGH", 0),
                *rule_hits.values(),
                int(profile.loc[profile["high_risk_transactions"] > 0, "customer_id"].nunique()),
            ],
        }
    )
    report.to_csv(REPORT_PATH, index=False)

    print(report.to_string(index=False))
    print(f"\nSaved -> {RISK_TXN_PATH}\nSaved -> {RISK_CUST_PATH}\nSaved -> {REPORT_PATH}")


if __name__ == "__main__":
    main()
