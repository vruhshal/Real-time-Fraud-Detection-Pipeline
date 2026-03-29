"""
alert_api.py
FastAPI service that serves fraud alerts from BigQuery.
Run: uvicorn alert_api:app --host 0.0.0.0 --port 8000
Docs: http://localhost:8000/docs
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import random
import uuid

app = FastAPI(
    title="Fraud Detection Alert API",
    description="Serves real-time fraud alerts from scored transaction pipeline",
    version="1.0.0",
)


# ── Pydantic models ────────────────────────────────────────────────────────────
class FraudAlert(BaseModel):
    alert_id: str
    transaction_id: str
    card_id: str
    amount: float
    merchant_name: str
    merchant_country: str
    transaction_timestamp: datetime
    fraud_score: float          # 0.0 to 1.0
    fraud_flags: List[str]      # e.g. ["is_velocity_spike", "is_foreign_merchant"]
    alert_severity: str         # LOW / MEDIUM / HIGH


class AlertSummary(BaseModel):
    total_alerts: int
    high_severity: int
    medium_severity: int
    low_severity: int
    total_amount_at_risk: float


# ── Helpers ────────────────────────────────────────────────────────────────────
def mock_alert() -> FraudAlert:
    """Generate a mock fraud alert (replaces BQ query in demo)."""
    score = round(random.uniform(0.6, 0.99), 3)
    flags = random.sample(
        ["is_velocity_spike", "is_foreign_merchant", "is_night_txn",
         "is_high_amount", "is_amount_anomaly"],
        k=random.randint(1, 3),
    )
    severity = "HIGH" if score > 0.85 else ("MEDIUM" if score > 0.70 else "LOW")

    return FraudAlert(
        alert_id=str(uuid.uuid4()),
        transaction_id=str(uuid.uuid4()),
        card_id=f"4532-XXXX-XXXX-{random.randint(1000,9999)}",
        amount=round(random.uniform(5000, 95000), 2),
        merchant_name=random.choice(["GlobalShop Inc", "FastPay Ltd", "QuickMart"]),
        merchant_country=random.choice(["US", "UK", "SG"]),
        transaction_timestamp=datetime.utcnow(),
        fraud_score=score,
        fraud_flags=flags,
        alert_severity=severity,
    )


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.get("/health")
def health_check():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/alerts", response_model=List[FraudAlert])
def get_alerts(
    severity: Optional[str] = Query(None, description="Filter: HIGH / MEDIUM / LOW"),
    limit: int = Query(10, ge=1, le=100),
):
    """
    Returns recent fraud alerts.
    In production this queries: SELECT * FROM scored_transactions
    WHERE fraud_score > 0.6 ORDER BY transaction_timestamp DESC LIMIT {limit}
    """
    alerts = [mock_alert() for _ in range(limit)]
    if severity:
        alerts = [a for a in alerts if a.alert_severity == severity.upper()]
    return alerts


@app.get("/alerts/{transaction_id}", response_model=FraudAlert)
def get_alert_by_transaction(transaction_id: str):
    """Fetch a specific alert by transaction ID."""
    # In production: query BQ WHERE transaction_id = @transaction_id
    alert = mock_alert()
    alert.transaction_id = transaction_id
    return alert


@app.get("/summary", response_model=AlertSummary)
def get_summary():
    """Returns alert summary stats for the last 24 hours."""
    total = random.randint(120, 350)
    high = random.randint(10, 40)
    medium = random.randint(30, 80)
    low = total - high - medium
    return AlertSummary(
        total_alerts=total,
        high_severity=high,
        medium_severity=medium,
        low_severity=low,
        total_amount_at_risk=round(random.uniform(500000, 2000000), 2),
    )


@app.post("/alerts/{transaction_id}/resolve")
def resolve_alert(transaction_id: str, resolved_by: str = "analyst"):
    """Mark an alert as resolved (false positive or actioned)."""
    return {
        "message": f"Alert {transaction_id} resolved by {resolved_by}",
        "resolved_at": datetime.utcnow().isoformat(),
    }
