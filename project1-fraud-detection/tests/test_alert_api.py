"""
test_alert_api.py
Unit tests for the FastAPI fraud alert API.
Run: pytest tests/test_alert_api.py -v
"""

import pytest
from fastapi.testclient import TestClient
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))
from alert_api import app

client = TestClient(app)


class TestHealthCheck:
    def test_health_returns_ok(self):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_health_has_timestamp(self):
        response = client.get("/health")
        assert "timestamp" in response.json()


class TestGetAlerts:
    def test_returns_list(self):
        response = client.get("/alerts")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_default_limit_is_10(self):
        response = client.get("/alerts")
        assert len(response.json()) == 10

    def test_custom_limit(self):
        response = client.get("/alerts?limit=5")
        assert len(response.json()) == 5

    def test_alert_has_required_fields(self):
        response = client.get("/alerts?limit=1")
        alert = response.json()[0]
        required = ["alert_id", "transaction_id", "card_id", "amount",
                    "fraud_score", "alert_severity", "fraud_flags"]
        for field in required:
            assert field in alert, f"Missing field: {field}"

    def test_fraud_score_in_range(self):
        response = client.get("/alerts?limit=20")
        for alert in response.json():
            assert 0.0 <= alert["fraud_score"] <= 1.0

    def test_severity_filter_high(self):
        response = client.get("/alerts?severity=HIGH&limit=50")
        alerts = response.json()
        for alert in alerts:
            assert alert["alert_severity"] == "HIGH"

    def test_severity_filter_case_insensitive(self):
        response = client.get("/alerts?severity=high&limit=50")
        assert response.status_code == 200

    def test_invalid_limit_too_large(self):
        response = client.get("/alerts?limit=999")
        assert response.status_code == 422   # validation error

    def test_invalid_limit_zero(self):
        response = client.get("/alerts?limit=0")
        assert response.status_code == 422


class TestGetAlertByTransaction:
    def test_returns_alert_for_valid_id(self):
        txn_id = "test-txn-123"
        response = client.get(f"/alerts/{txn_id}")
        assert response.status_code == 200
        assert response.json()["transaction_id"] == txn_id

    def test_has_fraud_flags_list(self):
        response = client.get("/alerts/some-txn-id")
        alert = response.json()
        assert isinstance(alert["fraud_flags"], list)
        assert len(alert["fraud_flags"]) >= 1


class TestSummary:
    def test_summary_returns_correct_shape(self):
        response = client.get("/summary")
        assert response.status_code == 200
        data = response.json()
        required = ["total_alerts", "high_severity", "medium_severity",
                    "low_severity", "total_amount_at_risk"]
        for field in required:
            assert field in data

    def test_severity_counts_sum_to_total(self):
        response = client.get("/summary")
        data = response.json()
        assert data["high_severity"] + data["medium_severity"] + data["low_severity"] \
               == data["total_alerts"]


class TestResolveAlert:
    def test_resolve_returns_success(self):
        response = client.post("/alerts/txn-abc-123/resolve?resolved_by=analyst1")
        assert response.status_code == 200
        data = response.json()
        assert "resolved_at" in data
        assert "txn-abc-123" in data["message"]
