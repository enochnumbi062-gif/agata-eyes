"""API contract tests. No network: Supabase and Gemini are unset in CI."""
import main
from fastapi.testclient import TestClient

client = TestClient(main.app)


def test_health_contract():
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "AgAtA-EyEs"


def test_unknown_horizons_target_is_rejected():
    assert client.get("/api/v1/jpl/vectors", params={"target": "arbitrary-command"}).status_code == 400


def test_invalid_horizons_date_is_rejected():
    params = {"target": "earth", "start": "2026-13-45", "stop": "2026-13-46"}
    assert client.get("/api/v1/jpl/vectors", params=params).status_code == 400


def test_invalid_norad_id_is_rejected():
    assert client.get("/api/v1/satellites/0").status_code == 400


def test_unknown_satellite_group_is_rejected():
    assert client.get("/api/v1/satellites", params={"group": "nope"}).status_code == 400


def test_status_reports_storage_mode():
    body = client.get("/api/v1/status").json()
    assert body["database"] in {"memory", "supabase"}
    assert "enabled" in body["ai"]
    assert set(body["supabase_env"]) == {"url", "key"}


def test_ai_requires_configuration(monkeypatch):
    monkeypatch.setattr(main, "GEMINI_API_KEY", "")
    assert client.post("/api/v1/ai/analyze", json={"question": "Bonjour"}).status_code == 503


def test_ai_rejects_too_short_question(monkeypatch):
    monkeypatch.setattr(main, "GEMINI_API_KEY", "test-key")
    assert client.post("/api/v1/ai/analyze", json={"question": "a"}).status_code == 422


def test_parse_vectors_reads_horizons_csv_block():
    result = ("header\n$$SOE\n2461322.5, A.D. 2026-Oct-09 00:00:00.0000, 1.0E+08, 2.0E+07, 3.0E+06, "
              "1.0E+00, 2.0E+01, 3.0E-01, 5.0E+02, 1.0E+08, 1.0E-01,\n$$EOE\ntrailer")
    rows = main.parse_vectors({"result": result})
    assert len(rows) == 1
    assert rows[0]["x"] == 1.0e8 and rows[0]["date"].startswith("A.D. 2026-Oct-09")
