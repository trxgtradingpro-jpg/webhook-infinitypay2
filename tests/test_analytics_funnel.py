def test_api_funnel_track_cta_publico(app_module, client, monkeypatch):
    calls = []
    monkeypatch.setattr(app_module, "_origem_confiavel_request", lambda: True)
    monkeypatch.setattr(
        app_module,
        "registrar_evento_funil",
        lambda *args, **kwargs: calls.append({"args": args, "kwargs": kwargs}) or True
    )

    response = client.post(
        "/api/funnel/track",
        json={
            "event_name": "cta_click",
            "cta_id": "hero_ativar",
            "destination": "/checkout/trx-gold",
            "source": "index"
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["tracked"] is True
    assert calls
    assert calls[0]["kwargs"]["stage"] == app_module.FUNNEL_STAGE_CTA_CLICK


def test_api_analytics_funnel_summary_admin(app_module, client, monkeypatch):
    with client.session_transaction() as sess:
        sess["admin"] = True

    monkeypatch.setattr(
        app_module,
        "carregar_eventos_funil_analytics_filtrados",
        lambda **kwargs: [
            {"stage": "visit", "visitor_key": "v1", "created_at": None},
            {"stage": "visit", "visitor_key": "v2", "created_at": None},
            {"stage": "cta_click", "visitor_key": "v1", "created_at": None, "meta": {"cta_id": "hero_ativar"}},
            {"stage": "checkout_submit", "user_key": "u1@example.com", "created_at": None},
            {"stage": "payment_confirmed", "user_key": "u1@example.com", "plano": "trx-gold", "created_at": None},
            {"stage": "activation", "user_key": "u1@example.com", "created_at": None},
            {"stage": "retention", "user_key": "u1@example.com", "created_at": None},
        ]
    )

    response = client.get("/api/analytics/funnel-summary?start=2026-01-01&end=2026-01-31")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["stage_counts"]["visit"] == 2
    assert payload["stage_counts"]["cta_click"] == 1
    assert payload["stage_counts"]["checkout_submit"] == 1
    assert payload["stage_counts"]["payment_confirmed"] == 1
    assert payload["stage_counts"]["activation"] == 1
    assert payload["stage_counts"]["retention"] == 1

    conversion_visit_cta = next(item for item in payload["conversions"] if item["from"] == "visit" and item["to"] == "cta_click")
    assert conversion_visit_cta["rate_percent"] == 50.0
