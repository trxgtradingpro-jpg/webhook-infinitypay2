def test_compra_plano_pago_redireciona_checkout(app_module, client, monkeypatch):
    monkeypatch.setattr(app_module, "validar_csrf_token", lambda token: True)
    monkeypatch.setattr(app_module, "listar_pedidos_pagos_por_email", lambda email, limite=1: [])
    monkeypatch.setattr(app_module, "resolver_afiliado_para_compra", lambda **kwargs: (None, {}))
    monkeypatch.setattr(app_module, "salvar_order", lambda **kwargs: None)
    monkeypatch.setattr(app_module, "criar_checkout_dinamico", lambda **kwargs: "https://checkout.example/mock")

    response = client.post(
        "/comprar",
        data={
            "csrf_token": "ok",
            "nome": "Gui Trader",
            "email": "gui@example.com",
            "telefone": "11999999999",
            "plano": "trx-gold",
        },
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["Location"] == "https://checkout.example/mock"


def test_compra_get_redireciona_home(client):
    response = client.get("/comprar", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/")

