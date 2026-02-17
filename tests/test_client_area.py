def test_cliente_area_redireciona_sem_sessao(client):
    response = client.get("/minha-conta", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/login")


def test_cliente_area_renderiza_com_sessao(app_module, client, monkeypatch):
    email = "cliente@example.com"
    conta = {
        "email": email,
        "nome": "Cliente Teste",
        "telefone": "11999999999",
        "first_access_required": False,
    }

    monkeypatch.setattr(app_module, "buscar_conta_cliente_por_email", lambda e: conta if e == email else None)
    monkeypatch.setattr(app_module, "descriptografar_texto_cliente", lambda v: v)
    monkeypatch.setattr(app_module, "existe_quiz_submission", lambda **kwargs: True)
    monkeypatch.setattr(
        app_module,
        "montar_progresso_onboarding_cliente",
        lambda e: {
            "steps": [{"key": "install", "checked": True}],
            "done_count": 1,
            "total_steps": 1,
            "percent": 100,
        },
    )
    monkeypatch.setattr(app_module, "listar_pedidos_acesso_por_email", lambda e, limite=30: [])
    monkeypatch.setattr(app_module, "montar_curva_capital_plano", lambda pedido: {"labels": [], "values": []})
    monkeypatch.setattr(app_module, "buscar_afiliado_por_email", lambda e, apenas_ativos=False: None)
    monkeypatch.setattr(app_module, "gerar_csrf_token", lambda: "csrf-test")

    with client.session_transaction() as sess:
        sess[app_module.CLIENT_SESSION_EMAIL_KEY] = email

    response = client.get("/minha-conta")
    assert response.status_code == 200

