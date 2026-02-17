from werkzeug.security import generate_password_hash


def test_cliente_login_sucesso(app_module, client, monkeypatch):
    senha = "SenhaForte@123"
    email = "cliente@example.com"
    conta = {
        "email": email,
        "password_hash": generate_password_hash(senha),
        "first_access_required": False,
    }

    monkeypatch.setattr(app_module, "validar_csrf_token", lambda token: True)
    monkeypatch.setattr(app_module, "buscar_conta_cliente_por_email", lambda e: conta if e == email else None)
    monkeypatch.setattr(app_module, "conta_cliente_requer_configuracao_senha", lambda c: False)
    monkeypatch.setattr(app_module, "atualizar_ultimo_login_conta_cliente", lambda e: True)
    monkeypatch.setattr(app_module, "limpar_remember_token_cliente", lambda e: True)

    response = client.post(
        "/login",
        data={
            "csrf_token": "ok",
            "email": email,
            "senha": senha,
        },
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/minha-conta")


def test_cliente_login_csrf_invalido(app_module, client, monkeypatch):
    monkeypatch.setattr(app_module, "validar_csrf_token", lambda token: False)
    response = client.post(
        "/login",
        data={"csrf_token": "bad", "email": "x@y.com", "senha": "123"},
        follow_redirects=False,
    )
    assert response.status_code == 403

