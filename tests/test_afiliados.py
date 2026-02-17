def test_admin_afiliado_adicionar_sucesso(app_module, client, monkeypatch):
    monkeypatch.setattr(app_module, "validar_csrf_token", lambda token: True)
    monkeypatch.setattr(app_module, "criar_afiliado", lambda **kwargs: True)

    with client.session_transaction() as sess:
        sess["admin"] = True

    response = client.post(
        "/admin/afiliados/adicionar",
        data={
            "csrf_token": "ok",
            "nome": "Afiliado Teste",
            "slug": "afiliado-teste",
            "email": "afiliado@example.com",
            "telefone": "11999999999",
            "ativo": "on",
        },
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert "/admin/afiliados?ok=1" in response.headers["Location"]


def test_admin_afiliado_adicionar_sem_login_redireciona(app_module, client, monkeypatch):
    monkeypatch.setattr(app_module, "validar_csrf_token", lambda token: True)
    response = client.post(
        "/admin/afiliados/adicionar",
        data={"csrf_token": "ok"},
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/admin/login")
