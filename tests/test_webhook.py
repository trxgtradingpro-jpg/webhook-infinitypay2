def test_webhook_sucesso(app_module, client, monkeypatch):
    order_id = "order-123"
    transaction_nsu = "txn-abc"
    funnel_calls = []
    order_pendente = {
        "order_id": order_id,
        "status": "PENDENTE",
        "plano": "trx-gold",
        "email": "cliente@example.com",
        "nome": "Cliente",
        "telefone": "11999999999",
    }

    calls = {"buscar": 0}

    def fake_buscar_order(order):
        calls["buscar"] += 1
        if calls["buscar"] == 1:
            return dict(order_pendente)
        retorno = dict(order_pendente)
        retorno["status"] = "PAGO"
        return retorno

    monkeypatch.setattr(app_module, "verificar_token_webhook", lambda: True)
    monkeypatch.setattr(app_module, "transacao_ja_processada", lambda nsu: False)
    monkeypatch.setattr(app_module, "buscar_order_por_id", fake_buscar_order)
    monkeypatch.setattr(app_module, "reservar_order_para_processamento", lambda order: True)
    monkeypatch.setattr(app_module, "compactar_plano", lambda pasta, saida: ("arquivo.zip", "senha"))
    monkeypatch.setattr(app_module, "enviar_email_com_retry", lambda order, plano, arquivo, senha: True)
    monkeypatch.setattr(app_module, "marcar_order_processada", lambda order: None)
    monkeypatch.setattr(app_module, "marcar_transacao_processada", lambda nsu: None)
    monkeypatch.setattr(app_module, "garantir_conta_cliente_para_order", lambda *args, **kwargs: None)
    monkeypatch.setattr(app_module, "registrar_comissao_pedido_afiliado", lambda *args, **kwargs: None)
    monkeypatch.setattr(app_module, "conceder_bonus_indicacao_pedido", lambda *args, **kwargs: None)
    monkeypatch.setattr(app_module, "registrar_compra_analytics", lambda *args, **kwargs: None)
    monkeypatch.setattr(app_module, "agendar_whatsapp_pos_pago", lambda order: None)
    monkeypatch.setattr(
        app_module,
        "registrar_evento_funil",
        lambda *args, **kwargs: funnel_calls.append({"args": args, "kwargs": kwargs}) or True
    )

    response = client.post(
        "/webhook/infinitypay",
        json={
            "transaction_nsu": transaction_nsu,
            "order_nsu": order_id,
            "paid_amount": 49700,
        },
    )

    assert response.status_code == 200
    assert response.get_json()["msg"] == "OK"
    assert any(
        call.get("kwargs", {}).get("stage") == app_module.FUNNEL_STAGE_PAYMENT_CONFIRMED
        for call in funnel_calls
    )


def test_webhook_nao_autorizado(app_module, client, monkeypatch):
    monkeypatch.setattr(app_module, "verificar_token_webhook", lambda: False)
    response = client.post("/webhook/infinitypay", json={"order_nsu": "x"})
    assert response.status_code == 401
