from flask import Flask, request, jsonify
from compactador import compactar_plano
from email_utils import enviar_email
import os
import json

app = Flask(__name__)

# ================= PLANOS =================
# A chave É O order_nsu enviado pela InfinitePay
PLANOS = {
    "trx-bronze-0001": {"nome": "TRX BRONZE", "pasta": "Licencas/TRX BRONZE"},
    "trx-prata-0001":  {"nome": "TRX PRATA",  "pasta": "Licencas/TRX PRATA"},
    "trx-gold-0001":   {"nome": "TRX GOLD",   "pasta": "Licencas/TRX GOLD"},
    "trx-black-0001":  {"nome": "TRX BLACK",  "pasta": "Licencas/TRX BLACK"},
    "trx_teste-0001":  {"nome": "TRX BRONZE", "pasta": "Licencas/TRX BRONZE"}
}
# ==========================================

PASTA_SAIDA = "saida"
ARQUIVO_PROCESSADOS = "processados.json"


# ---------------- UTILIDADES ----------------

def carregar_processados():
    if not os.path.exists(ARQUIVO_PROCESSADOS):
        return []

    try:
        with open(ARQUIVO_PROCESSADOS, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def salvar_processados(processados):
    with open(ARQUIVO_PROCESSADOS, "w", encoding="utf-8") as f:
        json.dump(processados, f)


# ---------------- WEBHOOK ----------------

@app.route("/webhook/infinitypay", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)

    print("📩 WEBHOOK RECEBIDO:")
    print(data)

    if not data:
        return jsonify({"msg": "Payload vazio"}), 200

    # Apenas pagamentos confirmados
    if data.get("status") != "paid":
        return jsonify({"msg": "Evento ignorado"}), 200

    pagamento_id = data.get("id") or data.get("transaction_id")
    order_nsu = data.get("order_nsu")

    cliente = data.get("customer", {})
    email = cliente.get("email")

    # Eventos incompletos → ignora com 200
    if not pagamento_id or not order_nsu or not email:
        print("⚠️ Evento incompleto ignorado")
        return jsonify({"msg": "Evento incompleto"}), 200

    processados = carregar_processados()

    # Evita duplicidade
    if pagamento_id in processados:
        print("🔁 Pagamento já processado:", pagamento_id)
        return jsonify({"msg": "Pagamento já processado"}), 200

    if order_nsu not in PLANOS:
        print("❌ Plano não reconhecido:", order_nsu)
        return jsonify({"msg": "Plano não reconhecido"}), 200

    plano = PLANOS[order_nsu]

    if not os.path.exists(plano["pasta"]):
        print("❌ Pasta do plano não encontrada")
        return jsonify({"error": "Erro interno"}), 500

    # -------- GERA ARQUIVO --------
    try:
        arquivo, senha = compactar_plano(plano["pasta"], PASTA_SAIDA)
    except Exception as e:
        print("❌ ERRO AO COMPACTAR:", e)
        return jsonify({"error": "Erro ao gerar arquivo"}), 500

    # -------- ENVIA EMAIL --------
    try:
        print("📧 Enviando email para:", email)
        enviar_email(
            destinatario=email,
            nome_plano=plano["nome"],
            arquivo=arquivo,
            senha=senha
        )
    except Exception as e:
        print("❌ ERRO AO ENVIAR EMAIL:", e)
        return jsonify({"error": "Falha ao enviar email"}), 500

    # -------- MARCA COMO PROCESSADO --------
    processados.append(pagamento_id)
    salvar_processados(processados)

    # -------- LIMPA ARQUIVO --------
    try:
        os.remove(arquivo)
    except Exception:
        pass

    print("✅ PROCESSO FINALIZADO COM SUCESSO")

    return jsonify({"msg": "Plano enviado com sucesso"}), 200


# ---------------- START ----------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    os.makedirs(PASTA_SAIDA, exist_ok=True)
    app.run(host="0.0.0.0", port=port)
