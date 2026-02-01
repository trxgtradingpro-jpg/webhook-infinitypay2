from flask import Flask, request, jsonify
from compactador import compactar_plano
from email_utils import enviar_email
import os

app = Flask(__name__)

# ================= PLANOS =================
PLANOS = {
    "chk_abc123": {"nome": "TRX BRONZE", "pasta": "Licencas/TRX BRONZE"},
    "chk_def456": {"nome": "TRX PRATA", "pasta": "Licencas/TRX PRATA"},
    "chk_ghi789": {"nome": "TRX GOLD", "pasta": "Licencas/TRX GOLD"},
    "chk_jkl000": {"nome": "TRX BLACK", "pasta": "Licencas/TRX BLACK"},
}
# ==========================================

PASTA_SAIDA = "saida"


@app.route("/webhook/infinitypay", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)

    if not data:
        return jsonify({"error": "Payload inválido"}), 400

    # Confirma pagamento
    if data.get("status") != "paid":
        return jsonify({"msg": "Pagamento não aprovado"}), 200

    plano_id = data.get("product_id")
    cliente = data.get("customer", {})

    email = cliente.get("email")
    nome_cliente = cliente.get("name", "Cliente")

    if not email:
        return jsonify({"error": "Email não encontrado"}), 400

    if plano_id not in PLANOS:
        return jsonify({"error": "Plano não reconhecido"}), 400

    plano = PLANOS[plano_id]

    if not os.path.exists(plano["pasta"]):
        return jsonify({"error": "Pasta do plano não encontrada"}), 500

    # Gera ZIP + senha
    arquivo, senha = compactar_plano(plano["pasta"], PASTA_SAIDA)

    # Envia email (com ZIP)
    enviar_email(
        destinatario=email,
        nome_cliente=nome_cliente,
        nome_plano=plano["nome"],
        arquivo=arquivo,
        senha=senha
    )

    # Remove o ZIP após envio (opcional, recomendado)
    try:
        os.remove(arquivo)
    except Exception:
        pass

    return jsonify({"msg": "Plano enviado com sucesso"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    os.makedirs(PASTA_SAIDA, exist_ok=True)
    app.run(host="0.0.0.0", port=port)
