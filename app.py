from flask import Flask, request, jsonify
from compactador import compactar_plano
from email_utils import enviar_email
import os

app = Flask(__name__)

# ================= PLANOS =================
PLANOS = {
    "chk_abc123": {"nome": "TRX BRONZE", "pasta": r"Licencas/TRX BRONZE"},
    "chk_def456": {"nome": "TRX PRATA", "pasta": r"Licencas/TRX PRATA"},
    "chk_ghi789": {"nome": "TRX GOLD", "pasta": r"Licencas/TRX GOLD"},
    "chk_jkl000": {"nome": "TRX BLACK", "pasta": r"Licencas/TRX BLACK"}
}
# ==========================================

PASTA_SAIDA = "saida"

@app.route("/webhook/infinitypay", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)

    if not data:
        return jsonify({"error": "Payload inválido"}), 400

    status = data.get("status")
    plano_id = data.get("product_id")
    email = data.get("customer", {}).get("email")

    if status != "paid":
        return jsonify({"msg": "Pagamento não aprovado"}), 200

    if not email:
        return jsonify({"error": "Email não encontrado"}), 400

    if plano_id not in PLANOS:
        return jsonify({"error": "Plano não reconhecido"}), 400

    plano = PLANOS[plano_id]

    if not os.path.exists(plano["pasta"]):
        return jsonify({"error": "Pasta do plano não encontrada"}), 500

    arquivo, senha = compactar_plano(plano["pasta"], PASTA_SAIDA)

    enviar_email(
        destinatario=email,
        nome_plano=plano["nome"],
        arquivo=arquivo,
        senha=senha
    )

    return jsonify({"msg": "Plano enviado com sucesso"}), 200


if __name__ == "__main__":
    os.makedirs(PASTA_SAIDA, exist_ok=True)
    app.run(port=5000)
