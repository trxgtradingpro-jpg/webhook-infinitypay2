from flask import Flask, request, jsonify
from compactador import compactar_plano
from email_utils import enviar_email
import os

app = Flask(__name__)

PLANOS = {
    "chk_abc123": {"nome": "TRX BRONZE", "pasta": "Licencas/TRX BRONZE"}
}

PASTA_SAIDA = "saida"

@app.route("/webhook/infinitypay", methods=["POST"])
def webhook():
    data = request.get_json(force=True)

    if data.get("status") != "paid":
        return jsonify({"msg": "Pagamento não aprovado"}), 200

    plano = PLANOS[data["product_id"]]
    email = data["customer"]["email"]

    arquivo, senha = compactar_plano(plano["pasta"], PASTA_SAIDA)

    enviar_email(
        destinatario=email,
        nome_plano=plano["nome"],
        arquivo=arquivo,
        senha=senha
    )

    return jsonify({"msg": "Plano enviado"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
