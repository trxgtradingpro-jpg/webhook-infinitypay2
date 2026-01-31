import os
import random
import string
import subprocess

def gerar_senha(tamanho=16):
    chars = string.ascii_letters + string.digits + "!@#$%&*"
    return ''.join(random.choice(chars) for _ in range(tamanho))

def compactar_plano(pasta_plano, pasta_saida):
    senha = gerar_senha()
    nome = os.path.basename(pasta_plano)
    zip_saida = os.path.join(pasta_saida, f"{nome}.zip")

    os.makedirs(pasta_saida, exist_ok=True)

    comando = ["zip", "-r", "-P", senha, zip_saida, pasta_plano]
    subprocess.run(comando)

    return zip_saida, senha
