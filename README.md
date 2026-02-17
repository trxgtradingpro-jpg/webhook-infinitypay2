# WhatsApp Sender (Baileys + Express)

Microserviço independente em Node.js para envio de mensagens WhatsApp via WhatsApp Web (Baileys), consumido por outro backend via HTTP.

## Requisitos

- Node.js 18+
- Variáveis de ambiente configuradas

## Variáveis de ambiente

- `PORT` (padrão: `10000`)
- `WA_SENDER_TOKEN` (**obrigatório**)
- `AUTH_DIR` (padrão: `./auth`)
- `MIN_SECONDS_BETWEEN_SAME_NUMBER` (padrão: `60`)

Se `WA_SENDER_TOKEN` não estiver definido, o serviço encerra na inicialização.

## Como rodar local

```bash
npm install
WA_SENDER_TOKEN="seu-token" npm start
```

Opcionalmente definindo todas as envs:

```bash
PORT=10000 \
WA_SENDER_TOKEN="seu-token" \
AUTH_DIR="./auth" \
MIN_SECONDS_BETWEEN_SAME_NUMBER=60 \
npm start
```

## Endpoints

### `GET /`
Retorna estado do serviço:

```json
{
  "ok": true,
  "connected": false,
  "has_qr": true
}
```

### `GET /qr`
Retorna QR atual em texto:

```json
{
  "ok": true,
  "qr": "codigo-qr"
}
```

### `POST /send`
Autenticação obrigatória:

`Authorization: Bearer {WA_SENDER_TOKEN}`

Body JSON:

```json
{
  "phone": "5511999999999",
  "message": "texto da mensagem",
  "order_id": "id-opcional"
}
```

Regras:
- telefone E.164 sem `+` (10 a 15 dígitos)
- `message` obrigatório
- rate limit por número (`MIN_SECONDS_BETWEEN_SAME_NUMBER`)
- envio somente com WhatsApp conectado

Resposta de sucesso:

```json
{
  "ok": true,
  "order_id": "id-opcional",
  "message_id": "..."
}
```

Resposta de erro:

```json
{
  "ok": false,
  "error": "descrição"
}
```

## Como escanear QR

1. Suba o serviço.
2. Chame `GET /qr` para obter o texto QR.
3. Escaneie no WhatsApp (Aparelhos conectados).
4. Após conexão, `GET /` retorna `connected: true`.

## Deploy no Render

1. Crie um Web Service no Render apontando para este repositório.
2. Build Command: `npm install`
3. Start Command: `npm start`
4. Configure env vars:
   - `WA_SENDER_TOKEN` (obrigatório)
   - `PORT` (opcional)
   - `AUTH_DIR` (recomendado `/var/data/auth`)
   - `MIN_SECONDS_BETWEEN_SAME_NUMBER`

## Disk Persistente (Render)

Para manter sessão entre deploys/restarts:

1. No Render, adicione **Persistent Disk**.
2. Monte o disco em `/var/data`.
3. Defina `AUTH_DIR=/var/data/auth`.

Sem disco persistente, será necessário escanear QR novamente após reinícios.

## Teste com curl

Health:

```bash
curl -s http://localhost:10000/
```

QR:

```bash
curl -s http://localhost:10000/qr
```

Envio:

```bash
curl -X POST http://localhost:10000/send \
  -H "Authorization: Bearer seu-token" \
  -H "Content-Type: application/json" \
  -d '{
    "phone":"5511999999999",
    "message":"Olá! Teste de envio.",
    "order_id":"pedido-123"
  }'
```

## Backend tests and CI/CD

Test suite (pytest):
- login (`/login`)
- purchase (`/comprar`)
- webhook (`/webhook/infinitypay`)
- client area (`/minha-conta`)
- affiliates (`/admin/afiliados/*`)

Run locally:

```bash
python -m pip install -r requirements.txt
python -m pip install pytest
APP_SKIP_DB_INIT=true BACKGROUND_WORKERS_ENABLED=false pytest
```

GitHub Actions:
- `.github/workflows/ci.yml`: mandatory validation on PR/push (compile + tests)
- `.github/workflows/deploy.yml`: deploy pipeline with pre-deploy validation and post-deploy healthcheck

Required repository secrets for deploy:
- `PRODUCTION_DEPLOY_WEBHOOK_URL`
- `PRODUCTION_HEALTHCHECK_URL`
- optional: `PRODUCTION_APP_URL` (for environment URL display)
