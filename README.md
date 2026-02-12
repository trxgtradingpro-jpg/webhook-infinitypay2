# webhook-infinitypay

## WhatsApp automático para plano grátis

Para enviar uma mensagem de WhatsApp **5 minutos** após a liberação do plano `trx-gratis`, configure estas variáveis:

- `WHATSAPP_API_URL` (obrigatória): endpoint HTTP da sua integração WhatsApp.
- `WHATSAPP_API_TOKEN` (opcional): token Bearer para autenticação.
- `WHATSAPP_DELAY_MINUTES` (opcional, padrão `5`): atraso em minutos.
- `WHATSAPP_TEMPLATE` (opcional): mensagem com placeholders `{nome}` e `{plano}`.

Payload enviado para `WHATSAPP_API_URL`:

```json
{
  "phone": "55DDDNUMERO",
  "message": "mensagem formatada",
  "order_id": "uuid-do-pedido"
}
```
