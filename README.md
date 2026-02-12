# webhook-infinitypay

## WhatsApp oficial (`wa.me`) para plano grátis

Para abrir a conversa oficial do WhatsApp com mensagem pronta após 5 minutos do `trx-gratis`, configure:

- `WHATSAPP_NUMERO` (obrigatória): número de destino (com ou sem `55`, apenas números).
- `WHATSAPP_MENSAGEM` (obrigatória/recomendada): mensagem padrão. Suporta `{nome}` e `{plano}`.
- `WHATSAPP_DELAY_MINUTES` (opcional, padrão `5`): tempo de espera para liberar o link no dashboard.

Formato gerado:

```text
https://wa.me/<numero>?text=<mensagem-url-encoded>
```

Exemplo:

```text
https://wa.me/5511940431906?text=Olá%20quero%20meu%20plano
```
