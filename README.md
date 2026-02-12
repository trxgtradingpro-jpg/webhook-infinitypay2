# webhook-infinitypay

## WhatsApp para plano grátis

O sistema agora possui **2 comportamentos**:

1. **Botão oficial `wa.me` no dashboard** (manual).
2. **Envio automático após alguns minutos** (via WhatsApp Cloud API oficial da Meta).

## Variáveis para o link oficial (`wa.me`)

- `WHATSAPP_NUMERO`: número que aparece no link (com ou sem `55`, só números).
- `WHATSAPP_MENSAGEM`: mensagem base (suporta `{nome}` e `{plano}`).
- `WHATSAPP_DELAY_MINUTES`: atraso para liberar ação de WhatsApp (padrão `5`).

Formato do botão:

```text
https://wa.me/<numero>?text=<mensagem-url-encoded>
```

Exemplo:

```text
https://wa.me/5511940431906?text=Olá%20quero%20meu%20plano
```

## Variáveis para envio automático (Meta Cloud API)

Para realmente enviar mensagem automática sem clique manual, configure também:

- `WHATSAPP_AUTO_SEND` (padrão `true`)
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_ACCESS_TOKEN`
- `WHATSAPP_GRAPH_VERSION` (padrão `v21.0`)

Sem `WHATSAPP_PHONE_NUMBER_ID` e `WHATSAPP_ACCESS_TOKEN`, o sistema continua exibindo o botão no dashboard, mas não consegue disparar a mensagem sozinho.
