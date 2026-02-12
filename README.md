# webhook-infinitypay

## WhatsApp para plano grátis

O sistema agora possui **2 comportamentos**:

1. **Botão oficial `wa.me` no dashboard** (manual).
2. **Envio automático após alguns minutos** (via WhatsApp Cloud API oficial da Meta).

## Variáveis para o link oficial (`wa.me`)

- `WHATSAPP_MENSAGEM`: mensagem base (suporta `{nome}` e `{plano}`).
- `WHATSAPP_DELAY_MINUTES`: atraso para liberar ação de WhatsApp (padrão `5`).

Formato do botão:

```text
https://wa.me/<telefone-do-usuario>?text=<mensagem-url-encoded>
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


## Dashboard admin

- Pedidos `trx-gratis` mostram status em azul.
- Após envio automático ou clique no botão WhatsApp, aparece `mensagem enviada`.
- Cada pedido possui botão `Excluir` com confirmação.

- O botão manual `WhatsApp` envia para o telefone do próprio usuário salvo no pedido (campo `telefone`).


## Novos recursos no dashboard

- Botão `Reenviar msg` para abrir novamente a conversa no WhatsApp do usuário.
- Coluna de contagem regressiva com dias restantes para completar 30 dias desde `created_at`.
- Alertas visuais quando faltam **5** e **3** dias para completar 30 dias.
- Detecção de usuários com dados duplicados (nome + email + telefone) e opção de `Excluir duplicados` mantendo o registro atual.

- Barra de pesquisa no dashboard por nome, email, telefone, data, status, plano e dias restantes.
- Planos pagos destacados com cores e símbolos por tipo de plano.
