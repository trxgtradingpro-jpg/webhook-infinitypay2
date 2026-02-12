# webhook-infinitypay

## WhatsApp para planos

O sistema possui:

1. **Botão oficial `wa.me` no dashboard** (manual), usando o telefone do próprio pedido.
2. **Envio automático para plano grátis** após alguns minutos (via WhatsApp Cloud API oficial da Meta).

## Variáveis de WhatsApp

- `WHATSAPP_MENSAGEM`: mensagem base (suporta `{nome}` e `{plano}`).
- `WHATSAPP_DELAY_MINUTES`: atraso para plano grátis (padrão `5`).
- `WHATSAPP_AUTO_SEND` (padrão `true`).
- `WHATSAPP_PHONE_NUMBER_ID`.
- `WHATSAPP_ACCESS_TOKEN`.
- `WHATSAPP_GRAPH_VERSION` (padrão `v21.0`).

Formato do link manual:

```text
https://wa.me/<telefone-do-usuario>?text=<mensagem-url-encoded>
```

## Dashboard admin

- Filtro por plano: `Todos`, `Somente pagos`, `Somente grátis` e planos específicos.
- Busca por nome, email, telefone, data, plano, status e dias restantes.
- Destaque visual de planos com cores e símbolos.
- Exibe: total pedidos, processados, pendentes, total pagos (somente planos com preço > 0), total grátis e faturamento total real (somando o preço de cada plano pago).
- Exibe usuários online em tempo real aproximado (heartbeat), sem contabilizar o próprio admin na tela do dashboard.
- Corrige exibição de horário para timezone configurável em `ADMIN_TIMEZONE` (padrão `America/Sao_Paulo`).
- Mostra botão `WhatsApp` também para planos pagos (quando telefone válido).
- Mantém detecção de duplicados e opção de excluir duplicados.
- Salva contador de mensagens enviadas por pedido em `whatsapp_mensagens_enviadas`.
