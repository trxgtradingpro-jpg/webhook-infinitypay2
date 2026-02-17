  (function setupWhatsAppExamples(){
    const waNumber = "5511940431906";
    const examples = [
      { title: "1. Quero ativar o grátis", msg: "Oi! Quero ativar o TRX Grátis e receber as instruções de instalação." },
      { title: "2. Já comprei e não recebi e-mail", msg: "Oi! Já comprei o plano, mas ainda não recebi o e-mail. Pode verificar para mim?" },
      { title: "3. E-mail caiu no spam", msg: "Oi! Acho que o e-mail do acesso caiu no spam. Pode me reenviar as instruções?" },
      { title: "4. Dúvida para escolher plano", msg: "Oi! Quero ajuda para escolher o melhor plano para o meu perfil e quantidade de contratos." },
      { title: "5. Ajuda na instalação", msg: "Oi! Preciso de suporte para baixar e instalar o TRX no meu computador." },
      { title: "6. Erro no acesso", msg: "Oi! Estou com erro ao abrir o arquivo/licenca do TRX. Podem me ajudar?" },
      { title: "7. Quero upgrade de plano", msg: "Oi! Quero fazer upgrade do meu plano atual para um plano acima." },
      { title: "8. Suporte técnico rápido", msg: "Oi! Preciso de suporte técnico rápido para configurar o TRX corretamente." },
      { title: "9. Dúvida sobre pagamento", msg: "Oi! Tenho uma dúvida sobre pagamento, cobrança e confirmação da compra." },
      { title: "10. Falar com especialista", msg: "Oi! Quero falar com um especialista antes de finalizar minha compra." }
    ];

    const floatBtn = document.getElementById("whatsappFloatBtn");
    const panel = document.getElementById("whatsappPanel");
    const list = document.getElementById("waList");
    const closeBtn = document.getElementById("waCloseBtn");
    if (!floatBtn || !panel || !list || !closeBtn) return;

    const openPanel = () => {
      panel.classList.add("open");
      panel.setAttribute("aria-hidden", "false");
      floatBtn.setAttribute("aria-expanded", "true");
    };

    const closePanel = () => {
      panel.classList.remove("open");
      panel.setAttribute("aria-hidden", "true");
      floatBtn.setAttribute("aria-expanded", "false");
    };

    const buildWaUrl = (msg) => `https://wa.me/${waNumber}?text=${encodeURIComponent(msg)}`;

    list.innerHTML = examples.map((item, idx) => `
      <button type="button" class="wa-item" data-index="${idx}">
        <span class="wa-item-title">${item.title}</span>
        <span class="wa-item-msg">${item.msg}</span>
      </button>
    `).join("");

    list.addEventListener("click", (event) => {
      const target = event.target.closest(".wa-item");
      if (!target) return;
      const idx = Number(target.getAttribute("data-index"));
      const payload = examples[idx];
      if (!payload) return;
      window.open(buildWaUrl(payload.msg), "_blank", "noopener,noreferrer");
      closePanel();
    });

    floatBtn.addEventListener("click", () => {
      if (panel.classList.contains("open")) {
        closePanel();
      } else {
        openPanel();
      }
    });

    closeBtn.addEventListener("click", closePanel);

    document.addEventListener("click", (event) => {
      const clickInsidePanel = panel.contains(event.target);
      const clickFloatBtn = floatBtn.contains(event.target);
      if (!clickInsidePanel && !clickFloatBtn) {
        closePanel();
      }
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") closePanel();
    });
  })();
