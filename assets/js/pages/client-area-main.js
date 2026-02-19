function getClientAreaConfig(){
  if (window.__TRX_CLIENT_AREA_CONFIG__ && typeof window.__TRX_CLIENT_AREA_CONFIG__ === "object"){
    return window.__TRX_CLIENT_AREA_CONFIG__;
  }

  const configEl = document.getElementById("clientAreaConfigJson");
  if (!configEl){
    window.__TRX_CLIENT_AREA_CONFIG__ = {};
    return window.__TRX_CLIENT_AREA_CONFIG__;
  }

  try {
    window.__TRX_CLIENT_AREA_CONFIG__ = JSON.parse(configEl.textContent || "{}");
  } catch (_) {
    window.__TRX_CLIENT_AREA_CONFIG__ = {};
  }

  return window.__TRX_CLIENT_AREA_CONFIG__;
}
(function(){
        const links = document.querySelectorAll(".js-upsell-link[data-upgrade-plan]");
        if (!links.length) return;

        const csrfToken = String(getClientAreaConfig().csrfToken || "");
        const endpoint = "/api/client/lead-upgrade-click";
        const funnelEndpoint = "/api/funnel/track";

        function registrarLead(targetPlan, source){
          return fetch(endpoint, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-CSRF-Token": csrfToken
            },
            credentials: "same-origin",
            keepalive: true,
            body: JSON.stringify({
              target_plan: targetPlan,
              source: source || "client_area_free_upsell"
            })
          });
        }

        function registrarFunnelCta(targetPlan, source, href){
          return fetch(funnelEndpoint, {
            method: "POST",
            headers: {
              "Content-Type": "application/json"
            },
            credentials: "same-origin",
            keepalive: true,
            body: JSON.stringify({
              event_name: "cta_click",
              cta_id: "client_area_upgrade",
              source: source || "client_area_free_upsell",
              plan: targetPlan || "",
              checkout_slug: (targetPlan || "").trim().toLowerCase(),
              destination: href || ""
            })
          });
        }

        links.forEach((link) => {
          link.addEventListener("click", function(event){
            const targetPlan = link.getAttribute("data-upgrade-plan") || "";
            const source = link.getAttribute("data-upgrade-source") || "client_area_free_upsell";
            const href = link.getAttribute("href") || "/";

            if (!targetPlan) return;

            if (
              event.defaultPrevented ||
              event.button !== 0 ||
              event.metaKey ||
              event.ctrlKey ||
              event.shiftKey ||
              event.altKey
            ) {
              registrarLead(targetPlan, source).catch(() => null);
              registrarFunnelCta(targetPlan, source, href).catch(() => null);
              return;
            }

            event.preventDefault();
            let redirected = false;
            const goCheckout = function(){
              if (redirected) return;
              redirected = true;
              window.location.href = href;
            };

            const fallback = setTimeout(goCheckout, 220);
            Promise.allSettled([
              registrarLead(targetPlan, source),
              registrarFunnelCta(targetPlan, source, href)
            ])
              .finally(() => {
                clearTimeout(fallback);
                goCheckout();
              });
          });
        });
      })();

(function setupAffiliateCopy(){
      const copyBtn = document.getElementById("affiliateCopyBtn");
      const field = document.getElementById("affiliateLinkField");
      const copyWrap = document.getElementById("affiliateCopyWrap");
      if (!copyBtn || !field) return;

      const originalLabel = copyBtn.textContent;
      const copyReady = copyBtn.getAttribute("data-copy-ready") === "1";
      let tooltipTimer = null;
      const setLabel = (text) => {
        copyBtn.textContent = text;
        setTimeout(() => {
          copyBtn.textContent = originalLabel;
        }, 1500);
      };
      const showTooltip = () => {
        if (!copyWrap || copyReady) return;
        copyWrap.classList.add("show-tooltip");
        clearTimeout(tooltipTimer);
        tooltipTimer = setTimeout(() => {
          copyWrap.classList.remove("show-tooltip");
        }, 2200);
      };

      copyBtn.addEventListener("click", async () => {
        if (!copyReady) {
          showTooltip();
          setLabel("Bloqueado");
          return;
        }

        const value = (field.value || "").trim();
        if (!value) return;

        try {
          if (navigator.clipboard && navigator.clipboard.writeText) {
            await navigator.clipboard.writeText(value);
          } else {
            field.focus();
            field.select();
            document.execCommand("copy");
          }
          setLabel("Copiado");
        } catch (_) {
          setLabel("Falhou");
        }
      });
    })();

(function setupOnboardingChecklist(){
      const card = document.getElementById("onboardingChecklistCard");
      if (!card) return;

      const flowContent = document.getElementById("onboardingFlowContent");
      const doneContent = document.getElementById("onboardingDoneContent");
      const doneTitleEl = document.getElementById("onboardingDoneTitle");
      const doneMetaEl = document.getElementById("onboardingDoneMeta");
      const stepsJsonEl = document.getElementById("onboardingStepsJson");
      if (!flowContent || !doneContent || !stepsJsonEl) return;

      let parsedSteps = [];
      try {
        parsedSteps = JSON.parse(stepsJsonEl.textContent || "[]");
      } catch (_) {
        parsedSteps = [];
      }

      const steps = Array.isArray(parsedSteps)
        ? parsedSteps
          .map((item) => ({
            key: String(item && item.key ? item.key : "").trim(),
            label: String(item && item.label ? item.label : "").trim(),
            checked: Boolean(item && item.checked)
          }))
          .filter((item) => item.key)
        : [];
      if (!steps.length) return;

      const progressMetaEl = document.getElementById("onboardingProgressMeta");
      const progressFillEl = document.getElementById("onboardingProgressFill");
      const progressPercentEl = document.getElementById("onboardingProgressPercent");
      const indicatorsEl = document.getElementById("onboardingStepIndicators");
      const currentStepCheckboxEl = document.getElementById("onboardingCurrentStepCheckbox");
      const currentStepLabelEl = document.getElementById("onboardingCurrentStepLabel");
      const currentStepHintEl = document.getElementById("onboardingCurrentStepHint");
      const prevBtn = document.getElementById("onboardingPrevBtn");
      const nextBtn = document.getElementById("onboardingNextBtn");
      const confirmBtn = document.getElementById("onboardingConfirmBtn");
      const statusEl = document.getElementById("onboardingStatusText");
      const saveMsgEl = document.getElementById("onboardingSaveMsg");
      const helpBtn = document.getElementById("onboardingHelpBtn");
      const helpModal = document.getElementById("onboardingHelpModal");
      const helpBackdrop = document.getElementById("onboardingHelpBackdrop");
      const helpClose = document.getElementById("onboardingHelpClose");
      const helpVideo = document.getElementById("onboardingHelpVideo");
      const helpTitleEl = document.getElementById("onboardingHelpTitle");
      const helpStepsEl = document.getElementById("onboardingHelpSteps");
      const helpLeadEl = document.getElementById("onboardingHelpLead");
      const helpNoteEl = document.getElementById("onboardingHelpNote");
      const helpVideoSrc = helpVideo ? helpVideo.getAttribute("src") : "";
      const defaultHelpVideoSrc = helpVideoSrc || "https://www.youtube.com/embed/u3GWhwR8bcQ?rel=0";
      const helpVideoBase = defaultHelpVideoSrc.split("?")[0];
      let currentHelpVideoSrc = defaultHelpVideoSrc;

      const HELP_CONTENT_BY_STEP = {
        email_accessed: {
          title: "Etapa 1: acessar o e-mail de liberacao",
          lead: "Veja este trecho para localizar o e-mail e confirmar se ele chegou na caixa correta:",
          noteHtml: "Se nao achar o e-mail, revise Spam/Promocoes e depois abra o <a href=\"#manual-instalacao\">Manual de instalacao</a>.",
          videoStart: 111,
          steps: [
            { strong: "Abra seu e-mail", text: " e procure por \"TRX PRO | Acesso e instalacao\"." },
            { strong: "Cheque Spam e Promocoes", text: " caso ele nao esteja na caixa principal." },
            { strong: "Depois de abrir o e-mail", text: " volte e marque esta etapa." }
          ]
        },
        tool_downloaded: {
          title: "Etapa 2: baixar os arquivos da ferramenta",
          lead: "Este ponto mostra onde clicar para fazer o download correto:",
          noteHtml: "Se o download falhar, use o link alternativo no <a href=\"#manual-instalacao\">Manual de instalacao</a>.",
          videoStart: 150,
          steps: [
            { strong: "Abra o e-mail de acesso", text: " e clique no link de download da ferramenta." },
            { strong: "Salve o arquivo .zip", text: " em uma pasta facil de encontrar (Downloads, por exemplo)." },
            { strong: "Confirme o fim do download", text: " e so entao marque esta etapa." }
          ]
        },
        zip_extracted: {
          title: "Etapa 3: descompactar com a senha",
          lead: "Aqui esta o trecho para extrair o arquivo com a senha correta:",
          noteHtml: "A senha esta no e-mail. Se necessario, confira novamente no <a href=\"#manual-instalacao\">Manual de instalacao</a>.",
          videoStart: 155,
          steps: [
            { strong: "Localize o arquivo .zip", text: " baixado na etapa anterior." },
            { strong: "Extraia o conteudo", text: " usando a senha enviada no e-mail." },
            { strong: "Verifique a pasta extraida", text: " e marque a etapa apos validar os arquivos." }
          ]
        },
        tool_installed: {
          title: "Etapa 4: assista o video de instalacao",
          lead: "Assista o video para esta etapa, pois o modo de instalacao atual e incompativel com o fluxo antigo.",
          noteHtml: "Se der bloqueio no Windows, execute como administrador e siga o <a href=\"#manual-instalacao\">Manual de instalacao</a>.",
          videoStart: 238,
          steps: [
            { strong: "Abra o video no ponto indicado", text: " e siga exatamente o passo a passo mostrado." },
            { strong: "Nao use o procedimento antigo", text: " para evitar erro de instalacao." },
            { strong: "Depois de concluir pelo video", text: " volte e marque esta etapa." }
          ]
        },
        robot_activated: {
          title: "Etapa 5: ativar o robo",
          lead: "Veja este trecho final para ativar o robo sem erro e concluir o checklist:",
          noteHtml: "Se precisar, abra o <a href=\"#manual-instalacao\">Manual de instalacao</a> e confirme cada campo antes de salvar.",
          videoStart: 283,
          steps: [
            { strong: "Abra a plataforma ja instalada", text: " e carregue os arquivos do robo." },
            { strong: "Confirme parametros iniciais", text: " conforme seu plano liberado." },
            { strong: "Valide que o robo esta ativo", text: " e finalize em \"Confirmar e salvar\"." }
          ]
        }
      };
      if (
        !progressMetaEl ||
        !progressFillEl ||
        !progressPercentEl ||
        !indicatorsEl ||
        !currentStepCheckboxEl ||
        !currentStepLabelEl ||
        !currentStepHintEl ||
        !prevBtn ||
        !nextBtn ||
        !confirmBtn ||
        !statusEl ||
        !saveMsgEl
      ) return;

      const endpoint = "/api/client/onboarding-progress";
      const csrfToken = String(getClientAreaConfig().csrfToken || "");
      let saveSeq = 0;
      let isSaving = false;

      const stepState = {};
      steps.forEach((step) => {
        stepState[step.key] = Boolean(step.checked);
      });

      function getPayload(){
        const payload = {};
        steps.forEach((step) => {
          payload[step.key] = Boolean(stepState[step.key]);
        });
        return payload;
      }

      function getProgress(payload){
        const totalSteps = steps.length;
        const doneCount = steps.reduce((acc, step) => (payload[step.key] ? acc + 1 : acc), 0);
        const percent = totalSteps ? Math.round((doneCount * 100) / totalSteps) : 0;
        return { doneCount, totalSteps, percent };
      }

      function findCurrentStepIndex(payload){
        const firstPending = steps.findIndex((step) => !payload[step.key]);
        if (firstPending >= 0) return firstPending;
        return Math.max(steps.length - 1, 0);
      }

      let currentStepIndex = findCurrentStepIndex(stepState);
      const isInitiallyComplete = Boolean(flowContent.hidden);

      function buildHelpVideoSrc(startSeconds){
        const start = Math.max(0, parseInt(startSeconds, 10) || 0);
        return helpVideoBase + "?start=" + start + "&rel=0";
      }

      function setHelpContentForStep(stepKey, stepLabel){
        const fallbackTitle = stepLabel ? ("Ajuda para " + stepLabel.toLowerCase()) : "Ajuda para esta etapa";
        const content = HELP_CONTENT_BY_STEP[stepKey] || {
          title: fallbackTitle,
          lead: "Siga este trecho do vÃ­deo para concluir a etapa atual com seguranÃ§a:",
          noteHtml: "Se precisar, abra o <a href=\"#manual-instalacao\">Manual de instalaÃ§Ã£o</a> para acompanhar o passo a passo.",
          videoStart: 0,
          steps: [
            { strong: "Abra o material da etapa", text: " e siga o passo a passo exibido." },
            { strong: "Confirme o resultado esperado", text: " antes de prosseguir." },
            { strong: "Volte ao checklist", text: " e marque quando concluir." }
          ]
        };

        if (helpTitleEl) {
          helpTitleEl.textContent = content.title;
        }
        if (helpLeadEl) {
          helpLeadEl.textContent = content.lead;
        }
        if (helpNoteEl) {
          helpNoteEl.innerHTML = content.noteHtml;
        }
        if (helpStepsEl) {
          helpStepsEl.innerHTML = "";
          content.steps.forEach((item) => {
            const li = document.createElement("li");
            const strong = document.createElement("strong");
            strong.textContent = item.strong || "";
            li.appendChild(strong);
            li.appendChild(document.createTextNode(item.text || ""));
            helpStepsEl.appendChild(li);
          });
        }

        currentHelpVideoSrc = buildHelpVideoSrc(content.videoStart);
      }

      function setDoneSummary(doneCount, totalSteps){
        if (doneTitleEl) {
          doneTitleEl.textContent = "Checklist de ativaÃ§Ã£o concluÃ­do.";
        }
        if (doneMetaEl) {
          doneMetaEl.textContent = doneCount + "/" + totalSteps + " etapas confirmadas. Sua Ã¡rea jÃ¡ estÃ¡ pronta para operaÃ§Ã£o.";
        }
      }

      function setCompletedView(isCompleted){
        card.classList.toggle("onboarding-done-card", isCompleted);
        flowContent.hidden = isCompleted;
        doneContent.hidden = !isCompleted;
      }

      function setSaveMessage(message, isError){
        saveMsgEl.textContent = message || "";
        saveMsgEl.classList.toggle("is-error", Boolean(isError));
      }

      function getStatusMessage(doneCount, totalSteps){
        if (doneCount <= 0) return "Marque a etapa atual para iniciar a ativacao.";
        if (doneCount >= totalSteps) return "Tudo marcado. Clique em Confirmar e salvar para finalizar.";
        return "Etapa " + (currentStepIndex + 1) + " de " + totalSteps + ". Avance com Prosseguir ou Voltar.";
      }

      function render(){
        const payload = getPayload();
        const { doneCount, totalSteps, percent } = getProgress(payload);
        const currentStep = steps[currentStepIndex];
        if (!currentStep) return;
        const currentStepChecked = Boolean(payload[currentStep.key]);

        progressMetaEl.textContent = doneCount + "/" + totalSteps + " etapas concluÃ­das";
        progressFillEl.style.width = percent + "%";
        progressPercentEl.textContent = percent + "%";
        statusEl.textContent = getStatusMessage(doneCount, totalSteps);

        indicatorsEl.innerHTML = steps.map((step, index) => {
          const classes = ["onboarding-step-dot"];
          if (payload[step.key]) classes.push("is-done");
          if (index === currentStepIndex) classes.push("is-active");
          return '<span class="' + classes.join(" ") + '"></span>';
        }).join("");

        currentStepCheckboxEl.dataset.stepKey = currentStep.key;
        currentStepCheckboxEl.checked = currentStepChecked;
        currentStepLabelEl.textContent = currentStep.label || ("Etapa " + (currentStepIndex + 1));
        if (currentStepIndex >= totalSteps - 1) {
          currentStepHintEl.textContent = "Ãšltima etapa: marque esta confirmaÃ§Ã£o e clique em Confirmar e salvar.";
        } else {
          currentStepHintEl.textContent = "Etapa " + (currentStepIndex + 1) + " de " + totalSteps + ". Marque e clique em Prosseguir.";
        }

        prevBtn.disabled = currentStepIndex <= 0 || isSaving;
        nextBtn.hidden = currentStepIndex >= totalSteps - 1;
        nextBtn.disabled = !currentStepChecked || isSaving;
        confirmBtn.hidden = currentStepIndex < totalSteps - 1;
        confirmBtn.disabled = doneCount < totalSteps || isSaving;

        if (helpBtn) {
          const helpPulse = !currentStepChecked && !isSaving;
          helpBtn.classList.toggle("is-guiding", helpPulse);
          helpBtn.classList.toggle("is-neutral", !helpPulse);
        }
        const nextPulse = !nextBtn.hidden && currentStepChecked && !nextBtn.disabled && !isSaving;
        nextBtn.classList.toggle("is-guiding-next", nextPulse);
      }

      function openHelpModal(){
        if (!helpModal) return;
        const stepKey = (currentStepCheckboxEl.dataset.stepKey || "").trim();
        const stepLabel = (currentStepLabelEl.textContent || "").trim();
        setHelpContentForStep(stepKey, stepLabel);
        helpModal.classList.add("is-open");
        helpModal.setAttribute("aria-hidden", "false");
        if (helpVideo && currentHelpVideoSrc) {
          helpVideo.setAttribute("src", currentHelpVideoSrc);
        }
      }

      function closeHelpModal(){
        if (!helpModal) return;
        helpModal.classList.remove("is-open");
        helpModal.setAttribute("aria-hidden", "true");
        if (helpVideo && currentHelpVideoSrc) {
          helpVideo.setAttribute("src", "");
          setTimeout(() => helpVideo.setAttribute("src", currentHelpVideoSrc), 80);
        }
      }

      if (helpBtn && helpModal && helpBackdrop && helpClose) {
        helpBtn.addEventListener("click", openHelpModal);
        helpBackdrop.addEventListener("click", closeHelpModal);
        helpClose.addEventListener("click", closeHelpModal);
        document.addEventListener("keydown", (ev) => {
          if (ev.key === "Escape" && helpModal.classList.contains("is-open")) {
            closeHelpModal();
          }
        });
      }

      async function saveProgress(){
        const localPayload = getPayload();
        const progress = getProgress(localPayload);
        if (progress.doneCount < progress.totalSteps) {
          setSaveMessage("Conclua todas as etapas antes de confirmar.", true);
          return;
        }

        isSaving = true;
        render();
        const seq = ++saveSeq;
        setSaveMessage("Confirmando e salvando checklist...", false);

        try {
          const response = await fetch(endpoint, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-CSRF-Token": csrfToken
            },
            credentials: "same-origin",
            body: JSON.stringify({
              steps: localPayload
            })
          });
          const data = await response.json().catch(() => ({}));

          if (seq !== saveSeq) return;
          if (!response.ok || !data.ok) {
            throw new Error("save_failed");
          }

          const savedSteps = (data && typeof data.steps === "object" && data.steps) ? data.steps : localPayload;
          steps.forEach((step) => {
            stepState[step.key] = Boolean(savedSteps[step.key]);
          });

          const savedProgress = getProgress(stepState);
          if (savedProgress.percent >= 100) {
            setDoneSummary(savedProgress.doneCount, savedProgress.totalSteps);
            setCompletedView(true);
          } else {
            currentStepIndex = findCurrentStepIndex(stepState);
            setCompletedView(false);
            render();
            setSaveMessage("Checklist salvo com sucesso.", false);
          }
        } catch (_) {
          if (seq !== saveSeq) return;
          setSaveMessage("Falha ao salvar. Tente novamente.", true);
        } finally {
          isSaving = false;
          if (!flowContent.hidden) render();
        }
      }

      currentStepCheckboxEl.addEventListener("change", () => {
        const stepKey = (currentStepCheckboxEl.dataset.stepKey || "").trim();
        if (!stepKey) return;
        stepState[stepKey] = Boolean(currentStepCheckboxEl.checked);
        setSaveMessage("", false);
        render();
      });

      prevBtn.addEventListener("click", () => {
        if (currentStepIndex <= 0 || isSaving) return;
        currentStepIndex -= 1;
        setSaveMessage("", false);
        render();
      });

      nextBtn.addEventListener("click", () => {
        if (isSaving) return;
        const step = steps[currentStepIndex];
        if (!step) return;
        if (!stepState[step.key]) {
          setSaveMessage("Marque a etapa atual antes de prosseguir.", true);
          return;
        }
        if (currentStepIndex < steps.length - 1) {
          currentStepIndex += 1;
          setSaveMessage("", false);
          render();
        }
      });

      confirmBtn.addEventListener("click", () => {
        if (isSaving) return;
        saveProgress();
      });

      if (isInitiallyComplete) {
        const initialProgress = getProgress(stepState);
        setDoneSummary(initialProgress.doneCount, initialProgress.totalSteps);
        setCompletedView(true);
        return;
      }

      setCompletedView(false);
      render();
    })();

(function setupAccountSections(){
      const menu = document.querySelector(".account-menu");
      if (!menu) return;

      const links = Array.from(menu.querySelectorAll('a[href^="#"]'));
      if (!links.length) return;

      const sections = Array.from(document.querySelectorAll(".section-anchor"));
      const sectionById = new Map();
      sections.forEach((section) => {
        if (!section.id) return;
        sectionById.set(section.id, section);
      });
      if (!sectionById.size) return;

      const firstAvailableId = links
        .map((link) => (link.getAttribute("href") || "").replace("#", ""))
        .find((id) => sectionById.has(id)) || "resumo";

      function setActiveSection(targetId){
        const activeId = sectionById.has(targetId) ? targetId : firstAvailableId;

        sectionById.forEach((section, id) => {
          const isActive = id === activeId;
          section.classList.toggle("is-hidden", !isActive);
          section.setAttribute("aria-hidden", isActive ? "false" : "true");
        });

        links.forEach((link) => {
          const linkId = (link.getAttribute("href") || "").replace("#", "");
          const isActive = linkId === activeId;
          link.classList.toggle("is-active", isActive);
          if (isActive){
            link.setAttribute("aria-current", "page");
          } else {
            link.removeAttribute("aria-current");
          }
        });
      }

      links.forEach((link) => {
        link.addEventListener("click", (event) => {
          event.preventDefault();
          const targetId = (link.getAttribute("href") || "").replace("#", "");
          setActiveSection(targetId);
          if (window.history && window.history.replaceState){
            window.history.replaceState(null, "", `#${targetId}`);
          } else {
            window.location.hash = targetId;
          }
        });
      });

      const hashId = (window.location.hash || "").replace("#", "");
      setActiveSection(hashId || firstAvailableId);
    })();

(function setupDiagnosticPrompt(){
      const card = document.getElementById("diagnosticPromptCard");
      const laterBtn = document.getElementById("diagnosticLaterBtn");
      const laterMsg = document.getElementById("diagnosticLaterMsg");
      if (!card || !laterBtn || !laterMsg) return;

      laterBtn.addEventListener("click", () => {
        card.classList.add("is-minimized");
        laterMsg.hidden = false;
      });
    })();

(function setupInstallVideo(){
      const videoWrap = document.getElementById("installVideo");
      const frame = document.getElementById("installVideoFrame");
      if (!videoWrap || !frame) return;

      const videoId = (videoWrap.dataset.videoId || "").trim();
      if (!videoId) return;

      const loadVideo = function(){
        if (videoWrap.dataset.loaded === "1") return;
        const params = new URLSearchParams({
          autoplay: "0",
          playsinline: "1",
          rel: "0",
          modestbranding: "1",
          controls: "1"
        });
        frame.src = `https://www.youtube.com/embed/${encodeURIComponent(videoId)}?${params.toString()}`;
        videoWrap.dataset.loaded = "1";
      };

      frame.addEventListener("load", () => {
        videoWrap.classList.add("is-ready");
      }, { once: true });

      if ("IntersectionObserver" in window){
        const observer = new IntersectionObserver((entries, obs) => {
          const visible = entries.some((entry) => entry.isIntersecting);
          if (!visible) return;
          loadVideo();
          obs.disconnect();
        }, { threshold: 0.2 });
        observer.observe(videoWrap);
      } else {
        loadVideo();
      }
    })();

(function(){
      const wrap = document.getElementById("notifWrap");
      const toggle = document.getElementById("notifToggleBtn");
      const panel = document.getElementById("notifPanel");
      if (!wrap || !toggle || !panel) return;

      function positionPanel(){
        if (!panel.classList.contains("open")) return;

        const margin = 12;
        const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 0;
        const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
        const triggerRect = toggle.getBoundingClientRect();
        const targetWidth = Math.max(260, Math.min(420, viewportWidth - (margin * 2)));

        panel.style.position = "fixed";
        panel.style.width = targetWidth + "px";
        panel.style.right = "auto";

        let left = triggerRect.right - targetWidth;
        if (left < margin) left = margin;
        if (left + targetWidth > viewportWidth - margin) {
          left = viewportWidth - margin - targetWidth;
        }
        panel.style.left = Math.max(margin, left) + "px";

        let top = triggerRect.bottom + 10;
        if (top < margin) top = margin;
        if (top > viewportHeight - 220) {
          top = Math.max(margin, viewportHeight - 220);
        }
        panel.style.top = top + "px";

        const maxHeight = Math.max(220, viewportHeight - top - margin);
        panel.style.maxHeight = maxHeight + "px";

        const list = panel.querySelector(".notif-list");
        if (list) {
          list.style.maxHeight = Math.max(140, maxHeight - 42) + "px";
        }
      }

      function openPanel(){
        panel.classList.add("open");
        panel.setAttribute("aria-hidden", "false");
        toggle.setAttribute("aria-expanded", "true");
        positionPanel();
      }

      function closePanel(){
        panel.classList.remove("open");
        panel.setAttribute("aria-hidden", "true");
        toggle.setAttribute("aria-expanded", "false");
        panel.style.left = "";
        panel.style.top = "";
        panel.style.right = "";
        panel.style.width = "";
        panel.style.maxHeight = "";

        const list = panel.querySelector(".notif-list");
        if (list) {
          list.style.maxHeight = "";
        }
      }

      toggle.addEventListener("click", function(){
        if (panel.classList.contains("open")) {
          closePanel();
        } else {
          openPanel();
        }
      });

      document.addEventListener("click", function(event){
        if (!wrap.contains(event.target)) {
          closePanel();
        }
      });

      document.addEventListener("keydown", function(event){
        if (event.key === "Escape") {
          closePanel();
        }
      });

      window.addEventListener("resize", function(){
        positionPanel();
      });

      window.addEventListener("scroll", function(){
        positionPanel();
      }, { passive: true });
    })();

