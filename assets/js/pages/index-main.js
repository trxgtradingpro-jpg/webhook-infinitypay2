    // LINKS (seus links oficiais)
    const INDEX_CONFIG = (window.TRX_INDEX_CONFIG && typeof window.TRX_INDEX_CONFIG === "object")
      ? window.TRX_INDEX_CONFIG
      : {};
    const CHECKOUT = (INDEX_CONFIG.checkout && typeof INDEX_CONFIG.checkout === "object")
      ? INDEX_CONFIG.checkout
      : {
        gratis: "/checkout/trx-gratis",
        bronze: "/checkout/trx-bronze",
        prata:  "/checkout/trx-prata",
        gold:   "/checkout/trx-gold",
        black:  "/checkout/trx-black"
      };

    const REPORT_AUTO_INTERVAL_MS = 12000;
    let autoReportTimer = null;
    let activeReportIndex = 0;

    function setupReportModal(track){
      const modal = document.getElementById("reportModal");
      const modalImg = document.getElementById("reportModalImg");
      const modalTitle = document.getElementById("reportModalTitle");
      const closeBtn = document.getElementById("reportModalClose");
      if (!modal || !modalImg || !modalTitle || !closeBtn) return;

      const closeModal = () => {
        modal.classList.remove("is-open");
        modal.setAttribute("aria-hidden", "true");
        document.body.style.overflow = "";
      };

      const openModal = (imgEl) => {
        const month = imgEl.getAttribute("data-month") || "";
        const period = imgEl.getAttribute("data-period") || "";
        const gain = imgEl.getAttribute("data-gain") || "";
        const cumulative = imgEl.getAttribute("data-cumulative") || "";
        modalImg.src = imgEl.currentSrc || imgEl.src || "";
        modalImg.alt = `Relatório mensal ampliado ${month}`.trim();
        modalTitle.textContent = `${month} • ${period} • ${gain} • Acumulado ${cumulative}`.trim();
        modal.classList.add("is-open");
        modal.setAttribute("aria-hidden", "false");
        document.body.style.overflow = "hidden";
      };

      if (modal.dataset.bound !== "1"){
        closeBtn.addEventListener("click", closeModal);
        modal.addEventListener("click", (event) => {
          if (event.target === modal) closeModal();
        });
        document.addEventListener("keydown", (event) => {
          if (event.key === "Escape" && modal.classList.contains("is-open")){
            closeModal();
          }
        });
        modal.dataset.bound = "1";
      }

      function bindZoomTarget(imgEl){
        if (!imgEl) return;
        if (imgEl.dataset.zoomBound === "1") return;
        imgEl.onclick = () => openModal(imgEl);
        imgEl.dataset.zoomBound = "1";
      }

      if (track){
        if (track.dataset.zoomDelegated !== "1"){
          track.addEventListener("click", (event) => {
            const zoomTarget = event.target.closest(".report-image img");
            if (!zoomTarget || !track.contains(zoomTarget)) return;
            openModal(zoomTarget);
          });
          track.dataset.zoomDelegated = "1";
        }
      }

      bindZoomTarget(document.getElementById("capitalCurveImg"));
    }

    function renderMonthlyReports(items){
      const track = document.getElementById("reportTrack");
      if (!track) return;
      setupReportModal(track);

      if (!items.length){
        track.innerHTML = `
          <article class="report-card neutral">
            <div class="report-image"></div>
            <div class="report-body">
              <h3 class="report-month">Relatórios mensais em atualização</h3>
              <p class="report-period">Aguarde a próxima atualização.</p>
              <span class="report-gain neutral">R$ 0,00</span>
            </div>
          </article>
        `;
        return;
      }

      track.innerHTML = items.map((item) => `
        <article class="report-card ${item.status || "neutral"}">
          <div class="report-image">
            <img
              src="${item.image_url}"
              alt="Relatório mensal ${item.month}"
              loading="lazy"
              data-month="${item.month}"
              data-period="Início: dia ${item.start_day} | Fim: dia ${item.end_day}"
              data-gain="${item.gain}"
              data-cumulative="${item.cumulative_gain || "R$ 0,00"}"
              onerror="this.style.opacity=.28; this.alt='Imagem indisponível';"
            >
          </div>
          <div class="report-body">
            <h3 class="report-month">${item.month}</h3>
            <p class="report-period">Início: dia ${item.start_day} | Fim: dia ${item.end_day}</p>
            <span class="report-gain ${item.status || "neutral"}">${item.gain}</span>
            <div class="report-cumulative ${item.cumulative_status || "neutral"}">Acumulado: <strong>${item.cumulative_gain || "R$ 0,00"}</strong></div>
          </div>
        </article>
      `).join("");

      const prev = document.getElementById("reportPrev");
      const next = document.getElementById("reportNext");
      const cards = Array.from(track.querySelectorAll(".report-card"));
      const monthlySection = document.getElementById("relatorios");
      let sectionVisible = false;

      function scrollToIndex(index, smooth = true){
        if (!cards.length) return;
        const normalized = (index + cards.length) % cards.length;
        activeReportIndex = normalized;
        const card = cards[normalized];
        track.scrollTo({
          left: Math.max(0, card.offsetLeft - 2),
          behavior: smooth ? "smooth" : "auto"
        });
      }

      function stopAuto(){
        if (!autoReportTimer) return;
        clearInterval(autoReportTimer);
        autoReportTimer = null;
      }

      function startAuto(){
        stopAuto();
        if (!sectionVisible) return;
        if (cards.length <= 1) return;
        autoReportTimer = setInterval(() => {
          scrollToIndex(activeReportIndex + 1, true);
        }, REPORT_AUTO_INTERVAL_MS);
      }

      function syncActiveIndexByScroll(){
        if (!cards.length) return;
        const viewportLeft = track.scrollLeft;
        let closestIndex = 0;
        let closestDistance = Number.POSITIVE_INFINITY;

        cards.forEach((card, index) => {
          const distance = Math.abs(card.offsetLeft - viewportLeft);
          if (distance < closestDistance){
            closestDistance = distance;
            closestIndex = index;
          }
        });

        activeReportIndex = closestIndex;
      }

      if (prev){
        prev.onclick = () => {
          scrollToIndex(activeReportIndex - 1, true);
          if (sectionVisible) startAuto();
        };
      }
      if (next){
        next.onclick = () => {
          scrollToIndex(activeReportIndex + 1, true);
          if (sectionVisible) startAuto();
        };
      }

      track.addEventListener("wheel", (event) => {
        if (Math.abs(event.deltaY) <= Math.abs(event.deltaX)) return;
        event.preventDefault();
        track.scrollBy({ left: event.deltaY, behavior: "auto" });
      }, { passive: false });

      track.addEventListener("scroll", syncActiveIndexByScroll);
      track.addEventListener("mouseenter", stopAuto);
      track.addEventListener("mouseleave", () => {
        if (sectionVisible) startAuto();
      });
      track.addEventListener("touchstart", stopAuto, { passive: true });
      track.addEventListener("touchend", () => {
        if (sectionVisible) startAuto();
      }, { passive: true });

      if (monthlySection){
        const visibilityObserver = new IntersectionObserver((entries) => {
          const entry = entries[0];
          if (!entry) return;
          sectionVisible = entry.isIntersecting && entry.intersectionRatio >= 0.45;
          if (sectionVisible){
            syncActiveIndexByScroll();
            startAuto();
          } else {
            stopAuto();
          }
        }, { threshold: [0, 0.45, 0.8] });

        visibilityObserver.observe(monthlySection);
      }

      scrollToIndex(0, false);
      syncActiveIndexByScroll();
    }


    async function loadMonthlyReports(){
      try {
        const response = await fetch("/api/reports/monthly", { cache: "no-store" });
        if (!response.ok) throw new Error("Erro ao carregar relatórios");
        const data = await response.json();
        const reports = Array.isArray(data.reports) ? data.reports : [];
        renderMonthlyReports(reports);
      } catch (error){
        renderMonthlyReports([]);
      }
    }

    function setupReveal(){
      const targets = document.querySelectorAll(".reveal-up");
      if (!targets.length) return;

      const observer = new IntersectionObserver((entries, obs) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;
          entry.target.classList.add("is-visible");
          obs.unobserve(entry.target);
        });
      }, { threshold: 0.18 });

      targets.forEach((target) => observer.observe(target));
    }

    function setupHeroVideoAutoplay(){
      const videoWrap = document.getElementById("heroVideo");
      const frame = document.getElementById("heroVideoFrame");
      if (!videoWrap || !frame) return;

      const videoId = (videoWrap.dataset.videoId || "").trim();
      if (!videoId) return;

      frame.addEventListener("load", () => {
        videoWrap.classList.add("is-ready");
        videoWrap.dataset.loadError = "0";
      }, { once: true });

      frame.addEventListener("error", () => {
        videoWrap.classList.remove("is-ready");
        videoWrap.dataset.loadError = "1";
      });

      if (videoWrap.dataset.loaded === "1") return;

      const params = new URLSearchParams({
        autoplay: "1",
        mute: "1",
        playsinline: "1",
        rel: "0",
        modestbranding: "1",
        controls: "1"
      });

      frame.src = `https://www.youtube.com/embed/${encodeURIComponent(videoId)}?${params.toString()}`;
      videoWrap.dataset.loaded = "1";
    }

    function setupContentProtection(){
      document.documentElement.classList.add("protect-mode");
      document.body.classList.add("protect-mode");

      const cancelDefault = (event) => event.preventDefault();
      ["contextmenu", "copy", "cut", "paste", "selectstart", "dragstart"].forEach((eventName) => {
        document.addEventListener(eventName, cancelDefault);
      });

      document.addEventListener("keydown", (event) => {
        const key = (event.key || "").toLowerCase();
        const ctrlOrMeta = event.ctrlKey || event.metaKey;

        if (event.key === "PrintScreen"){
          event.preventDefault();
          if (navigator.clipboard && navigator.clipboard.writeText){
            navigator.clipboard.writeText("");
          }
        }

        if (event.key === "F12"){
          event.preventDefault();
        }

        if (ctrlOrMeta && ["c", "x", "u", "s", "p", "a"].includes(key)){
          event.preventDefault();
        }

        if (event.ctrlKey && event.shiftKey && ["i", "j", "c", "k", "s"].includes(key)){
          event.preventDefault();
        }
      }, true);

      window.addEventListener("beforeprint", () => {
        document.body.classList.add("print-blocked");
      });

      window.addEventListener("afterprint", () => {
        document.body.classList.remove("print-blocked");
      });
    }

    setupContentProtection();
    loadMonthlyReports();
    setupReveal();
    setupHeroVideoAutoplay();

    function goCheckout(plan){
      const url = CHECKOUT[plan] || CHECKOUT.gold;
      window.location.href = url;
    }

    // Contagem regressiva
    let seconds = 14 * 60 + 4; // 14:04
    const topEl = document.getElementById("topCountdown");
    const barEl = document.getElementById("priceCountdown");

    function fmt(s){
      const m = Math.floor(s/60);
      const ss = s % 60;
      return { m, ss, txt: `${m}:${String(ss).padStart(2,'0')}` };
    }

    setInterval(() => {
      seconds = Math.max(0, seconds - 1);
      const f = fmt(seconds);
      if (topEl) topEl.textContent = `${f.txt} para atualização de condição comercial`;
      if (barEl) barEl.textContent = `${f.m}min ${String(f.ss).padStart(2,'0')}s`;
    }, 1000);

    // "Robôs operando agora"
    const liveEl = document.getElementById("liveRobots");
    let base = 375;

    setInterval(() => {
      const delta = Math.floor(Math.random() * 3) - 1;
      base = Math.max(320, Math.min(520, base + delta));
      if (liveEl) liveEl.textContent = `${base} ROBÔS OPERANDO AGORA`;
    }, 2500);
