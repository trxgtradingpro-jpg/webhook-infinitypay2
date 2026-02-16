(function () {
  var STORAGE_KEY = "trx_theme";
  var root = document.documentElement;

  function isValidTheme(value) {
    return value === "light" || value === "dark";
  }

  function getStoredTheme() {
    try {
      var value = localStorage.getItem(STORAGE_KEY);
      return isValidTheme(value) ? value : null;
    } catch (_) {
      return null;
    }
  }

  function prefersDarkTheme() {
    return !!(window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches);
  }

  function resolveTheme() {
    return getStoredTheme() || (prefersDarkTheme() ? "dark" : "light");
  }

  function setTheme(theme, persist) {
    var next = isValidTheme(theme) ? theme : "dark";
    root.setAttribute("data-theme", next);
    root.style.colorScheme = next;

    if (persist) {
      try {
        localStorage.setItem(STORAGE_KEY, next);
      } catch (_) {}
    }

    updateToggle(next);
  }

  function updateToggle(theme) {
    var button = document.getElementById("theme-toggle-btn");
    if (!button) return;

    var isLight = theme === "light";
    var label = button.querySelector("[data-role='label']");

    button.dataset.theme = theme;
    button.setAttribute("aria-pressed", isLight ? "true" : "false");
    button.setAttribute("title", isLight ? "Tema claro ativo" : "Tema escuro ativo");

    if (label) {
      label.textContent = isLight ? "Tema claro" : "Tema escuro";
    }
  }

  function ensureToggle() {
    if (document.getElementById("theme-toggle-btn")) {
      updateToggle(resolveTheme());
      return;
    }

    var wrap = document.createElement("div");
    wrap.className = "theme-toggle-wrap";

    wrap.innerHTML = [
      '<button id="theme-toggle-btn" class="theme-toggle-btn" type="button" aria-label="Alternar tema" aria-pressed="false">',
      '  <span class="theme-toggle-icon" aria-hidden="true"></span>',
      '  <span data-role="label">Tema escuro</span>',
      '</button>'
    ].join("");

    document.body.appendChild(wrap);

    var button = document.getElementById("theme-toggle-btn");
    if (!button) return;

    button.addEventListener("click", function () {
      var current = root.getAttribute("data-theme") === "light" ? "light" : "dark";
      var next = current === "light" ? "dark" : "light";

      root.classList.add("theme-transition");
      setTheme(next, true);
      window.setTimeout(function () {
        root.classList.remove("theme-transition");
      }, 280);
    });

    updateToggle(resolveTheme());
  }

  setTheme(resolveTheme(), false);

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", ensureToggle, { once: true });
  } else {
    ensureToggle();
  }

  if (window.matchMedia) {
    var media = window.matchMedia("(prefers-color-scheme: dark)");
    var onSystemThemeChange = function (event) {
      if (getStoredTheme()) return;
      setTheme(event.matches ? "dark" : "light", false);
    };

    if (media.addEventListener) {
      media.addEventListener("change", onSystemThemeChange);
    } else if (media.addListener) {
      media.addListener(onSystemThemeChange);
    }
  }

  window.addEventListener("storage", function (event) {
    if (event.key !== STORAGE_KEY || !isValidTheme(event.newValue)) return;
    setTheme(event.newValue, false);
  });
})();
