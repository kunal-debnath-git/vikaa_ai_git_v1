// frontend/config.js
// Backend target + shared “trusted dev” detection (loopback, LAN, Cursor / VS Code web hosts).

(function () {
  function _priv4Host(h) {
    return /^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test((h || "").toLowerCase());
  }

  /** Matches backend is_trusted_dev_execution_context for the SPA origin. */
  function vikaaIsTrustedDevFrontend() {
    const h = (window.location.hostname || "").toLowerCase();
    if (!h) return true; // file://
    if (h === "localhost" || h === "127.0.0.1") return true;
    if (_priv4Host(h)) return true;
    if (h.endsWith(".cursor.sh") || h.endsWith(".cursor.com")) return true;
    if (h.includes("vscode-webview")) return true;
    if (h.endsWith(".vscode-cdn.net")) return true;
    return false;
  }

  function vikaaComputeApiBaseUrl() {
    const raw = window.location.hostname || "";
    const h = raw.toLowerCase();
    if (!h) return "http://localhost:10000";
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:10000";
    if (_priv4Host(h)) return "http://" + raw + ":10000";
    return "https://app-wtiw.onrender.com";
  }

  window.vikaaIsTrustedDevFrontend = vikaaIsTrustedDevFrontend;

  window.CONFIG = {
    API_BASE_URL: vikaaComputeApiBaseUrl(),
  };
})();

console.log("🛠️ Vikaa Config Loaded. Backend target:", window.CONFIG.API_BASE_URL);
