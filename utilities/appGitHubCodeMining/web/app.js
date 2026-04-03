// vikaa.ai Deep GitHub Miner Web App JS
// - Supports two modes: Repository search (/api/repo_search) and Deep code search (/api/deep_search)
// - Updates progress/status, renders result cards, and handles cancel via AbortController
// Debug: Watch console for fetch errors; network tab shows exact API request and response.
const API = "/api";
const form = document.getElementById("searchForm");
const runBtn = document.getElementById("runBtn");
const cancelBtn = document.getElementById("cancelBtn");
const progressBar = document.getElementById("progressBar");
const progressText = document.getElementById("progressText");
const statusText = document.getElementById("statusText");
const resultsEl = document.getElementById("results");
const summaryEl = document.getElementById("summary");
const timingEl = document.getElementById("timing");
const llmProviderEl = document.getElementById("llm_provider");
const llmModelEl = document.getElementById("llm_model");
const modeEl = document.getElementById("mode");
const tokenField = document.getElementById("tokenField");

const MODEL_OPTIONS = {
  gpt: [
    { value: "gpt-4o-mini", label: "gpt-4o-mini (GPT)" },
    { value: "gpt-4o", label: "gpt-4o (GPT)" },
    { value: "gpt-3.5-turbo", label: "gpt-3.5-turbo (GPT)" },
  ],
  gemini: [
    { value: "models/gemini-1.5-flash", label: "models/gemini-1.5-flash (Gemini)" },
    { value: "gemini-1.5-pro", label: "gemini-1.5-pro (Gemini)" },
  ],
};

function refreshModelOptions() {
  const prov = (llmProviderEl?.value || "gpt").toLowerCase();
  const opts = MODEL_OPTIONS[prov] || MODEL_OPTIONS.gpt;
  if (!llmModelEl) return;
  llmModelEl.innerHTML = "";
  for (const o of opts) {
    const opt = document.createElement("option");
    opt.value = o.value;
    opt.textContent = o.label;
    llmModelEl.appendChild(opt);
  }
}
if (llmProviderEl) {
  llmProviderEl.addEventListener("change", refreshModelOptions);
  refreshModelOptions();
}

if (modeEl && tokenField) {
  const refreshTokenVisibility = () => {
    tokenField.style.display = modeEl.value === 'deep' ? '' : 'none';
  };
  modeEl.addEventListener('change', refreshTokenVisibility);
  refreshTokenVisibility();
}

let currentController = null;
function setProgress(pct, text) {
  progressBar.style.width = `${pct}%`;
  progressText.textContent = text || `${pct}%`;
}
function setStatus(text) { statusText.textContent = text; }

function serializeForm(form) {
  const data = new FormData(form);
  const obj = {};
  for (const [k, v] of data.entries()) {
    if (k === "llm_expand" || k === "llm_filter") obj[k] = true;
    else if (["months","max","min_stars","llm_rerank_top"].includes(k)) obj[k] = Number(v);
    else if (k === "llm_temperature") obj[k] = Number(v);
    else obj[k] = v;
  }
  return obj;
}

function renderResults(items) {
  resultsEl.innerHTML = "";
  items.forEach((r, i) => {
    const card = document.createElement("div");
    card.className = "card";
    if (modeEl?.value === 'deep') {
      const url = r?.meta?.url || r?.meta?.blob_url_guess || r?.meta?.repo_url;
      const source = r.source || 'code';
      const score = r.score?.toFixed?.(3) ?? r.score;
      const title = url
        ? `<a href="${url}" target="_blank" rel="noreferrer">${source}</a>`
        : `<span>${source}</span>`;
      card.innerHTML = `
        <div class="title">${i+1}. ${title} • Score: ${score}</div>
        <pre class="desc" style="white-space:pre-wrap">${(r.snippet || "").slice(0, 1200)}</pre>
      `;
      if (url) {
        card.classList.add('clickable');
        card.addEventListener('click', (ev) => {
          const t = ev.target;
          if (t && t.tagName === 'A') return; // let anchor work
          window.open(url, '_blank', 'noopener');
        });
      }
    } else {
      card.innerHTML = `
        <div class="title">${i+1}. <a href="${r.url}" target="_blank" rel="noreferrer">${r.full_name || r.name}</a></div>
        <div class="meta">⭐ ${r.stars} • ${r.score !== undefined ? `Score: ${r.score} • ` : (r.relevance !== undefined ? `Rel: ${r.relevance} • ` : "")}Lang: ${r.language || "-"} • Updated: ${r.updated_at || "-"}</div>
        <div class="desc">${(r.description || "").slice(0, 240)}</div>
      `;
    }
    resultsEl.appendChild(card);
  });
}

async function runSearch() {
  const payload = serializeForm(form);
  const mode = (payload.mode || modeEl?.value || 'repo');
  currentController = new AbortController();
  const signal = currentController.signal;
  let url = '';
  let fetchInit = { signal };
  const t0 = performance.now();
  setStatus("Running");
  setProgress(10, "Submitting request…");
  try {
    if (mode === 'deep') {
      // deep search via GET to keep simple
      const params = new URLSearchParams();
      params.set('prompt', payload.prompt);
      if (payload.github_token) params.set('github_token', payload.github_token);
      params.set('top_k', '5');
      url = `${API}/deep_search?${params.toString()}`;
    } else {
      const params = new URLSearchParams();
      params.set("prompt", payload.prompt);
      params.set("language", payload.language || "");
      params.set("months", String(payload.months || 3));
      params.set("max", String(payload.max || 50));
      params.set("min_stars", String(payload.min_stars || 0));
      if (payload.llm_expand) params.set("llm_expand", "true");
      if (payload.llm_provider) params.set("llm_provider", payload.llm_provider);
      if (payload.llm_model) params.set("llm_model", payload.llm_model);
      if (typeof payload.llm_temperature === "number") params.set("llm_temperature", String(payload.llm_temperature));
      if (typeof payload.llm_rerank_top === "number" && payload.llm_rerank_top > 0) params.set("llm_rerank_top", String(payload.llm_rerank_top));
      if (payload.llm_filter) params.set("llm_filter", "true");
      url = `${API}/repo_search?${params.toString()}`;
    }

    const res = await fetch(url, { signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
    setProgress(50, "Processing response…");
    const data = await res.json();
    setProgress(80, "Rendering results…");
    const arr = data.results || data || [];
    renderResults(arr);
    const durMs = Math.round(performance.now() - t0);
    timingEl.textContent = `Completed in ${durMs} ms`;
    if (mode === 'deep') {
      summaryEl.textContent = `${arr.length} code snippets found. Plan: ${(data.agent_plan || []).join(' → ')}`;
    } else {
      summaryEl.textContent = `${arr.length} repositories found.`;
    }
    setProgress(100, "Done"); setStatus("Completed");
  } catch (err) {
    setStatus(err.name === "AbortError" ? "Cancelled" : "Error");
    summaryEl.textContent = err.name === "AbortError" ? "Search cancelled." : `Error: ${err.message}`;
    setProgress(0, "");
  } finally {
    runBtn.disabled = false; currentController = null;
  }
}

form.addEventListener("submit", (e) => {
  e.preventDefault(); resultsEl.innerHTML = ""; summaryEl.textContent = ""; timingEl.textContent = "";
  runBtn.disabled = true; setProgress(0, "Starting…"); runSearch();
});

cancelBtn.addEventListener("click", () => { if (currentController) currentController.abort(); });
