    const TAB_ORDER = ['ingestion', 'retrieval', 'crag', 'generation', 'observability', 'summary'];

    // ── Retrieval flag auto-intelligence ─────────────────────────────────────
    let _lastRetCount    = null;   // count from last retrieval run
    let _lastRetAvgScore = null;   // avg VS score from last retrieval run
    let _retQuerySnap    = '';     // query snapshot to detect topic change
    const _retManual     = new Set(); // flags manually overridden by user

    function _autoSetRetFlags() {
      const query  = (document.getElementById('retQuery')?.value || '').trim();
      const words  = query.split(/\s+/).filter(Boolean).length;
      const isQ    = /^(who|what|when|where|why|how|tell|explain|describe|summarize|list|give)\b/i.test(query) || query.endsWith('?');
      const topK   = parseInt(document.getElementById('topK')?.value)  || 10;
      const topN   = parseInt(document.getElementById('topN')?.value)  || 5;

      // Reset manual overrides and retry flag when query topic changes
      if (query !== _retQuerySnap) {
        _retManual.clear();
        const retryEl = document.getElementById('retIsRetry');
        if (retryEl) retryEl.checked = false;
        _retQuerySnap = query;
      }

      const rules = [
        // id                  auto value                             reason shown in tooltip
        { id: 'retSynthesize',     val: isQ || words > 3,            reason: isQ ? 'Question detected' : words > 3 ? 'Descriptive query' : 'Keyword-only query' },
        { id: 'retOverrideHyde',   val: words > 0,                   reason: 'HyDE bridges vocabulary gaps in vector search' },
        { id: 'retOverrideRerank', val: topK > topN,                  reason: topK > topN ? `top_k(${topK}) > top_n(${topN}) — re-rank has value` : 'top_k equals top_n — no benefit' },
        { id: 'retIsRetry',        val: _lastRetCount !== null && (_lastRetCount === 0 || (_lastRetAvgScore !== null && _lastRetAvgScore < 0.35)),
                                                                       reason: _lastRetCount === 0 ? 'Last run returned 0 results' : 'Last run had low relevance scores' },
      ];

      rules.forEach(({ id, val, reason }) => {
        if (_retManual.has(id)) return; // user overrode — leave it
        const el    = document.getElementById(id);
        const label = el?.closest('label');
        if (!el || !label) return;
        el.checked = val;
        // Update auto-hint span
        let hint = label.querySelector('.flag-auto-hint');
        if (!hint) { hint = document.createElement('span'); hint.className = 'flag-auto-hint'; label.appendChild(hint); }
        hint.textContent = val ? ' · auto' : '';
        hint.title = reason;
      });
    }

    function _initRetFlagListeners() {
      // Query input → re-run auto (debounced)
      const qEl = document.getElementById('retQuery');
      if (qEl) {
        let _t;
        qEl.addEventListener('input', () => { clearTimeout(_t); _t = setTimeout(_autoSetRetFlags, 350); });
      }
      // top_k / top_n change → re-run auto
      ['topK','topN'].forEach(id => {
        document.getElementById(id)?.addEventListener('change', _autoSetRetFlags);
      });
      // Manual click on any flag → mark as overridden
      ['retSynthesize','retOverrideHyde','retOverrideRerank','retIsRetry'].forEach(id => {
        document.getElementById(id)?.addEventListener('change', () => {
          _retManual.add(id);
          const label = document.getElementById(id)?.closest('label');
          const hint  = label?.querySelector('.flag-auto-hint');
          if (hint) hint.textContent = '';
        });
      });
    }

    // ── Tab switching ──
    function switchTab(name, btn) {
      document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.getElementById('panel-' + name).classList.add('active');
      btn.classList.add('active');
      if (name === 'summary') refreshJSON();
      updateTabNavButtons();
    }

    function goTabStep(delta) {
      const active = document.querySelector('.tab-btn.active');
      const cur = active && active.dataset.tab ? active.dataset.tab : TAB_ORDER[0];
      let i = TAB_ORDER.indexOf(cur);
      if (i < 0) i = 0;
      i = Math.max(0, Math.min(TAB_ORDER.length - 1, i + delta));
      const next = TAB_ORDER[i];
      const btn = document.querySelector('.tab-btn[data-tab="' + next + '"]');
      if (btn) switchTab(next, btn);
    }

    function updateTabNavButtons() {
      const active = document.querySelector('.tab-btn.active');
      const cur = active && active.dataset.tab ? active.dataset.tab : TAB_ORDER[0];
      const i = TAB_ORDER.indexOf(cur);
      const back = document.getElementById('btn-tab-back');
      const next = document.getElementById('btn-tab-next');
      if (back) back.disabled = i <= 0;
      if (next) next.disabled = i >= TAB_ORDER.length - 1;
    }
    updateTabNavButtons();

    function openToolManualModal() {
      const d = document.getElementById('toolManualModal');
      if (d && typeof d.showModal === 'function') d.showModal();
    }

    // ── Advanced accordion ──
    function toggleAdv(bodyId, triggerId) {
      const body = document.getElementById(bodyId);
      const trig = document.getElementById(triggerId);
      body.classList.toggle('open');
      trig.classList.toggle('open');
    }

    function togglePipelineFlow() {
      const body = document.getElementById('pipelineFlowBody');
      const trig = document.getElementById('trigPipelineFlow');
      if (!body || !trig) return;
      body.classList.toggle('open');
      trig.classList.toggle('open');
      trig.setAttribute('aria-expanded', body.classList.contains('open') ? 'true' : 'false');
    }

    function toggleChecklistPanel() {
      const body = document.getElementById('checklistBody');
      const trig = document.getElementById('trigChecklistPanel');
      if (!body || !trig) return;
      body.classList.toggle('open');
      trig.classList.toggle('open');
      trig.setAttribute('aria-expanded', body.classList.contains('open') ? 'true' : 'false');
    }

    function toggleJsonPanel() {
      const body = document.getElementById('jsonPanelBody');
      const trig = document.getElementById('trigJsonPanel');
      if (!body || !trig) return;
      body.classList.toggle('open');
      trig.classList.toggle('open');
      trig.setAttribute('aria-expanded', body.classList.contains('open') ? 'true' : 'false');
    }

    // ── Conditional UI toggles ──
    function toggleSparseModel() {
      document.getElementById('sparseModelWrap').style.display =
        document.getElementById('sparseEnabled').checked ? '' : 'none';
    }
    function toggleHydeModel() {
      document.getElementById('hydeWrap').style.display =
        document.getElementById('hydeEnabled').checked ? '' : 'none';
    }
    function toggleRerankerModel() {
      document.getElementById('rerankerWrap').style.display =
        document.getElementById('rerankerEnabled').checked ? '' : 'none';
    }
    function toggleWebFallback() {
      document.getElementById('webFallbackWrap').style.display =
        document.getElementById('webFallbackEnabled').checked ? '' : 'none';
    }
    function toggleFaithfulness() {
      const on = document.getElementById('faithfulnessEnabled').checked;
      document.getElementById('faithfulnessWrap').style.display = on ? '' : 'none';
      const adv = document.getElementById('faithfulnessAdvWrap');
      if (adv) adv.style.display = on ? '' : 'none';
    }
    function toggleMLflow() {
      document.getElementById('mlflowWrap').style.display =
        document.getElementById('mlflowEnabled').checked ? '' : 'none';
    }
    function toggleLangSmith() {
      const wrap = document.getElementById('langsmithWrap');
      if (wrap) wrap.style.display = document.getElementById('langsmithEnabled').checked ? 'block' : 'none';
    }
    function toggleRagas() {
      const on = document.getElementById('ragasEnabled').checked;
      document.getElementById('ragasWrap').style.display = on ? '' : 'none';
      const adv = document.getElementById('ragasAdvWrap');
      if (adv) adv.style.display = on ? '' : 'none';
    }
    function updateModelPlaceholder() {
      const map = {
        gemini:       'gemini-2.0-flash',
        openai:       'gpt-4o',
        anthropic:    'claude-opus-4-6',
        databricks:   'databricks-meta-llama-3-3-70b-instruct',
        azure_openai: 'gpt-4o (deployment name)',
      };
      const v = document.getElementById('llmProvider').value;
      const inp = document.getElementById('llmModel');
      inp.placeholder = 'e.g. ' + (map[v] || 'model-id');
      // Auto-fill default when field is blank or still shows the previous default
      const defaults = Object.values(map);
      if (!inp.value.trim() || defaults.includes(inp.value.trim())) {
        inp.value = map[v] || '';
      }
    }

    // ── Build config ──
    function buildConfig() {
      const v = id => document.getElementById(id)?.value ?? '';
      const b = id => document.getElementById(id)?.checked ?? false;
      const r = name => document.querySelector(`input[name="${name}"]:checked`)?.value ?? '';
      return {
        ingestion: {
          source_type: v('sourceType'), metadata_enrichment: v('metadataEnrich'),
          chunker: {
            strategy: v('chunkStrategy'), breakpoint_type: v('breakpointType'),
            breakpoint_threshold: parseFloat(v('breakpointAmt')),
            fallback_chunk_size: parseInt(v('chunkSize')), chunk_overlap: parseInt(v('chunkOverlap'))
          },
          embeddings: {
            dense_model: v('denseModel'), databricks_endpoint: v('embeddingEndpoint'),
            normalize: r('normEmbed') === 'true',
            sparse_enabled: b('sparseEnabled'),
            sparse_model: b('sparseEnabled') ? v('sparseModel') : null
          },
          databricks: {
            catalog: v('dbCatalog'), schema: v('dbSchema'), table: v('dbTable'),
            vs_endpoint: v('vsEndpoint'), index_name: v('vsIndex'),
            primary_key: v('primaryKey'), content_column: v('contentCol'),
            pipeline_type: r('pipelineType')
          }
        },
        retrieval: {
          top_k: parseInt(v('topK')), rerank_top_n: parseInt(v('topN')),
          query_type: r('queryType'),
          retrieve_columns: v('retrieveCols').split(',').map(s => s.trim()),
          hyde: { enabled: b('hydeEnabled'), llm: v('llmModel'), apply_on: v('hydeApplyOn') },
          reranker: { enabled: b('rerankerEnabled'), model: v('rerankerModel'), batch_size: parseInt(v('rerankerBatch')) }
        },
        crag: {
          relevance_threshold: parseFloat(v('relevanceThreshold')),
          grader_llm: v('llmModel'), grader_top_docs: parseInt(v('graderTopDocs')),
          grader_temperature: parseFloat(v('graderTemp')),
          max_iterations: parseInt(v('maxIterations')), requery_strategy: v('requeueStrategy'),
          web_fallback: {
            enabled: b('webFallbackEnabled'), provider: v('webProvider'),
            max_results: parseInt(v('webMaxResults')),
            trigger_score: parseFloat(v('webFallbackScore'))
          },
          tools: { python_repl: b('toolPython'), databricks_sql: b('toolSQL'), calculator: b('toolCalc') }
        },
        generation: {
          llm_provider: v('llmProvider'), llm_model: v('llmModel'),
          temperature: parseFloat(v('llmTemp')), max_tokens: parseInt(v('llmMaxTokens')),
          system_prompt: v('systemPrompt'),
          citations: { enabled: b('citationsEnabled'), resolve_check: b('citationResolve') },
          faithfulness: {
            enabled: b('faithfulnessEnabled'), nli_model: v('nliModel'),
            threshold: parseFloat(v('faithfulnessThreshold')), max_regeneration: parseInt(v('maxRegen'))
          },
          guardrails_ai: b('guardrailsEnabled')
        },
        observability: {
          mlflow: {
            enabled: b('mlflowEnabled'), tracking_uri: v('mlflowURI'),
            experiment: v('mlflowExperiment'), latency_alert_ms: parseInt(v('mlflowLatencyAlert'))
          },
          langsmith: { enabled: b('langsmithEnabled'), project: v('langsmithProject') },
          ragas: {
            enabled: b('ragasEnabled'),
            metrics: {
              faithfulness: b('metricFaithfulness'), answer_relevancy: b('metricAnswerRel'),
              context_recall: b('metricContextRecall'), context_precision: b('metricContextPrec')
            },
            min_faithfulness: parseFloat(v('ragasFaithThresh')),
            min_answer_relevancy: parseFloat(v('ragasRelThresh')),
            log_to_mlflow: r('ragasMLflow') === 'yes'
          }
        }
      };
    }

    // ── Summary flow labels ──
    function updateFlowLabels(cfg) {
      const denseShort = (cfg.ingestion.embeddings.dense_model||'').split('/').pop().replace('bge-large-en-v1.5','BGE-large');
      document.getElementById('flowEmbedLabel').textContent = denseShort || 'BGE-large';
      document.getElementById('flowRetrieveLabel').textContent =
        cfg.retrieval.query_type === 'hybrid' ? `Hybrid k=${cfg.retrieval.top_k}` : `Semantic k=${cfg.retrieval.top_k}`;
      document.getElementById('flowGradeLabel').textContent = `CRAG ≥${cfg.crag.relevance_threshold.toFixed(2)}`;
      document.getElementById('flowGenLabel').textContent = cfg.generation.llm_model || 'LLM';
      document.getElementById('flowGuardLabel').textContent =
        cfg.generation.faithfulness.enabled ? `NLI ≥${cfg.generation.faithfulness.threshold.toFixed(2)}` : 'Off';
      document.getElementById('flowMaxIter').textContent = cfg.crag.max_iterations;
      document.getElementById('flowWebScore').textContent =
        cfg.crag.web_fallback.enabled ? cfg.crag.web_fallback.trigger_score.toFixed(2) : 'N/A';
    }

    function refreshJSON() {
      const cfg = buildConfig();
      updateFlowLabels(cfg);
      document.getElementById('jsonOutput').textContent = JSON.stringify(cfg, null, 2);
      runValidate();   // auto-validate when Summary tab opens
    }
    function downloadJSON() {
      const blob = new Blob([JSON.stringify(buildConfig(), null, 2)], {type: 'application/json'});
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob); a.download = 'rag_config.json'; a.click();
    }

    // ── Save / Load ──
    const STORAGE_KEY = 'vikaa_rag_config_v1';
    function saveConfig() {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(buildConfig()));
      showToast('Draft saved locally (browser)');
    }
    function resetConfig() {
      if (!confirm('Reset all settings to defaults?')) return;
      localStorage.removeItem(STORAGE_KEY); location.reload();
    }

    // ── Summary: Validate, Save, Load ────────────────────────────────────────
    async function runValidate() {
      const cfg = buildConfig();
      const listEl    = document.getElementById('checklist');
      const logEl     = document.getElementById('checklistLog');
      const summaryEl = document.getElementById('checklistSummary');
      listEl.innerHTML = '<li style="color:#9ca3af;font-size:13px;"><i class="fas fa-spinner fa-spin"></i> Checking…</li>';
      logEl.style.display = 'none';
      summaryEl.style.display = 'none';
      try {
        const r = await fetch(`${getApiBase()}/tools/rag-configurator/summary/validate`, {
          method: 'POST',
          headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' },
          body: JSON.stringify({ config: cfg, check_index: true }),
        });
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || JSON.stringify(data));

        listEl.innerHTML = (data.items || []).map(item => {
          const icon  = item.passed ? 'fa-circle-check' : 'fa-circle-xmark';
          const color = item.passed ? '#16a34a' : '#dc2626';
          const detail = item.detail ? `<span style="font-size:11px;opacity:.65;margin-left:6px;">${item.detail}</span>` : '';
          return `<li><i class="fas ${icon}" style="color:${color};"></i> ${item.label}${detail}</li>`;
        }).join('');

        const allOk = data.all_ok;
        summaryEl.style.display = 'block';
        summaryEl.innerHTML = allOk
          ? `<span style="color:#16a34a;"><i class="fas fa-circle-check"></i> All ${data.total} checks passed — ready for production</span>`
          : `<span style="color:#a16207;"><i class="fas fa-triangle-exclamation"></i> ${data.passed}/${data.total} checks passed</span>`;

      } catch(err) {
        listEl.innerHTML = '';
        logEl.textContent = '✗ Validation error: ' + err.message;
        logEl.className = 'ing-log ing-log--error';
        logEl.style.display = 'block';
      }
    }


    async function loadConfigFromServer() {
      setSummaryLog('Loading saved config…', 'info');
      try {
        const r = await fetch(`${getApiBase()}/tools/rag-configurator/summary/load`, {
          headers: getAuthHeaders(),
        });
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || JSON.stringify(data));
        _applyConfig(data.config);
        setSummaryLog('✓ Config loaded from server — UI updated', 'success');
        showToast('Config loaded from server');
        refreshJSON();
      } catch(err) {
        setSummaryLog('✗ ' + err.message, 'error');
      }
    }

    function _applyConfig(cfg) {
      const set  = (id, val) => { const el = document.getElementById(id); if (el && val !== null && val !== undefined) el.value = val; };
      const chk  = (id, val) => { const el = document.getElementById(id); if (el) el.checked = !!val; };
      const rad  = (name, val) => { const el = document.querySelector(`input[name="${name}"][value="${val}"]`); if (el) el.checked = true; };
      const slid = (id, dispId, val) => { set(id, val); const d = document.getElementById(dispId); if (d && val !== undefined) d.textContent = parseFloat(val).toFixed(2); };

      const i = cfg.ingestion || {};
      set('sourceType', i.source_type); set('metadataEnrich', i.metadata_enrichment);
      if (i.chunker) {
        set('chunkStrategy', i.chunker.strategy); set('breakpointType', i.chunker.breakpoint_type);
        slid('breakpointAmt', 'bpAmtVal', i.chunker.breakpoint_threshold);
        set('chunkSize', i.chunker.fallback_chunk_size); set('chunkOverlap', i.chunker.chunk_overlap);
      }
      if (i.embeddings) {
        set('denseModel', i.embeddings.dense_model); set('embeddingEndpoint', i.embeddings.databricks_endpoint);
        rad('normEmbed', i.embeddings.normalize ? 'true' : 'false');
        chk('sparseEnabled', i.embeddings.sparse_enabled); if (i.embeddings.sparse_model) set('sparseModel', i.embeddings.sparse_model);
        toggleSparseModel();
      }
      if (i.databricks) {
        set('dbCatalog', i.databricks.catalog); set('dbSchema', i.databricks.schema);
        set('dbTable', i.databricks.table); set('vsEndpoint', i.databricks.vs_endpoint);
        set('vsIndex', i.databricks.index_name); set('primaryKey', i.databricks.primary_key);
        set('contentCol', i.databricks.content_column); rad('pipelineType', i.databricks.pipeline_type);
      }

      const ret = cfg.retrieval || {};
      set('topK', ret.top_k); set('topN', ret.rerank_top_n); rad('queryType', ret.query_type);
      set('retrieveCols', (ret.retrieve_columns || []).join(','));
      if (ret.hyde) { chk('hydeEnabled', ret.hyde.enabled); if (ret.hyde.apply_on) set('hydeApplyOn', ret.hyde.apply_on); toggleHydeModel(); }
      if (ret.reranker) { chk('rerankerEnabled', ret.reranker.enabled); set('rerankerModel', ret.reranker.model); set('rerankerBatch', ret.reranker.batch_size); toggleRerankerModel(); }

      const crag = cfg.crag || {};
      slid('relevanceThreshold', 'relThreshVal', crag.relevance_threshold);
      set('graderTopDocs', crag.grader_top_docs);
      slid('graderTemp', 'graderTempVal', crag.grader_temperature);
      set('maxIterations', crag.max_iterations); if (crag.requery_strategy) set('requeueStrategy', crag.requery_strategy);
      if (crag.web_fallback) {
        chk('webFallbackEnabled', crag.web_fallback.enabled); if (crag.web_fallback.provider) set('webProvider', crag.web_fallback.provider);
        set('webMaxResults', crag.web_fallback.max_results);
        slid('webFallbackScore', 'wfScoreVal', crag.web_fallback.trigger_score); toggleWebFallback();
      }
      if (crag.tools) { chk('toolPython', crag.tools.python_repl); chk('toolSQL', crag.tools.databricks_sql); chk('toolCalc', crag.tools.calculator); }

      const gen = cfg.generation || {};
      if (gen.llm_provider) set('llmProvider', gen.llm_provider);
      set('llmModel', gen.llm_model);
      slid('llmTemp', 'llmTempVal', gen.temperature);
      set('llmMaxTokens', gen.max_tokens); set('systemPrompt', gen.system_prompt);
      if (gen.citations) { chk('citationsEnabled', gen.citations.enabled); chk('citationResolve', gen.citations.resolve_check); }
      if (gen.faithfulness) {
        chk('faithfulnessEnabled', gen.faithfulness.enabled); if (gen.faithfulness.nli_model) set('nliModel', gen.faithfulness.nli_model);
        slid('faithfulnessThreshold', 'faithThreshVal', gen.faithfulness.threshold);
        set('maxRegen', gen.faithfulness.max_regeneration); toggleFaithfulness();
      }
      chk('guardrailsEnabled', gen.guardrails_ai);

      const obs = cfg.observability || {};
      if (obs.mlflow) { chk('mlflowEnabled', obs.mlflow.enabled); set('mlflowURI', obs.mlflow.tracking_uri); set('mlflowExperiment', obs.mlflow.experiment); set('mlflowLatencyAlert', obs.mlflow.latency_alert_ms); toggleMLflow(); }
      if (obs.langsmith) { chk('langsmithEnabled', obs.langsmith.enabled); set('langsmithProject', obs.langsmith.project); toggleLangSmith(); }
      if (obs.ragas) {
        chk('ragasEnabled', obs.ragas.enabled);
        if (obs.ragas.metrics) { chk('metricFaithfulness', obs.ragas.metrics.faithfulness); chk('metricAnswerRel', obs.ragas.metrics.answer_relevancy); chk('metricContextRecall', obs.ragas.metrics.context_recall); chk('metricContextPrec', obs.ragas.metrics.context_precision); }
        slid('ragasFaithThresh', 'ragasFaithVal', obs.ragas.min_faithfulness);
        slid('ragasRelThresh', 'ragasRelVal', obs.ragas.min_answer_relevancy);
        rad('ragasMLflow', obs.ragas.log_to_mlflow ? 'yes' : 'no'); toggleRagas();
      }
    }

    function setSummaryLog(msg, type) {
      const el = document.getElementById('summaryLog');
      el.textContent = msg;
      el.className = 'ing-log ing-log--' + type;
      el.style.display = 'block';
    }

    // ── Toast ──
    function showToast(msg) {
      const t = document.getElementById('toast');
      document.getElementById('toastMsg').textContent = msg;
      t.classList.add('show');
      setTimeout(() => t.classList.remove('show'), 2800);
    }

    // ── Restore saved config ──
    (function restoreConfig() {
      try {
        const saved = localStorage.getItem(STORAGE_KEY);
        if (!saved) return;
        _applyConfig(JSON.parse(saved));
      } catch(e) { console.warn('Could not restore RAG config:', e); }
    })();

    // ── API helpers ──────────────────────────────────────────────────────────
    function getApiBase() {
      const b = (typeof CONFIG !== 'undefined' && CONFIG.API_BASE_URL) ? CONFIG.API_BASE_URL : 'http://localhost:10000';
      return String(b).replace(/\/$/, '');
    }
    function getAuthHeaders() {
      const raw = localStorage.getItem('authData');
      let token = null;
      try { token = raw ? JSON.parse(raw)?.accessToken : null; } catch(_) {}
      if (token) return { 'Authorization': `Bearer ${token}` };
      const h = (window.location.hostname || '').toLowerCase();
      if (h === 'localhost' || h === '127.0.0.1') return {};
      throw new Error('Please log in to use this feature.');
    }

    // ── File drop zone (multi-file, combined max 20 MB, same doc type) ───────
    const INGEST_MAX_BYTES = 20 * 1024 * 1024;
    let _ingestFiles = [];
    let _ingestBatchIndex = 0;
    let _ingestBatchLog = [];

    function _ingestFileKind(file) {
      const ext = file.name.split('.').pop().toLowerCase();
      const extMap = {
        pdf: 'pdf', html: 'html', htm: 'html',
        json: 'json', jsonl: 'json',
        txt: 'text', md: 'text',
        csv: 'csv', xml: 'xml',
        xlsx: 'excel', xls: 'excel',
        docx: 'word', doc: 'word',
        pptx: 'ppt', ppt: 'ppt',
      };
      return extMap[ext] || null;
    }

    function _sourceTypeForKind(kind) {
      const stMap = {
        pdf: 'pdf', html: 'html', json: 'json', text: 'multi',
        csv: 'csv', xml: 'xml', excel: 'excel', word: 'word', ppt: 'ppt',
      };
      return stMap[kind] || 'multi';
    }

    function renderIngestFileList() {
      const wrap = document.getElementById('ingestFileListWrap');
      const ul = document.getElementById('ingestFileList');
      const hint = document.getElementById('ingestTotalHint');
      ul.innerHTML = '';
      if (!_ingestFiles.length) {
        wrap.hidden = true;
        document.getElementById('btnIngest').disabled = true;
        if (hint) hint.textContent = '';
        return;
      }
      wrap.hidden = false;
      document.getElementById('btnIngest').disabled = false;
      const total = _ingestFiles.reduce((s, f) => s + f.size, 0);
      if (hint) {
        hint.textContent = `(${_ingestFiles.length} · ${_fmtBytes(total)} / ${_fmtBytes(INGEST_MAX_BYTES)})`;
      }
      _ingestFiles.forEach((file, i) => {
        const li = document.createElement('li');
        li.className = 'ingest-file-item';
        const row = document.createElement('div');
        row.className = 'file-info-row';
        const icon = document.createElement('i');
        icon.className = 'fas fa-file-circle-check dz-icon ok';
        icon.style.cssText = 'font-size:1.15rem;margin:0;flex-shrink:0;';
        const meta = document.createElement('div');
        meta.className = 'ingest-file-meta';
        const nameEl = document.createElement('div');
        nameEl.className = 'fi-name';
        nameEl.textContent = file.name;
        const sizeEl = document.createElement('div');
        sizeEl.className = 'fi-size';
        sizeEl.textContent = _fmtBytes(file.size);
        meta.append(nameEl, sizeEl);
        const rm = document.createElement('button');
        rm.type = 'button';
        rm.className = 'btn btn-ghost fi-clear';
        rm.innerHTML = '<i class="fas fa-xmark"></i> Remove';
        rm.onclick = () => removeIngestFile(i);
        row.append(icon, meta, rm);
        li.appendChild(row);
        ul.appendChild(li);
      });
    }

    function onFileSelect(e) {
      const picked = Array.from(e.target?.files || e.dataTransfer?.files || []);
      if (!picked.length) return;

      for (const f of picked) {
        if (!_ingestFileKind(f)) {
          setIngestionLog(`Unsupported type: "${f.name}". Use PDF, HTML, JSON, TXT, or MD.`, 'error');
          if (e.target && e.target.files) e.target.value = '';
          return;
        }
        if (f.size > INGEST_MAX_BYTES) {
          setIngestionLog(`"${f.name}" exceeds 20 MB (single file limit).`, 'error');
          if (e.target && e.target.files) e.target.value = '';
          return;
        }
      }

      const merged = _ingestFiles.slice();
      for (const f of picked) {
        if (!merged.some((x) => x.name === f.name && x.size === f.size)) merged.push(f);
      }
      const total = merged.reduce((s, f) => s + f.size, 0);
      if (total > INGEST_MAX_BYTES) {
        setIngestionLog(`Total size ${_fmtBytes(total)} exceeds combined 20 MB limit.`, 'error');
        if (e.target && e.target.files) e.target.value = '';
        return;
      }

      const kinds = [...new Set(merged.map(_ingestFileKind))];
      if (kinds.length > 1) {
        setIngestionLog('Multiple files must be the same type (all PDF, all HTML, all JSON, or all TXT/MD).', 'error');
        if (e.target && e.target.files) e.target.value = '';
        return;
      }

      _ingestFiles = merged;
      document.getElementById('sourceType').value = _sourceTypeForKind(kinds[0]);
      renderIngestFileList();
      if (e.target && e.target.files) e.target.value = '';
    }

    function removeIngestFile(index) {
      _ingestFiles.splice(index, 1);
      _ingestBatchIndex = 0;
      renderIngestFileList();
    }

    function clearIngestFile() {
      _ingestFiles = [];
      _ingestBatchIndex = 0;
      const fi = document.getElementById('fileInput');
      if (fi) fi.value = '';
      renderIngestFileList();
    }
    function _fmtBytes(b) {
      if (b < 1024) return b + ' B';
      if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
      return (b / 1048576).toFixed(2) + ' MB';
    }
    function onDragOver(e)  { e.preventDefault(); e.currentTarget.classList.add('drag-over'); }
    function onDragLeave(e) { e.currentTarget.classList.remove('drag-over'); }
    function onDrop(e) {
      e.preventDefault();
      e.currentTarget.classList.remove('drag-over');
      onFileSelect(e);
    }

    // ── Step 2 — Provision ───────────────────────────────────────────────────
    async function provisionPipeline() {
      const btn = document.getElementById('btnProvision');
      btn.disabled = true;
      btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Provisioning…';
      setIngestionLog('Provisioning Delta table + VS index…', 'info');
      try {
        const cfg = buildConfig();
        const r = await fetch(`${getApiBase()}/tools/rag-configurator/provision`, {
          method: 'POST',
          headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' },
          body: JSON.stringify({ ingestion: cfg.ingestion }),
        });
        const data = await r.json();
        if (!r.ok) throw new Error(typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail || data));
        const indexReady = ['ONLINE','ONLINE_NO_PENDING_UPDATE'].includes(data.index_state);
        btn.innerHTML = '<i class="fas fa-circle-check"></i> Provisioned';
        btn.style.cssText += ';background:#d1fae5;color:#065f46;';
        setIngestionLog(
          `${indexReady ? '✓' : '⚠'} Table: ${data.table_fqn}  |  Index: ${data.index}  |  State: ${data.index_state}` +
          (indexReady ? '' : '  — index initializing, check status below'),
          indexReady ? 'success' : 'warn'
        );
        refreshIndexStatus();
      } catch(err) {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-layer-group"></i> Retry Provision';
        setIngestionLog('✗ ' + err.message, 'error');
      }
    }

    // ── Step 3 — Ingest (duplicate-aware, multi-file sequential) ─────────────
    async function runIngest(mode = 'check') {
      if (!_ingestFiles.length) return;
      const btn = document.getElementById('btnIngest');
      if (mode === 'check') {
        document.getElementById('dupCard').style.display = 'none';
        _ingestBatchIndex = 0;
        _ingestBatchLog = [];
      }
      await _runIngestStep(mode, btn);
    }

    async function _runIngestStep(mode, btn) {
      const n = _ingestFiles.length;
      const idx = _ingestBatchIndex;
      if (idx >= n) {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-upload"></i> Upload &amp; Ingest';
        _ingestBatchLog.push(`✓ Completed ${n} file(s).`);
        setIngestionLog(_ingestBatchLog.join('\n'), 'success');
        setTimeout(refreshIndexStatus, 2500);
        return;
      }

      const file = _ingestFiles[idx];
      btn.disabled = true;
      btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Ingesting…';
      setIngestionLog(
        mode === 'check'
          ? `Checking for duplicates (${idx + 1}/${n}): "${file.name}"…`
          : `Mode: ${mode} — (${idx + 1}/${n}) "${file.name}" (${_fmtBytes(file.size)})…`,
        'info'
      );

      try {
        const cfg = buildConfig();
        const form = new FormData();
        form.append('file', file, file.name);
        form.append('ingestion_config', JSON.stringify(cfg.ingestion));
        form.append('mode', mode);
        const headers = { ...getAuthHeaders() };
        const r = await fetch(`${getApiBase()}/tools/rag-configurator/ingest`, {
          method: 'POST', headers, body: form,
        });
        const data = await r.json();
        if (!r.ok) throw new Error(typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail || data));

        if (data.action_required) {
          btn.disabled = false;
          btn.innerHTML = '<i class="fas fa-upload"></i> Upload &amp; Ingest';
          _showDupCard(data);
          return;
        }

        if (data.skipped) {
          _ingestBatchLog.push(`⊘ [${idx + 1}/${n}] ${file.name}: skipped — not re-ingested.`);
          setIngestionLog(_ingestBatchLog.join('\n'), 'info');
          _ingestBatchIndex++;
          await _runIngestStep('check', btn);
          return;
        }

        const delNote = data.chunks_deleted > 0 ? `  |  Replaced: ${data.chunks_deleted} old chunks removed` : '';
        const dupNote = data.duplicate_info ? `  |  ⚠ duplicate ingested as ${mode}` : '';
        const syncNote = data.sync_triggered
          ? 'triggered'
          : data.sync_pending
            ? 'index still initializing — will sync automatically once ONLINE'
            : data.sync_error ? data.sync_error : 'skipped';
        const partialFail = data.chunks_inserted < data.chunks_total;
        const hasWarning = data.sync_pending || data.sync_error;
        const logType = partialFail ? 'error' : hasWarning ? 'warn' : 'success';
        const prefix = logType === 'success' ? '✓' : logType === 'warn' ? '⚠' : '✗';
        _ingestBatchLog.push(
          `${prefix} [${idx + 1}/${n}] ${file.name}: ${data.chunks_inserted}/${data.chunks_total} chunks` +
            `  |  Pages: ${data.pages_loaded}  |  VS sync: ${syncNote}` +
            delNote + dupNote
        );
        setIngestionLog(_ingestBatchLog.join('\n'), logType);

        if (partialFail) {
          _ingestBatchLog.push(`✗ [${idx + 1}/${n}] ${file.name}: partial insert (${data.chunks_inserted}/${data.chunks_total} chunks).`);
          setIngestionLog(_ingestBatchLog.join('\n'), 'error');
          btn.disabled = false;
          btn.innerHTML = '<i class="fas fa-upload"></i> Upload &amp; Ingest';
          return;
        }

        _ingestBatchIndex++;
        await _runIngestStep('check', btn);
      } catch (err) {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-upload"></i> Upload &amp; Ingest';
        setIngestionLog('✗ ' + err.message, 'error');
      }
    }

    function _showDupCard(data) {
      const card  = document.getElementById('dupCard');
      const title = document.getElementById('dupTitle');
      const msg   = document.getElementById('dupMsg');
      const btnR  = document.getElementById('dupBtnReplace');
      card.style.display = 'block';
      if (data.duplicate_type === 'exact_content') {
        title.textContent = 'Exact duplicate detected';
        const names = (data.all_sources || [data.existing_source]).join(', ');
        msg.innerHTML =
          `This file's content already exists in the index as <strong>${names}</strong> ` +
          `(${data.existing_chunks} chunks). ` +
          `What would you like to do?`;
        btnR.innerHTML = '<i class="fas fa-arrows-rotate"></i> Replace existing';
      } else {
        title.textContent = 'Same filename — different content';
        msg.innerHTML =
          `<strong>${data.existing_source}</strong> is already in the index ` +
          `(${data.existing_chunks} chunks) but the file content has changed. ` +
          `<strong>Replace</strong> to delete the old version and re-ingest, ` +
          `or <strong>Append</strong> to keep both.`;
        btnR.innerHTML = '<i class="fas fa-arrows-rotate"></i> Replace (update)';
      }
      setIngestionLog(`Duplicate found (${data.duplicate_type}) — choose an action above.`, 'warn' );
    }

    function resolveIngest(mode) {
      document.getElementById('dupCard').style.display = 'none';
      const btn = document.getElementById('btnIngest');
      _runIngestStep(mode, btn);
    }

    function setIngestionLog(msg, type) {
      const el = document.getElementById('ingestionLog');
      el.textContent = msg;
      el.className = 'ing-log ing-log--' + type;
      el.style.display = 'block';
    }

    // ── Retrieval test ───────────────────────────────────────────────────────
    async function runRetrieval() {
      const query = document.getElementById('retQuery').value.trim();
      if (!query) { setRetLog('Enter a query first.', 'warn'); return; }

      const btn = document.getElementById('btnRetrieve');
      btn.disabled = true;
      btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Searching…';
      setRetLog('Running retrieval pipeline…', 'info');
      document.getElementById('retResults').innerHTML = '';
      document.getElementById('retPipelineTags').style.display = 'none';

      try {
        const cfg = buildConfig();

        // Allow per-query overrides without changing saved config
        const retCfg = JSON.parse(JSON.stringify(cfg.retrieval));
        if (document.getElementById('retOverrideHyde').checked) {
          retCfg.hyde = { ...(retCfg.hyde || {}), enabled: true, apply_on: 'always' };
        }
        if (!document.getElementById('retOverrideRerank').checked) {
          retCfg.reranker = { ...(retCfg.reranker || {}), enabled: false };
        }

        const body = {
          query,
          retrieval: retCfg,
          ingestion: cfg.ingestion,
          is_retry: document.getElementById('retIsRetry').checked,
          confidence: null,
          synthesize: document.getElementById('retSynthesize').checked,
          synthesis_model: cfg.generation?.llm_model || retCfg.hyde?.llm || 'gemini-2.0-flash',
        };

        const r = await fetch(`${getApiBase()}/tools/rag-configurator/retrieve`, {
          method: 'POST',
          headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const data = await r.json();
        if (!r.ok) throw new Error(typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail || data));

        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-magnifying-glass"></i> Search';

        // ── Update auto-flag state from result ───────────────────────────────
        _lastRetCount = data.count || 0;
        if (data.results?.length) {
          const scores = data.results.map(c => parseFloat(c.rerank_score ?? c.score ?? 0)).filter(s => !isNaN(s));
          _lastRetAvgScore = scores.length ? scores.reduce((a,b) => a+b, 0) / scores.length : null;
        } else {
          _lastRetAvgScore = null;
        }
        _retManual.delete('retIsRetry'); // allow auto to re-evaluate retry flag after a result
        _autoSetRetFlags();

        if (!data.count) {
          setRetLog('No results returned. Check that the index is ONLINE and has been synced.', 'warn');
          return;
        }

        // Pipeline tags
        const tagsEl = document.getElementById('retPipelineTags');
        tagsEl.style.display = 'block';
        tagsEl.innerHTML =
          `<span class="ret-pipeline-tag ret-pipeline-tag--vs"><i class="fas fa-magnifying-glass"></i> VS ${(data.query_type||'hybrid').toUpperCase()}</span>` +
          (data.hyde_applied ? `<span class="ret-pipeline-tag ret-pipeline-tag--hyde"><i class="fas fa-wand-magic-sparkles"></i> HyDE applied</span>` : '') +
          (data.reranked     ? `<span class="ret-pipeline-tag ret-pipeline-tag--rerank"><i class="fas fa-ranking-star"></i> Re-ranked</span>` : '');

        setRetLog(
          `✓ ${data.count} chunk${data.count !== 1 ? 's' : ''} retrieved` +
          `  |  Index: ${data.index}` +
          (data.hyde_applied ? `  |  HyDE expanded query` : '') +
          (data.reranked ? `  |  Cross-encoder re-ranked` : ''),
          'success'
        );

        // Render answer + source chunks
        const container = document.getElementById('retResults');
        const answerHtml = _renderAnswer(data, query);
        const chunksHtml = data.results.map((chunk, i) => _retCard(chunk, i)).join('');
        container.innerHTML = answerHtml +
          `<button class="ret-sources-toggle" id="retSourcesToggle" onclick="toggleSources()">
            <i class="fas fa-chevron-down"></i>
            Source Chunks (${data.count})
          </button>
          <div class="ret-sources-body" id="retSourcesBody">
            <div style="padding-top:8px;">${chunksHtml}</div>
          </div>`;

      } catch(err) {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-magnifying-glass"></i> Search';
        setRetLog('✗ ' + err.message, 'error');
      }
    }

    function _renderAnswer(data, query) {
      if (data.synthesis_error) {
        return `<div class="ret-answer-card">
          <div class="ret-answer-title"><i class="fas fa-circle-xmark" style="color:#dc2626;"></i> Synthesis failed</div>
          <div class="ret-answer-error">${data.synthesis_error}</div>
        </div>`;
      }
      if (data.answer) {
        const escaped = data.answer.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
        return `<div class="ret-answer-card">
          <div class="ret-answer-title"><i class="fas fa-circle-check"></i> Answer</div>
          <div class="ret-answer-body">${escaped}</div>
        </div>`;
      }
      // No synthesis — show note instead
      return `<div class="ret-context-note" style="margin-bottom:10px;">
        <i class="fas fa-circle-info"></i>
        <span>Enable <strong>Synthesize answer</strong> to get a direct response. Below are the raw retrieved chunks.</span>
      </div>`;
    }

    function toggleSources() {
      const btn  = document.getElementById('retSourcesToggle');
      const body = document.getElementById('retSourcesBody');
      btn.classList.toggle('open');
      body.classList.toggle('open');
    }

    function _cleanChunkText(raw) {
      return (raw || '')
        .replace(/\r\n|\r/g, '\n')          // normalise line endings
        .replace(/[ \t]+/g, ' ')            // collapse horizontal whitespace
        .replace(/\n{3,}/g, '\n\n')         // max 2 consecutive blank lines
        .replace(/[^\x20-\x7E\xA0-\u024F\n]/g, ' ') // drop non-printable / odd PDF chars
        .replace(/ {2,}/g, ' ')             // collapse any remaining double spaces
        .trim();
    }

    function _retCard(chunk, index) {
      const vsScore     = chunk.score       != null ? parseFloat(chunk.score).toFixed(3)       : null;
      const rerankScore = chunk.rerank_score != null ? parseFloat(chunk.rerank_score).toFixed(3) : null;
      const rawContent  = chunk.content || chunk.text || '';
      const content     = _cleanChunkText(rawContent)
                            .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
      const source      = (chunk.source || '—').split('/').pop().split('\\').pop(); // filename only
      const page        = chunk.page != null ? `Page ${Math.round(chunk.page)}` : '';
      const chunkId     = chunk.chunk_id ? chunk.chunk_id.slice(0, 10) + '…' : '';
      const cardId      = `retcard-${index}`;
      const metaParts   = [page, chunkId].filter(Boolean);
      return `
        <div class="ret-result-card">
          <div class="ret-result-header">
            <div class="ret-rank">${index + 1}</div>
            <div class="ret-source-wrap">
              <div class="ret-source" title="${source}">${source}</div>
              ${metaParts.length ? `<div class="ret-meta">${metaParts.join('  ·  ')}</div>` : ''}
            </div>
            <div class="ret-score-wrap">
              ${vsScore     ? `<span class="ret-score ret-score--vs" title="VS similarity score">VS&nbsp;${vsScore}</span>` : ''}
              ${rerankScore ? `<span class="ret-score ret-score--rerank" title="Cross-encoder re-rank score">↑&nbsp;${rerankScore}</span>` : ''}
            </div>
          </div>
          <div class="ret-divider"></div>
          <div class="ret-content" id="${cardId}-content">${content}</div>
          <button class="ret-toggle-expand" onclick="toggleRetContent('${cardId}')">show more</button>
        </div>`;
    }

    function toggleRetContent(cardId) {
      const el  = document.getElementById(cardId + '-content');
      const btn = el.nextElementSibling;
      const expanded = el.classList.toggle('expanded');
      btn.textContent = expanded ? 'show less' : 'show more';
    }

    function setRetLog(msg, type) {
      const el = document.getElementById('retLog');
      el.textContent = msg;
      el.className = 'ing-log ing-log--' + type;
      el.style.display = 'block';
    }

    // ── CRAG test ────────────────────────────────────────────────────────────
    async function runCrag() {
      const query = document.getElementById('cragQuery').value.trim();
      if (!query) { setCragLog('Enter a query first.', 'warn'); return; }

      const btn = document.getElementById('btnCrag');
      btn.disabled = true;
      btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Running…';
      setCragLog('Running CRAG loop…', 'info');
      document.getElementById('cragResults').innerHTML = '';
      document.getElementById('cragPipelineTags').style.display = 'none';

      try {
        const cfg = buildConfig();
        const body = {
          query,
          crag:      cfg.crag,
          retrieval: cfg.retrieval,
          ingestion: cfg.ingestion,
          synthesize: document.getElementById('cragSynthesize').checked,
          synthesis_model: cfg.generation?.llm_model || 'gemini-2.0-flash',
        };

        const r = await fetch(`${getApiBase()}/tools/rag-configurator/crag/run`, {
          method: 'POST',
          headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const data = await r.json();
        if (!r.ok) throw new Error(typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail || data));

        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-arrows-rotate"></i> Run CRAG';

        // Decision badge
        const decisionColor = {
          pass:         'ret-pipeline-tag--vs',
          requery:      'ret-pipeline-tag--hyde',
          web_fallback: 'ret-pipeline-tag--rerank',
          fail:         '',
        }[data.decision] || '';
        const decisionIcon = {
          pass:         'fa-circle-check',
          requery:      'fa-rotate',
          web_fallback: 'fa-globe',
          fail:         'fa-circle-xmark',
        }[data.decision] || 'fa-circle-question';

        const tagsEl = document.getElementById('cragPipelineTags');
        tagsEl.style.display = 'block';
        tagsEl.innerHTML =
          `<span class="ret-pipeline-tag ${decisionColor}"><i class="fas ${decisionIcon}"></i> ${data.decision?.toUpperCase()}</span>` +
          `<span class="ret-pipeline-tag"><i class="fas fa-gauge-high"></i> Grade&nbsp;${(data.grade_score||0).toFixed(2)}</span>` +
          (data.iterations > 0 ? `<span class="ret-pipeline-tag ret-pipeline-tag--hyde"><i class="fas fa-rotate"></i> ${data.iterations} re-quer${data.iterations===1?'y':'ies'}</span>` : '') +
          (data.source === 'web' ? `<span class="ret-pipeline-tag ret-pipeline-tag--rerank"><i class="fas fa-globe"></i> Web fallback</span>` : '');

        if (!data.count) {
          setCragLog('No results returned. Check that the index is ONLINE and has been synced.', 'warn');
          return;
        }

        const gradeLabel = data.grade_score >= (cfg.crag.relevance_threshold || 0.6)
          ? '✓ Passed relevance threshold'
          : (data.decision === 'web_fallback' ? '⚠ Used web fallback' : '⚠ Best-effort result');
        const logType = data.decision === 'pass' ? 'success' : (data.decision === 'fail' ? 'error' : 'warn');
        setCragLog(
          `${gradeLabel}  |  Grade: ${(data.grade_score||0).toFixed(2)}  |  ${data.count} chunk${data.count!==1?'s':''}  |  ${data.iterations} iteration${data.iterations!==1?'s':''}`,
          logType,
        );

        const container = document.getElementById('cragResults');
        const answerHtml = _renderAnswer(data, query);
        const chunksHtml = data.results.map((chunk, i) => _cragCard(chunk, i)).join('');
        container.innerHTML = answerHtml +
          (data.trace?.length ? _renderCragTrace(data.trace) : '') +
          `<button class="ret-sources-toggle" id="cragSourcesToggle" onclick="toggleCragSources()">
            <i class="fas fa-chevron-down"></i>
            ${data.source === 'web' ? 'Web Results' : 'Source Chunks'} (${data.count})
          </button>
          <div class="ret-sources-body" id="cragSourcesBody">
            <div style="padding-top:8px;">${chunksHtml}</div>
          </div>`;

      } catch(err) {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-arrows-rotate"></i> Run CRAG';
        setCragLog('✗ ' + err.message, 'error');
      }
    }

    function _renderCragTrace(trace) {
      const rows = trace.map(t => {
        const iter = t.iteration !== undefined ? String(t.iteration) : '?';
        const score = t.grade_score != null ? parseFloat(t.grade_score).toFixed(2) : '—';
        const q = (t.query || (t.sub_queries || []).join(' | ') || '—')
          .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
        return `<tr><td style="padding:3px 8px;opacity:.7;font-size:11px;">${iter}</td>
          <td style="padding:3px 8px;font-size:11px;max-width:320px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${q}">${q}</td>
          <td style="padding:3px 8px;font-size:11px;">${score}</td></tr>`;
      }).join('');
      return `<details style="margin:8px 0;font-size:12px;">
        <summary style="cursor:pointer;color:#6b7280;">CRAG trace (${trace.length} step${trace.length!==1?'s':''})</summary>
        <table style="margin-top:6px;border-collapse:collapse;width:100%;">
          <thead><tr>
            <th style="text-align:left;padding:3px 8px;font-size:11px;opacity:.6;">Iter</th>
            <th style="text-align:left;padding:3px 8px;font-size:11px;opacity:.6;">Query</th>
            <th style="text-align:left;padding:3px 8px;font-size:11px;opacity:.6;">Grade</th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </details>`;
    }

    function _cragCard(chunk, index) {
      const vsScore    = chunk.score       != null ? parseFloat(chunk.score).toFixed(3)       : null;
      const gradeScore = chunk.grade_score != null ? parseFloat(chunk.grade_score).toFixed(2)  : null;
      const isWeb      = chunk.source_type === 'web';
      const rawContent = chunk.content || chunk.text || '';
      const content    = _cleanChunkText(rawContent)
                          .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
      const source     = isWeb
        ? (chunk.title || chunk.source || '—').slice(0,60)
        : (chunk.source || '—').split('/').pop().split('\\').pop();
      const page        = chunk.page != null ? `Page ${Math.round(chunk.page)}` : '';
      const chunkId     = chunk.chunk_id && !isWeb ? chunk.chunk_id.slice(0, 10) + '…' : '';
      const cardId      = `cragcard-${index}`;
      const metaParts   = [page, chunkId].filter(Boolean);
      return `
        <div class="ret-result-card" ${isWeb ? 'style="border-left:3px solid #3b82f6;"' : ''}>
          <div class="ret-result-header">
            <div class="ret-rank">${isWeb ? '<i class="fas fa-globe" style="font-size:11px;color:#3b82f6;"></i>' : index + 1}</div>
            <div class="ret-source-wrap">
              <div class="ret-source" title="${source}">${source}</div>
              ${isWeb && chunk.source ? `<div class="ret-meta"><a href="${chunk.source}" target="_blank" rel="noopener" style="color:#3b82f6;font-size:10px;">${chunk.source.slice(0,60)}</a></div>` : ''}
              ${!isWeb && metaParts.length ? `<div class="ret-meta">${metaParts.join('  ·  ')}</div>` : ''}
            </div>
            <div class="ret-score-wrap">
              ${vsScore    ? `<span class="ret-score ret-score--vs" title="VS similarity score">VS&nbsp;${vsScore}</span>` : ''}
              ${gradeScore ? `<span class="ret-score ret-score--rerank" title="CRAG grade score">Grade&nbsp;${gradeScore}</span>` : ''}
            </div>
          </div>
          <div class="ret-divider"></div>
          <div class="ret-content" id="${cardId}-content">${content}</div>
          <button class="ret-toggle-expand" onclick="toggleRetContent('${cardId}')">show more</button>
        </div>`;
    }

    function toggleCragSources() {
      const btn  = document.getElementById('cragSourcesToggle');
      const body = document.getElementById('cragSourcesBody');
      btn.classList.toggle('open');
      body.classList.toggle('open');
    }

    function setCragLog(msg, type) {
      const el = document.getElementById('cragLog');
      el.textContent = msg;
      el.className = 'ing-log ing-log--' + type;
      el.style.display = 'block';
    }

    // ── Generation test ──────────────────────────────────────────────────────
    async function runGeneration() {
      const query = document.getElementById('genQuery').value.trim();
      if (!query) { setGenLog('Enter a question first.', 'warn'); return; }

      const btn = document.getElementById('btnGenerate');
      btn.disabled = true;
      btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Generating…';
      setGenLog('Running generation pipeline…', 'info');
      document.getElementById('genResults').innerHTML = '';
      document.getElementById('genPipelineTags').style.display = 'none';

      try {
        const cfg = buildConfig();
        const body = {
          query,
          generation: cfg.generation,
          retrieval:  cfg.retrieval,
          ingestion:  cfg.ingestion,
        };

        const r = await fetch(`${getApiBase()}/tools/rag-configurator/generate`, {
          method: 'POST',
          headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const data = await r.json();
        if (!r.ok) throw new Error(typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail || data));

        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-wand-magic-sparkles"></i> Generate';

        const faith    = data.faithfulness || {};
        const faithScore = faith.score != null ? parseFloat(faith.score) : null;
        const faithPassed = faith.faithful !== false;
        const cits     = data.citations || {};
        const fabCount = (cits.fabricated || []).length;

        // Pipeline tags
        const tagsEl = document.getElementById('genPipelineTags');
        tagsEl.style.display = 'block';
        tagsEl.innerHTML =
          `<span class="ret-pipeline-tag ret-pipeline-tag--vs"><i class="fas fa-robot"></i> ${(data.provider||'llm').toUpperCase()} — ${data.model||''}</span>` +
          (faithScore != null ? `<span class="ret-pipeline-tag ${faithPassed ? 'ret-pipeline-tag--rerank' : ''}" style="${faithPassed?'':'background:#fef2f2;color:#dc2626;'}"><i class="fas fa-shield-halved"></i> Faithfulness&nbsp;${faithScore.toFixed(2)}</span>` : '') +
          (data.regenerations > 0 ? `<span class="ret-pipeline-tag ret-pipeline-tag--hyde"><i class="fas fa-rotate"></i> ${data.regenerations} regen${data.regenerations!==1?'s':''}</span>` : '') +
          (fabCount > 0 ? `<span class="ret-pipeline-tag" style="background:#fef9c3;color:#a16207;"><i class="fas fa-triangle-exclamation"></i> ${fabCount} fabricated citation${fabCount!==1?'s':''}</span>` : '');

        const logType = !faithPassed ? 'warn' : fabCount > 0 ? 'warn' : 'success';
        const faithLabel = faithScore != null
          ? `Faithfulness: ${faithScore.toFixed(2)}${faithPassed ? ' ✓' : ' ⚠ below threshold'}`
          : 'Faithfulness check skipped';
        setGenLog(
          `✓ Answer generated  |  ${faithLabel}  |  ${data.chunks_used} chunk${data.chunks_used!==1?'s':''} used` +
          (data.regenerations > 0 ? `  |  ${data.regenerations} regeneration${data.regenerations!==1?'s':''}` : ''),
          logType,
        );

        const container = document.getElementById('genResults');

        // Answer card
        let answerHtml = '';
        if (data.answer) {
          const escaped = data.answer.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
          answerHtml = `<div class="ret-answer-card">
            <div class="ret-answer-title"><i class="fas fa-circle-check"></i> Generated Answer</div>
            <div class="ret-answer-body">${escaped}</div>
          </div>`;
        }

        // Citations card
        let citHtml = '';
        if ((cits.valid||[]).length || fabCount > 0) {
          const validRows = (cits.valid||[]).map(id => {
            const src = (cits.resolution_map||{})[id] || '';
            return `<div style="font-size:12px;margin:2px 0;"><code style="background:#f3f4f6;padding:1px 4px;border-radius:3px;">${id}</code>${src ? ` → <span style="opacity:.7">${src.split('/').pop().split('\\\\').pop()}</span>` : ''}</div>`;
          }).join('');
          const fabRows = (cits.fabricated||[]).map(id =>
            `<div style="font-size:12px;margin:2px 0;color:#dc2626;"><code style="background:#fef2f2;padding:1px 4px;border-radius:3px;">${id}</code> ⚠ not found in index</div>`
          ).join('');
          citHtml = `<details style="margin:8px 0;font-size:12px;">
            <summary style="cursor:pointer;color:#6b7280;">Citations (${(cits.valid||[]).length} valid${fabCount?' / '+fabCount+' fabricated':''})</summary>
            <div style="margin-top:6px;">${validRows}${fabRows}</div>
          </details>`;
        }

        // Source chunks toggle
        const chunksHtml = (data.chunks||[]).map((chunk, i) => _retCard(chunk, i)).join('');
        container.innerHTML = answerHtml + citHtml +
          `<button class="ret-sources-toggle" id="genSourcesToggle" onclick="toggleGenSources()">
            <i class="fas fa-chevron-down"></i>
            Source Chunks (${data.chunks_used})
          </button>
          <div class="ret-sources-body" id="genSourcesBody">
            <div style="padding-top:8px;">${chunksHtml}</div>
          </div>`;

      } catch(err) {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-wand-magic-sparkles"></i> Generate';
        setGenLog('✗ ' + err.message, 'error');
      }
    }

    function toggleGenSources() {
      const btn  = document.getElementById('genSourcesToggle');
      const body = document.getElementById('genSourcesBody');
      btn.classList.toggle('open');
      body.classList.toggle('open');
    }

    function setGenLog(msg, type) {
      const el = document.getElementById('genLog');
      el.textContent = msg;
      el.className = 'ing-log ing-log--' + type;
      el.style.display = 'block';
    }

    // ── Observability ────────────────────────────────────────────────────────
    async function checkObsHealth() {
      setObsLog('Checking observability backends…', 'info');
      document.getElementById('obsResults').innerHTML = '';
      try {
        const r = await fetch(`${getApiBase()}/tools/rag-configurator/observability/health`, {
          headers: getAuthHeaders(),
        });
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || JSON.stringify(data));

        const backends = data.backends || {};
        let html = '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:8px;">';
        for (const [name, info] of Object.entries(backends)) {
          const ok = info.configured;
          html += `<div style="padding:8px 14px;border-radius:8px;font-size:13px;
            background:${ok?'#f0fdf4':'#fef2f2'};color:${ok?'#16a34a':'#dc2626'};
            border:1px solid ${ok?'#bbf7d0':'#fecaca'};">
            <i class="fas ${ok?'fa-circle-check':'fa-circle-xmark'}"></i>
            <strong>${name}</strong>${info.tracking_uri ? ' — ' + info.tracking_uri : ''}
            ${info.error ? '<div style="font-size:11px;opacity:.7;">' + info.error + '</div>' : ''}
            ${info.note  ? '<div style="font-size:11px;opacity:.7;">' + info.note  + '</div>' : ''}
          </div>`;
        }
        html += '</div>';
        document.getElementById('obsResults').innerHTML = html;
        setObsLog('Backend health check complete.', 'success');
      } catch(err) {
        setObsLog('✗ ' + err.message, 'error');
      }
    }

    async function runEvaluation() {
      const raw = document.getElementById('evalSamples').value.trim();
      if (!raw) { setObsLog('Paste at least one sample first.', 'warn'); return; }

      // Parse pipe-separated lines
      const samples = [];
      for (const line of raw.split('\n')) {
        const parts = line.split('|').map(s => s.trim()).filter(Boolean);
        if (parts.length < 3) continue;
        samples.push({
          question:     parts[0],
          answer:       parts[1],
          contexts:     [parts[2]],
          ground_truth: parts[3] || '',
        });
      }
      if (!samples.length) {
        setObsLog('No valid samples parsed. Format: question | answer | context (| ground_truth optional)', 'warn');
        return;
      }

      const btn = document.getElementById('btnEval');
      btn.disabled = true;
      btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Evaluating…';
      setObsLog(`Running Ragas on ${samples.length} sample${samples.length!==1?'s':''}…`, 'info');
      document.getElementById('obsResults').innerHTML = '';

      try {
        const cfg = buildConfig();
        const body = { samples, observability: cfg.observability };

        const r = await fetch(`${getApiBase()}/tools/rag-configurator/observability/evaluate`, {
          method: 'POST',
          headers: { ...getAuthHeaders(), 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const data = await r.json();
        if (!r.ok) throw new Error(typeof data.detail === 'string' ? data.detail : JSON.stringify(data.detail || data));

        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-chart-bar"></i> Run Ragas Evaluation';

        const scores  = data.scores || {};
        const flags   = data.flags  || {};
        const allOk   = Object.values(flags).every(Boolean);
        setObsLog(
          `✓ Evaluation complete — ${data.sample_count} sample${data.sample_count!==1?'s':''}  |  ${data.elapsed_s}s` +
          (data.mlflow?.run_id ? `  |  MLflow run: ${data.mlflow.run_id}` : ''),
          allOk ? 'success' : 'warn',
        );

        // Score cards
        const scoreNames = {
          faithfulness: 'Faithfulness', answer_relevancy: 'Answer Relevancy',
          context_recall: 'Context Recall', context_precision: 'Context Precision',
        };
        let html = '<div style="display:flex;flex-wrap:wrap;gap:10px;margin-top:10px;">';
        for (const [key, label] of Object.entries(scoreNames)) {
          if (!(key in scores)) continue;
          const val   = scores[key];
          const pct   = Math.round(val * 100);
          const color = val >= 0.7 ? '#16a34a' : val >= 0.5 ? '#a16207' : '#dc2626';
          const bg    = val >= 0.7 ? '#f0fdf4' : val >= 0.5 ? '#fefce8' : '#fef2f2';
          const bdr   = val >= 0.7 ? '#bbf7d0' : val >= 0.5 ? '#fef08a' : '#fecaca';
          html += `<div style="padding:12px 16px;border-radius:10px;background:${bg};border:1px solid ${bdr};min-width:140px;text-align:center;">
            <div style="font-size:22px;font-weight:700;color:${color};">${pct}%</div>
            <div style="font-size:12px;color:#6b7280;margin-top:2px;">${label}</div>
          </div>`;
        }
        html += '</div>';

        // Per-sample table
        if (data.per_sample?.length) {
          html += `<details style="margin-top:12px;font-size:12px;">
            <summary style="cursor:pointer;color:#6b7280;">Per-sample breakdown (${data.per_sample.length})</summary>
            <div style="overflow-x:auto;margin-top:8px;">
              <table style="border-collapse:collapse;width:100%;font-size:12px;">
                <thead><tr style="background:#f9fafb;">
                  <th style="padding:5px 8px;text-align:left;border:1px solid #e5e7eb;">Question</th>
                  ${Object.keys(scoreNames).filter(k => k in scores).map(k =>
                    `<th style="padding:5px 8px;border:1px solid #e5e7eb;">${scoreNames[k]}</th>`
                  ).join('')}
                </tr></thead>
                <tbody>
                  ${data.per_sample.map(row => `<tr>
                    <td style="padding:5px 8px;border:1px solid #e5e7eb;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${(row.question||'').replace(/"/g,'&quot;')}">${(row.question||'').slice(0,60)}</td>
                    ${Object.keys(scoreNames).filter(k => k in scores).map(k => {
                      const v = row[k];
                      const c = v != null ? (v >= 0.7 ? '#16a34a' : v >= 0.5 ? '#a16207' : '#dc2626') : '#9ca3af';
                      return `<td style="padding:5px 8px;border:1px solid #e5e7eb;text-align:center;color:${c};">${v != null ? (v*100).toFixed(0)+'%' : '—'}</td>`;
                    }).join('')}
                  </tr>`).join('')}
                </tbody>
              </table>
            </div>
          </details>`;
        }

        document.getElementById('obsResults').innerHTML = html;

      } catch(err) {
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-chart-bar"></i> Run Ragas Evaluation';
        setObsLog('✗ ' + err.message, 'error');
      }
    }

    function setObsLog(msg, type) {
      const el = document.getElementById('obsLog');
      el.textContent = msg;
      el.className = 'ing-log ing-log--' + type;
      el.style.display = 'block';
    }

    // ── Index status ─────────────────────────────────────────────────────────
    let _idxPollTimer = null;

    const _IDX_PROGRESS = {
      'PROVISIONING_PIPELINE_RESOURCES': { pct: 18, stripes: true },
      'INITIALIZING_EMBEDDING_ACCESS':   { pct: 38, stripes: true },
      'OFFLINE':                         { pct: 5,  stripes: false },
      'SYNCING':                         { pct: 72, stripes: true },
      'ONLINE_NO_PENDING_UPDATE':        { pct: 100, stripes: false, done: true },
      'ONLINE':                          { pct: 100, stripes: false, done: true },
    };

    function _applyProgress(state, isFailed, isReady) {
      const wrap = document.getElementById('idxProgressWrap');
      const bar  = document.getElementById('idxProgressBar');
      const hint = document.getElementById('idxPollHint');
      if (!wrap) return;

      if (state === 'NOT_FOUND' || state === 'UNKNOWN') {
        wrap.classList.remove('visible');
        hint.style.display = 'none';
        return;
      }

      wrap.classList.add('visible');
      bar.classList.remove('stripes', 'online', 'error');

      if (isFailed) {
        bar.style.width = '100%';
        bar.classList.add('error');
        hint.style.display = 'none';
        return;
      }

      const cfg = _IDX_PROGRESS[state];
      const pct = cfg ? cfg.pct : 50;
      bar.style.width = pct + '%';

      if (isReady) {
        bar.classList.add('online');
        hint.style.display = 'none';
      } else {
        if (cfg && cfg.stripes) bar.classList.add('stripes');
        hint.textContent = 'Auto-refreshing every 10 s until index is ONLINE…';
        hint.style.display = 'block';
      }
    }

    async function refreshIndexStatus(silent) {
      const catalog = (document.getElementById('dbCatalog').value  || 'workspace').trim();
      const schema  = (document.getElementById('dbSchema').value   || 'agentic_rag').trim();
      const index   = (document.getElementById('vsIndex').value    || 'rag_index').trim();
      const card    = document.getElementById('indexStatusCard');
      const badge   = document.getElementById('indexStatusBadge');
      const detail  = document.getElementById('indexStatusDetail');
      const msg     = document.getElementById('indexStatusMsg');
      card.style.display = 'block';
      if (!silent) {
        badge.textContent = '…'; badge.className = 'idx-badge idx-badge--loading';
        detail.textContent = ''; msg.textContent = '';
      }
      try {
        const qs = new URLSearchParams({ catalog, schema, index });
        const r = await fetch(
          `${getApiBase()}/tools/rag-configurator/index-status?${qs}`,
          { headers: getAuthHeaders() }
        );
        const data = await r.json();
        const state   = data.state || 'UNKNOWN';
        const isFailed = state.startsWith('FAIL') || state.startsWith('OFFLINE_FAIL');
        const isReady  = !!data.ready;

        badge.textContent = state;
        badge.className = 'idx-badge ' + (
          isReady    ? 'idx-badge--online'  :
          state === 'NOT_FOUND' ? 'idx-badge--missing' :
          isFailed   ? 'idx-badge--error'   : 'idx-badge--pending'
        );
        const rows = data.row_count != null
          ? `  ·  ${Number(data.row_count).toLocaleString()} rows indexed` : '';
        detail.textContent = `${catalog}.${schema}.${index}${rows}`;
        if (data.message) msg.textContent = data.message;

        _applyProgress(state, isFailed, isReady);

        // Auto-poll when still provisioning/syncing; stop when done or failed
        if (_idxPollTimer) clearTimeout(_idxPollTimer);
        if (!isReady && !isFailed && state !== 'NOT_FOUND') {
          _idxPollTimer = setTimeout(() => refreshIndexStatus(true), 10000);
        }
      } catch(err) {
        badge.textContent = 'ERROR'; badge.className = 'idx-badge idx-badge--error';
        detail.textContent = err.message;
        _applyProgress('ERROR', true, false);
      }
    }

    // Initialise retrieval flag auto-intelligence once DOM is ready
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', _initRetFlagListeners);
    } else {
      _initRetFlagListeners();
    }
