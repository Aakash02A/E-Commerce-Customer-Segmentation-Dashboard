// Customer Segmentation page: analysis switcher, KPI cards, distribution chart,
// segment cards with click-to-filter, and export links.
(function () {
  Charts.defaults();
  const params = new URLSearchParams(location.search);
  const select = document.getElementById("analysis-select");
  const loadingEl = document.getElementById("seg-loading");
  const emptyEl = document.getElementById("seg-empty");
  const errorEl = document.getElementById("seg-error");
  const contentEl = document.getElementById("seg-content");
  const bannerMeta = document.getElementById("seg-banner-meta");
  const kpiGrid = document.getElementById("seg-kpis");
  const cardsGrid = document.getElementById("seg-cards");

  function showLoading(show) { loadingEl.classList.toggle("d-none", !show); }
  function showError(m) {
    errorEl.textContent = m;
    errorEl.classList.remove("d-none");
    contentEl.classList.add("d-none");
    emptyEl.classList.add("d-none");
  }
  function clearError() { errorEl.classList.add("d-none"); }
  function setEmpty(show) {
    emptyEl.classList.toggle("d-none", !show);
    contentEl.classList.toggle("d-none", show);
  }

  // --- Analysis switcher ---
  async function loadAnalyses() {
    showLoading(true);
    let analyses;
    try { analyses = (await api("/api/analyses")).data.analyses; }
    catch (e) { showLoading(false); showError(e.message); return; }
    showLoading(false);
    if (!analyses.length) { select.innerHTML = ""; setEmpty(true); return; }
    const wanted = params.get("analysis") || Session.getAnalysisId();
    const active = analyses.find((a) => a.analysis_id === wanted) ? wanted : analyses[0].analysis_id;
    select.innerHTML = analyses.map((a) =>
      `<option value="${a.analysis_id}"${a.analysis_id === active ? " selected" : ""}>${escapeHtml(a.source_name || "dataset")} · ${a.customers_analyzed} customers</option>`
    ).join("");
    setEmpty(false);
    await load(active, analyses);
  }

  // --- Load dashboard data for one analysis ---
  async function load(analysisId, analyses) {
    clearError();
    showLoading(true);
    Session.setAnalysisId(analysisId);

    // Find this analysis's metadata for the banner.
    const meta = (analyses || []).find((a) => a.analysis_id === analysisId) || {};

    let d;
    try { d = (await api(`/api/analyses/${analysisId}/dashboard`)).data; }
    catch (e) { showLoading(false); showError(e.message); return; }
    showLoading(false);

    // --- Banner ---
    const overview = d.overview || {};
    bannerMeta.innerHTML = [
      `<div>Dataset <span>${escapeHtml(meta.source_name || "-")}</span></div>`,
      `<div>Reference Date <span>${escapeHtml(overview.date_range && overview.date_range.last || meta.reference_date || "-")}</span></div>`,
      `<div>Customers <span>${formatNumber(overview.total_customers)}</span></div>`,
    ].join("");

    // --- Export links (use existing /api/analyses/<id>/export endpoint) ---
    document.getElementById("btn-export-xlsx").href =
      `/api/analyses/${analysisId}/export?format=xlsx&dataset=full`;
    document.getElementById("btn-export-csv").href =
      `/api/analyses/${analysisId}/export?format=csv&dataset=customers`;

    // --- KPI cards ---
    const sb = d.segment_breakdown;
    const segments = sb.available ? sb.segments : [];
    const largest = segments.length ? segments[0] : null;

    const kpis = [
      { icon: "bi-people", label: "Total Customers", value: formatNumber(overview.total_customers), sub: "analyzed in this dataset" },
      { icon: "bi-diagram-3", label: "Segments", value: formatNumber(segments.length), sub: "distinct groups identified" },
      { icon: "bi-trophy", label: "Largest Segment", value: largest ? largest.segment : "-", sub: largest ? `${formatNumber(largest.count)} customers (${largest.percentage}%)` : "No segments yet" },
      { icon: "bi-currency-dollar", label: "Revenue", value: overview.total_revenue != null ? formatCurrency(overview.total_revenue) : "-", sub: "from valid transactions" },
    ];

    kpiGrid.innerHTML = kpis.map((k) => `
      <div class="card kpi-card" role="listitem">
        <div class="kpi-top">
          <div class="kpi-label">${escapeHtml(k.label)}</div>
          <div class="kpi-icon"><i class="bi ${k.icon}"></i></div>
        </div>
        <div class="kpi-value">${escapeHtml(String(k.value))}</div>
        <div class="kpi-sub">${escapeHtml(k.sub)}</div>
      </div>`).join("");

    // --- Distribution chart (donut) + legend ---
    const legendEl = document.getElementById("seg-legend");
    if (sb.available && segments.length) {
      Charts.doughnut("seg-chart", "segpage",
        segments.map((s) => s.segment),
        segments.map((s) => s.count));
      legendEl.innerHTML = segments.map((s) => {
        const href = `/customers?analysis=${encodeURIComponent(analysisId)}&segment=${encodeURIComponent(s.segment)}`;
        return `<a class="seg-legend-row" href="${href}" role="listitem">
          <span class="swatch" style="background:${Charts.segmentColor(s.segment)}"></span>
          <span class="seg-name">${escapeHtml(s.segment)}</span>
          <span class="seg-count">${formatNumber(s.count)}</span>
          <span class="seg-pct">${s.percentage}%</span>
        </a>`;
      }).join("");
    } else {
      Charts.destroy("segpage");
      legendEl.innerHTML = '<p class="text-secondary mb-0">No segments available for this analysis yet.</p>';
    }

    // --- Segment cards ---
    if (!sb.available) {
      cardsGrid.innerHTML = '<div class="col-12 text-secondary p-4">No segments available for this analysis. Run segmentation first.</div>';
      return;
    }
    cardsGrid.innerHTML = segments.map((s) => {
      const href = `/customers?analysis=${encodeURIComponent(analysisId)}&segment=${encodeURIComponent(s.segment)}`;
      return `<a class="card seg-card h-100 text-decoration-none" data-segment="${escapeHtml(s.segment)}" href="${href}" role="listitem">
        <div class="card-body">
          <div class="mb-2"><span class="seg-badge" data-segment="${escapeHtml(s.segment)}">${escapeHtml(s.segment)}</span></div>
          <div class="kpi-value" style="font-size:1.6rem">${formatNumber(s.count)}</div>
          <div class="kpi-sub">${s.percentage}% of customers</div>
          <p class="small text-secondary mt-2 mb-0">${escapeHtml(s.description)}</p>
        </div>
      </a>`;
    }).join("");
  }

  select.addEventListener("change", () => load(select.value));
  loadAnalyses();
})();
