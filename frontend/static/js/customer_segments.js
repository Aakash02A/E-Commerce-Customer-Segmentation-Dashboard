// Customer segments: distribution chart + segment cards (link to filtered customers).
(function () {
  Charts.defaults();
  const params = new URLSearchParams(location.search);
  const select = document.getElementById("analysis-select");
  const emptyEl = document.getElementById("seg-empty");
  const errorEl = document.getElementById("seg-error");
  const contentEl = document.getElementById("seg-content");

  function showError(m) { errorEl.textContent = m; errorEl.classList.remove("d-none"); }
  function setEmpty(show) { emptyEl.classList.toggle("d-none", !show); contentEl.classList.toggle("d-none", show); }

  async function loadAnalyses() {
    let analyses;
    try { analyses = (await api("/api/analyses")).data.analyses; }
    catch (e) { showError(e.message); return; }
    if (!analyses.length) { select.innerHTML = ""; setEmpty(true); return; }
    const wanted = params.get("analysis") || Session.getAnalysisId();
    const active = analyses.find((a) => a.analysis_id === wanted) ? wanted : analyses[0].analysis_id;
    select.innerHTML = analyses.map((a) =>
      `<option value="${a.analysis_id}" ${a.analysis_id === active ? "selected" : ""}>${escapeHtml(a.source_name || "dataset")} · ${a.customers_analyzed} customers</option>`).join("");
    setEmpty(false);
    await load(active);
  }

  async function load(analysisId) {
    Session.setAnalysisId(analysisId);
    let d;
    try { d = (await api(`/api/analyses/${analysisId}/dashboard`)).data; }
    catch (e) { showError(e.message); return; }

    const sb = d.segment_breakdown;
    if (!sb.available) {
      Charts.destroy("segpage");
      document.getElementById("seg-cards").innerHTML = "";
      document.getElementById("seg-summary").innerHTML =
        '<span class="text-secondary">No segments yet for this analysis.</span>';
      return;
    }
    Charts.doughnut("seg-chart", "segpage", sb.segments.map((s) => s.segment), sb.segments.map((s) => s.count));
    document.getElementById("seg-summary").innerHTML =
      `<div class="kpi-value">${formatNumber(sb.segmented_customers)}</div>
       <div class="kpi-label">Segmented customers across ${sb.segments.length} segments</div>`;

    document.getElementById("seg-cards").innerHTML = sb.segments.map((s, i) => {
      const color = Charts.PALETTE[i % Charts.PALETTE.length];
      const href = `/customers?analysis=${encodeURIComponent(analysisId)}&segment=${encodeURIComponent(s.segment)}`;
      return `<div class="col-6 col-lg-3"><a class="text-decoration-none" href="${href}">
        <div class="card h-100 seg-card" style="border-left:4px solid ${color}"><div class="card-body">
        <div class="fw-bold">${escapeHtml(s.segment)}</div>
        <div class="kpi-value" style="font-size:1.5rem;color:${color}">${formatNumber(s.count)}</div>
        <div class="text-secondary small">${s.percentage}% of customers</div>
        <div class="small text-secondary mt-2">${escapeHtml(s.description)}</div>
        </div></div></a></div>`;
    }).join("");
  }

  select.addEventListener("change", () => load(select.value));
  loadAnalyses();
})();
