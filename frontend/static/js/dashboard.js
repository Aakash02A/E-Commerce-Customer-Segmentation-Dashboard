// Analytics dashboard: switch between analyses and render persisted results.
(function () {
  Charts.defaults();

  const select = document.getElementById("analysis-select");
  const refreshBtn = document.getElementById("refresh");
  const emptyEl = document.getElementById("dash-empty");
  const errorEl = document.getElementById("dash-error");
  const contentEl = document.getElementById("dash-content");

  let currentId = null;

  function showError(msg) {
    errorEl.textContent = msg;
    errorEl.classList.remove("d-none");
  }
  function clearError() { errorEl.classList.add("d-none"); }

  function setEmpty(show) {
    emptyEl.classList.toggle("d-none", !show);
    contentEl.classList.toggle("d-none", show);
  }

  // Load the list of analyses into the switcher and pick the active one.
  async function loadAnalyses() {
    clearError();
    let analyses;
    try {
      const res = await api("/api/analyses");
      analyses = res.data.analyses;
    } catch (e) {
      showError(e.message);
      return;
    }
    if (!analyses.length) {
      select.innerHTML = "";
      setEmpty(true);
      return;
    }
    const stored = Session.getAnalysisId();
    const active = analyses.find((a) => a.analysis_id === stored) ? stored : analyses[0].analysis_id;
    select.innerHTML = analyses
      .map((a) => `<option value="${a.analysis_id}" ${a.analysis_id === active ? "selected" : ""}>` +
        `${escapeHtml(a.source_name || "dataset")} · ${escapeHtml(a.status)} · ` +
        `${a.customers_analyzed} customers · ${escapeHtml((a.created_at || "").slice(0, 16))}</option>`)
      .join("");
    setEmpty(false);
    await loadDashboard(active);
  }

  function kpiCard(label, value, icon, color) {
    return `<div class="card kpi-card">
      <div class="kpi-top">
        <div class="kpi-label">${label}</div>
        <div class="kpi-icon" style="background:${color}1a;color:${color}"><i class="bi ${icon}"></i></div>
      </div>
      <div class="kpi-value">${value}</div></div>`;
  }

  // Show "-" for genuinely unavailable (null) values, distinct from a real 0.
  function num(v, digits = 0) { return v == null ? "-" : formatNumber(v, digits); }
  function money(v) { return v == null ? "-" : formatCurrency(v); }

  function renderAnalysis(info) {
    document.getElementById("info-id").innerHTML =
      `<span class="sample-values">${escapeHtml(info.analysis_id)}</span>`;
    document.getElementById("info-source").textContent = info.source_name || "-";
    document.getElementById("info-source").title = info.source_name || "";
    const pill = { completed: "success", running: "info", pending: "neutral", failed: "danger" }[info.status] || "neutral";
    document.getElementById("info-status").innerHTML =
      `<span class="pill pill-${pill}">${escapeHtml(info.status.replace(/_/g, " "))}</span>`;
    document.getElementById("info-ref").textContent = info.reference_date || "-";
    document.getElementById("link-segments").href = "/segments?analysis=" + encodeURIComponent(info.analysis_id);
    document.getElementById("link-customers").href = "/customers?analysis=" + encodeURIComponent(info.analysis_id);
    renderExport(info);
  }

  // Point the export buttons at the current analysis; disable until completed.
  function renderExport(info) {
    const controls = document.getElementById("export-controls");
    const disabledNote = document.getElementById("export-disabled");
    const enabled = info.status === "completed";
    controls.querySelectorAll("a[data-export]").forEach((a) => {
      if (enabled) {
        const q = new URLSearchParams({ format: a.dataset.export, dataset: a.dataset.dataset });
        a.href = `/api/analyses/${encodeURIComponent(info.analysis_id)}/export?${q}`;
        a.classList.remove("disabled");
        a.removeAttribute("aria-disabled");
      } else {
        a.href = "#";
        a.classList.add("disabled");
        a.setAttribute("aria-disabled", "true");
      }
    });
    disabledNote.classList.toggle("d-none", enabled);
  }

  function renderStageNote(stages) {
    const el = document.getElementById("stage-note");
    if (stages.aggregated && stages.rfm_scored && stages.segmented) { el.innerHTML = ""; return; }
    let msg;
    if (!stages.aggregated) msg = "This analysis has not completed aggregation, so no metrics are available yet.";
    else if (!stages.rfm_scored) msg = "Aggregation is complete, but RFM scoring and segmentation have not run yet — overview metrics are shown below.";
    else msg = "RFM scores are ready, but segmentation has not run yet.";
    el.innerHTML = `<div class="alert alert-warning"><i class="bi bi-exclamation-triangle"></i> ${escapeHtml(msg)}</div>`;
  }

  function renderOverview(ov) {
    document.getElementById("kpi-row").innerHTML =
      kpiCard("Eligible Customers", num(ov.total_customers), "bi-people", "#2563eb") +
      kpiCard("Valid Orders", num(ov.total_orders), "bi-receipt", "#0891b2") +
      kpiCard("Total Revenue", money(ov.total_revenue), "bi-cash-stack", "#16a34a") +
      kpiCard("Units Purchased", num(ov.total_quantity), "bi-box-seam", "#7c3aed") +
      kpiCard("Avg Order Value", money(ov.average_order_value), "bi-graph-up-arrow", "#f59e0b") +
      kpiCard("Date Range",
        ov.date_range.first ? `<span class="h6" style="font-size:1rem">${escapeHtml(ov.date_range.first)} → ${escapeHtml(ov.date_range.last)}</span>` : "-",
        "bi-calendar3", "#db2777");
  }

  function toCountMap(dist) {
    const m = {};
    (dist || []).forEach((d) => { m[d.score] = d.count; });
    return m;
  }

  function renderSegments(sb) {
    const tbody = document.getElementById("seg-table");
    const chartCanvas = document.getElementById("seg-chart");
    const chartEmpty = document.getElementById("seg-chart-empty");
    if (!sb.available) {
      Charts.destroy("seg");
      chartCanvas.classList.add("d-none");
      chartEmpty.classList.remove("d-none");
      tbody.innerHTML = '<tr><td colspan="4" class="text-secondary text-center py-3">No segments yet.</td></tr>';
      return;
    }
    chartCanvas.classList.remove("d-none");
    chartEmpty.classList.add("d-none");
    Charts.doughnut("seg-chart", "seg", sb.segments.map((s) => s.segment), sb.segments.map((s) => s.count));
    tbody.innerHTML = sb.segments.map((s) =>
      `<tr><td><span class="seg-badge" data-segment="${escapeHtml(s.segment)}">${escapeHtml(s.segment)}</span></td>
        <td class="text-end fw-semibold">${formatNumber(s.count)}</td>
        <td class="text-end">${s.percentage}%</td>
        <td><small class="text-secondary">${escapeHtml(s.description)}</small></td></tr>`).join("");
  }

  const SUMMARY_LABELS = {
    recency_days: ["Avg Recency (days)", "bi-clock-history", "#2563eb", 0],
    frequency: ["Avg Orders / Customer", "bi-bag", "#0891b2", 1],
    monetary: ["Avg Spend / Customer", "bi-cash", "#16a34a", 2],
    total_quantity: ["Avg Units / Customer", "bi-box", "#7c3aed", 1],
  };

  function renderRfm(rfm) {
    const available = rfm.scores_available;
    const chartsShown = ["recency", "frequency", "monetary"];
    document.getElementById("rfm-unavailable").classList.toggle("d-none", available);
    chartsShown.forEach((k) => {
      const canvas = document.getElementById("rfm-" + k);
      canvas.closest(".card").style.opacity = available ? "1" : "0.35";
      if (available) {
        Charts.scoreBar("rfm-" + k, "rfm-" + k, toCountMap(rfm.distributions[k]));
      } else {
        Charts.destroy("rfm-" + k);
      }
    });

    // Summary cards (raw recency/frequency/monetary/quantity stats).
    const el = document.getElementById("rfm-summary");
    el.innerHTML = Object.keys(SUMMARY_LABELS).map((key) => {
      const [label, icon, color, digits] = SUMMARY_LABELS[key];
      const s = rfm.summaries[key] || {};
      const value = s.available ? num(s.avg, digits) : "-";
      const sub = s.available ? `range ${num(s.min, digits)}–${num(s.max, digits)}` : "not available";
      return `<div class="card kpi-card">
        <div class="kpi-top">
          <div class="kpi-label">${label}</div>
          <div class="kpi-icon" style="background:${color}1a;color:${color}"><i class="bi ${icon}"></i></div>
        </div>
        <div class="kpi-value">${value}</div>
        <div class="kpi-sub">${escapeHtml(sub)}</div></div>`;
    }).join("");
  }

  async function loadDashboard(analysisId) {
    currentId = analysisId;
    Session.setAnalysisId(analysisId);
    clearError();
    try {
      const res = await api(`/api/analyses/${analysisId}/dashboard`);
      const d = res.data;
      renderAnalysis(d.analysis);
      renderStageNote(d.stages);
      renderOverview(d.overview);
      renderSegments(d.segment_breakdown);
      renderRfm(d.rfm);
    } catch (e) {
      showError("Could not load dashboard: " + e.message);
    }
  }

  select.addEventListener("change", () => loadDashboard(select.value));
  refreshBtn.addEventListener("click", loadAnalyses);

  loadAnalyses();
})();
