// Data quality validation page: run validation, render the report.
(function () {
  const uploadId = Session.getUploadId();
  const content = document.getElementById("val-content");
  const empty = document.getElementById("val-empty");
  const runBtn = document.getElementById("run-validation");
  const reportWrap = document.getElementById("report-wrap");
  const cardsEl = document.getElementById("report-cards");
  const statusEl = document.getElementById("report-status");
  const checksEl = document.getElementById("report-checks");
  const proceedBtn = document.getElementById("continue-analysis");
  const analysisNote = document.getElementById("analysis-note");

  if (!uploadId) {
    empty.classList.remove("d-none");
    return;
  }
  content.classList.remove("d-none");

  // Load any existing report first (e.g. returning to this page).
  (async function () {
    try {
      const res = await api(`/api/uploads/${uploadId}/validate`);
      if (res.data.report) renderReport(res.data.report);
    } catch (e) { /* no report yet */ }
  })();

  runBtn.addEventListener("click", async () => {
    runBtn.disabled = true;
    runBtn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Validating…';
    const rules = {
      cancelled_policy: document.getElementById("rule-cancelled").value,
      negative_quantity_policy: document.getElementById("rule-negqty").value,
      duplicate_policy: document.getElementById("rule-dup").value,
    };
    try {
      const res = await api(`/api/uploads/${uploadId}/validate`, {
        method: "POST", body: JSON.stringify({ rules }),
      });
      renderReport(res.data.report);
      if (res.data.report.passed) Notifications.success(res.message);
      else Notifications.warning(res.message);
    } catch (e) {
      // 422 still carries a report body (success=true but not 2xx)
      const rep = e.payload && e.payload.data && e.payload.data.report;
      if (rep) renderReport(rep);
      else Notifications.error(e.message);
    } finally {
      runBtn.disabled = false;
      runBtn.innerHTML = '<i class="bi bi-play-circle"></i> Re-run Validation';
    }
  });

  function card(label, value, color) {
    return `<div class="col-6 col-lg-3"><div class="card h-100"><div class="card-body text-center">
      <div class="kpi-value" style="color:${color}">${value}</div>
      <div class="kpi-label">${label}</div></div></div></div>`;
  }

  function renderReport(report) {
    reportWrap.classList.remove("d-none");
    const s = report.summary;
    cardsEl.innerHTML =
      card("Total Rows", formatNumber(s.total_rows), "#0f172a") +
      card("Valid Rows", formatNumber(s.valid_rows), "#16a34a") +
      card("Excluded / Invalid", formatNumber(s.invalid_rows), "#dc2626") +
      card("Distinct Customers", formatNumber(s.distinct_customers), "#2563eb");

    if (report.passed) {
      statusEl.className = "alert alert-success";
      statusEl.innerHTML = `<i class="bi bi-check-circle-fill"></i> <strong>Validation passed.</strong> ${formatNumber(s.valid_rows)} eligible rows are ready for analysis.`;
      proceedBtn.disabled = false;
      analysisNote.textContent = "RFM segmentation (PySpark) runs in the next phase (Phase 6–7) and will consume these eligible rows.";
    } else {
      statusEl.className = "alert alert-danger";
      const errs = (report.blocking_errors || []).map((e) => e.message || e).join(" ");
      statusEl.innerHTML = `<i class="bi bi-x-octagon-fill"></i> <strong>Analysis blocked.</strong> ${escapeHtml(errs)}`;
      proceedBtn.disabled = true;
      analysisNote.textContent = "";
    }

    const rows = [];
    (report.blocking_errors || []).forEach((e) =>
      rows.push(rowHtml("blocking", "Required", e.message, "-")));
    (report.excluded || []).forEach((x) =>
      rows.push(rowHtml("danger", "Excluded", x.message, x.count)));
    (report.warnings || []).forEach((w) =>
      rows.push(rowHtml("warning", "Warning", w.message, w.count)));
    checksEl.innerHTML = rows.join("") ||
      '<tr><td colspan="3" class="text-secondary">No issues detected.</td></tr>';
  }

  function rowHtml(cls, tag, msg, count) {
    const badge = { blocking: "text-bg-danger", danger: "text-bg-danger",
      warning: "text-bg-warning" }[cls] || "text-bg-secondary";
    return `<tr><td><span class="badge ${badge}">${tag}</span></td>
      <td>${escapeHtml(msg)}</td><td class="text-end">${count}</td></tr>`;
  }

  // Run the full analysis pipeline (aggregate -> RFM -> segment) for this upload,
  // then remember the analysis and open the dashboard.
  proceedBtn.addEventListener("click", async () => {
    proceedBtn.disabled = true;
    proceedBtn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Processing…';
    try {
      const agg = await api(`/api/uploads/${uploadId}/aggregate`, { method: "POST", body: "{}" });
      const aid = agg.data.analysis_id;
      await api(`/api/analyses/${aid}/rfm`, { method: "POST", body: "{}" });
      await api(`/api/analyses/${aid}/segment`, { method: "POST", body: "{}" });
      Session.setAnalysisId(aid);
      Notifications.success("Analysis complete. Opening dashboard…");
      setTimeout(() => { window.location.href = "/dashboard"; }, 500);
    } catch (e) {
      Notifications.error(e.message);
      proceedBtn.disabled = false;
      proceedBtn.innerHTML = 'Proceed to Analysis <i class="bi bi-arrow-right"></i>';
    }
  });
})();
