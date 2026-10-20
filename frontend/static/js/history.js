// Dataset-history table (Phase 11 Task 2: extracted from the inline block in
// analysis_history.html so the strict CSP script-src ('self' + CDN, no
// 'unsafe-inline') holds on every page). Relies on the api()/escapeHtml()
// globals from main.js, which load before this file (see base.html).
(function loadHistory() {
  const STATUS_BADGE = {
    uploaded: "secondary", mapping_required: "info", mapped: "info",
    validation_failed: "danger", ready_for_analysis: "success", failed: "danger",
  };
  (async function () {
    const tbody = document.getElementById("history-rows");
    try {
      const res = await api("/api/uploads");
      const items = res.data.uploads;
      if (!items.length) {
        tbody.innerHTML = '<tr><td colspan="5" class="text-secondary p-4 text-center">No datasets uploaded yet.</td></tr>';
        return;
      }
      tbody.innerHTML = items.map((u) => {
        const badge = STATUS_BADGE[u.status] || "secondary";
        return `<tr>
          <td class="fw-semibold">${escapeHtml(u.original_filename)}</td>
          <td class="text-uppercase"><small>${escapeHtml(u.extension)}</small></td>
          <td><span class="badge text-bg-${badge}">${escapeHtml(u.status.replace(/_/g, " "))}</span></td>
          <td><small class="text-secondary">${escapeHtml(u.created_at)}</small></td>
          <td class="text-end"><a class="btn btn-sm btn-outline-primary" href="/upload" title="Resume this dataset">Open</a></td>
        </tr>`;
      }).join("");
    } catch (e) {
      tbody.innerHTML = `<tr><td colspan="5" class="text-danger p-4 text-center">${escapeHtml(e.message)}</td></tr>`;
    }
  })();
})();
