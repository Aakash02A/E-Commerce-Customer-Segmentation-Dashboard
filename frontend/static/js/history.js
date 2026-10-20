// Analysis History page: searchable/filterable/sortable library of analyses from
// /api/analyses. Column headers sort client-side; row actions link to the run.
(function () {
  const loadingEl = document.getElementById("history-loading");
  const errorEl = document.getElementById("history-error");
  const emptyEl = document.getElementById("history-empty");
  const contentEl = document.getElementById("history-content");
  const metaEl = document.getElementById("history-meta");
  const countEl = document.getElementById("history-count");
  const rowsEl = document.getElementById("history-rows");
  const searchInput = document.getElementById("history-search");
  const statusFilter = document.getElementById("history-status");
  const resetBtn = document.getElementById("history-reset");
  const headers = Array.from(document.querySelectorAll("th.sortable"));

  let allAnalyses = [];
  const state = { sort: "created_at", dir: "desc" };

  const PILL = {
    completed: "success", running: "info", pending: "neutral",
    failed: "danger", segmented: "success", aggregated: "info",
  };

  function showLoading(show) { loadingEl.classList.toggle("d-none", !show); }
  function showError(m) {
    errorEl.textContent = m;
    errorEl.classList.remove("d-none");
    contentEl.classList.add("d-none");
    emptyEl.classList.add("d-none");
  }
  function setEmpty(show) {
    emptyEl.classList.toggle("d-none", !show);
    contentEl.classList.toggle("d-none", show);
  }

  async function load() {
    showLoading(true);
    try {
      allAnalyses = (await api("/api/analyses")).data.analyses;
    } catch (e) { showLoading(false); showError(e.message); return; }
    showLoading(false);

    if (!allAnalyses.length) { setEmpty(true); return; }
    setEmpty(false);

    const completed = allAnalyses.filter((a) => a.status === "completed").length;
    const total = allAnalyses.length;
    const dates = allAnalyses.map((a) => a.created_at).filter(Boolean).sort();
    metaEl.textContent = `${total} analys${total === 1 ? "is" : "es"} (${completed} completed) · Most recent: ${dates[dates.length - 1] || "N/A"}`;

    render();
  }

  function compare(a, b) {
    const col = state.sort;
    let av = a[col], bv = b[col];
    if (col === "customers_analyzed") { av = av || 0; bv = bv || 0; }
    else { av = (av || "").toString().toLowerCase(); bv = (bv || "").toString().toLowerCase(); }
    if (av < bv) return state.dir === "asc" ? -1 : 1;
    if (av > bv) return state.dir === "asc" ? 1 : -1;
    return 0;
  }

  function updateSortIndicators() {
    headers.forEach((th) => {
      const ind = th.querySelector(".sort-ind");
      if (ind) ind.remove();
      if (th.dataset.col === state.sort) {
        const span = document.createElement("span");
        span.className = "sort-ind";
        span.textContent = state.dir === "asc" ? " ▲" : " ▼";
        th.appendChild(span);
        th.setAttribute("aria-sort", state.dir === "asc" ? "ascending" : "descending");
      } else {
        th.setAttribute("aria-sort", "none");
      }
      th.setAttribute("tabindex", "0");
      th.setAttribute("role", "columnheader");
    });
  }

  function render() {
    const q = searchInput.value.trim().toLowerCase();
    const status = statusFilter.value;

    let items = allAnalyses.slice();
    if (status) items = items.filter((a) => a.status === status);
    if (q) items = items.filter((a) =>
      (a.source_name || "").toLowerCase().includes(q) ||
      (a.analysis_id || "").toLowerCase().includes(q));
    items.sort(compare);

    countEl.textContent = `Showing ${items.length} of ${allAnalyses.length} analyses`;
    updateSortIndicators();

    if (!items.length) {
      rowsEl.innerHTML = '<tr><td colspan="6" class="text-secondary p-4 text-center">No analyses match these filters.</td></tr>';
      return;
    }

    rowsEl.innerHTML = items.map((a) => {
      const pill = PILL[a.status] || "neutral";
      const dashboardHref = `/dashboard?analysis=${encodeURIComponent(a.analysis_id)}`;
      const segmentsHref = `/segments?analysis=${encodeURIComponent(a.analysis_id)}`;
      return `<tr>
        <td class="fw-semibold">${escapeHtml(a.source_name || "dataset")}</td>
        <td><small>${escapeHtml(a.reference_date || "-")}</small></td>
        <td class="text-end">${formatNumber(a.customers_analyzed)}</td>
        <td><span class="pill pill-${pill}">${escapeHtml((a.status || "").replace(/_/g, " "))}</span></td>
        <td><small class="text-secondary">${escapeHtml((a.created_at || "").slice(0, 16))}</small></td>
        <td class="history-row-actions">
          <a class="btn btn-sm btn-outline-primary me-1" href="${dashboardHref}"><i class="bi bi-speedometer2"></i> Dashboard</a>
          <a class="btn btn-sm btn-outline-secondary" href="${segmentsHref}"><i class="bi bi-people"></i> Segments</a>
        </td>
      </tr>`;
    }).join("");
  }

  // --- Event handlers ---
  headers.forEach((th) => {
    const activate = () => {
      const col = th.dataset.col;
      if (state.sort === col) state.dir = state.dir === "asc" ? "desc" : "asc";
      else { state.sort = col; state.dir = col === "customers_analyzed" || col === "created_at" ? "desc" : "asc"; }
      render();
    };
    th.addEventListener("click", activate);
    th.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); activate(); }
    });
  });

  let searchTimer;
  searchInput.addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(render, 200);
  });
  statusFilter.addEventListener("change", render);
  resetBtn.addEventListener("click", () => {
    searchInput.value = "";
    statusFilter.value = "";
    state.sort = "created_at"; state.dir = "desc";
    render();
  });

  load();
})();
