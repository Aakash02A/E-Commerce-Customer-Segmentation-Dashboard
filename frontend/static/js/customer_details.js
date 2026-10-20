// Customer details table: server-side search, segment filter, sort, pagination.
(function () {
  const params = new URLSearchParams(location.search);
  const select = document.getElementById("analysis-select");
  const searchInput = document.getElementById("search");
  const segmentFilter = document.getElementById("segment-filter");
  const pageSizeSel = document.getElementById("page-size");
  const rowsEl = document.getElementById("cust-rows");
  const countEl = document.getElementById("cust-count");
  const footerEl = document.getElementById("cust-footer-info");
  const pagerEl = document.getElementById("cust-pager");
  const emptyEl = document.getElementById("cust-empty");
  const errorEl = document.getElementById("cust-error");
  const contentEl = document.getElementById("cust-content");
  const exportEl = document.getElementById("cust-export");

  const state = {
    analysisId: null,
    search: "",
    segment: params.get("segment") || "",
    sort: "monetary",
    direction: "desc",
    page: 1,
    pageSize: 20,
  };

  function showError(m) { errorEl.textContent = m; errorEl.classList.remove("d-none"); }
  function clearError() { errorEl.classList.add("d-none"); }
  function setEmpty(show) { emptyEl.classList.toggle("d-none", !show); contentEl.classList.toggle("d-none", show); }

  function fmt(v, money = false, digits = 0) {
    if (v == null || v === "") return "-";
    return money ? formatCurrency(v) : formatNumber(v, digits);
  }

  async function loadTable() {
    clearError();
    const q = new URLSearchParams({
      search: state.search, segment: state.segment, sort: state.sort,
      direction: state.direction, page: state.page, page_size: state.pageSize,
    });
    let d;
    try { d = (await api(`/api/analyses/${state.analysisId}/customers?${q}`)).data; }
    catch (e) { showError(e.message); return; }
    countEl.textContent = `${formatNumber(d.total)} customer${d.total === 1 ? "" : "s"}` +
      (state.segment ? ` in "${state.segment}"` : "") + (state.search ? ` matching "${state.search}"` : "");
    footerEl.textContent = `Page ${d.page} of ${d.pages} · ${d.total} total`;
    if (!d.items.length) {
      rowsEl.innerHTML = '<tr><td colspan="7" class="text-center text-secondary py-4">No customers match these filters.</td></tr>';
      pagerEl.innerHTML = "";
      return;
    }
    rowsEl.innerHTML = d.items.map((c) => `<tr>
      <td class="fw-semibold">${escapeHtml(c.customer_id)}</td>
      <td><small>${escapeHtml(c.last_order_date || "-")}</small></td>
      <td class="text-end">${fmt(c.recency_days)}</td>
      <td class="text-end">${fmt(c.frequency)}</td>
      <td class="text-end">${fmt(c.monetary, true, 2)}</td>
      <td class="text-center"><small class="sample-values">${fmt(c.r_score)}/${fmt(c.f_score)}/${fmt(c.m_score)}</small></td>
      <td>${c.segment ? `<span class="seg-badge" data-segment="${escapeHtml(c.segment)}">${escapeHtml(c.segment)}</span>` : '<span class="text-secondary">-</span>'}</td>
    </tr>`).join("");
    renderPager(d.page, d.pages);
  }

  function renderPager(page, pages) {
    if (pages <= 1) { pagerEl.innerHTML = ""; return; }
    const btn = (label, target, disabled, active, ariaLabel) =>
      `<li class="page-item ${disabled ? "disabled" : ""} ${active ? "active" : ""}">` +
      `<a class="page-link" href="#" data-page="${target}"` +
      `${ariaLabel ? ` aria-label="${ariaLabel}"` : ""}` +
      `${active ? ` aria-current="page"` : ""}>${label}</a></li>`;
    let html = btn("&laquo;", page - 1, page <= 1, false, "Previous page");
    const start = Math.max(1, page - 2), end = Math.min(pages, start + 4);
    for (let p = start; p <= end; p++) html += btn(p, p, false, p === page, `Page ${p}`);
    html += btn("&raquo;", page + 1, page >= pages, false, "Next page");
    pagerEl.innerHTML = html;
  }

  pagerEl.addEventListener("click", (e) => {
    const a = e.target.closest("a[data-page]");
    if (!a) return;
    e.preventDefault();
    const p = parseInt(a.dataset.page, 10);
    if (p >= 1) { state.page = p; loadTable(); }
  });

  function updateSortIndicators() {
    document.querySelectorAll("th.sortable").forEach((th) => {
      const base = th.querySelector(".sort-ind");
      if (base) base.remove();
      if (th.dataset.col === state.sort) {
        const ind = document.createElement("span");
        ind.className = "sort-ind";
        ind.textContent = state.direction === "asc" ? " ▲" : " ▼";
        th.appendChild(ind);
        th.setAttribute("aria-sort", state.direction === "asc" ? "ascending" : "descending");
      } else {
        th.setAttribute("aria-sort", "none");
      }
      th.setAttribute("tabindex", "0");
      th.setAttribute("role", "columnheader");
    });
  }

  document.querySelectorAll("th.sortable").forEach((th) => {
    const activate = () => {
      const col = th.dataset.col;
      if (state.sort === col) state.direction = state.direction === "asc" ? "desc" : "asc";
      else { state.sort = col; state.direction = "desc"; }
      state.page = 1;
      updateSortIndicators();
      loadTable();
    };
    th.addEventListener("click", activate);
    th.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); activate(); }
    });
  });

  let searchTimer;
  searchInput.addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => { state.search = searchInput.value.trim(); state.page = 1; loadTable(); }, 300);
  });
  segmentFilter.addEventListener("change", () => { state.segment = segmentFilter.value; state.page = 1; loadTable(); });
  pageSizeSel.addEventListener("change", () => { state.pageSize = parseInt(pageSizeSel.value, 10); state.page = 1; loadTable(); });
  document.getElementById("reset").addEventListener("click", () => {
    searchInput.value = ""; segmentFilter.value = ""; state.search = ""; state.segment = ""; state.page = 1; loadTable();
  });

  // Populate the segment dropdown from this analysis's available segments.
  async function populateSegments() {
    try {
      const d = (await api(`/api/analyses/${state.analysisId}/dashboard`)).data;
      const names = d.segment_breakdown.segments.map((s) => s.segment);
      segmentFilter.innerHTML = '<option value="">All segments</option>' +
        names.map((n) => `<option value="${escapeHtml(n)}" ${n === state.segment ? "selected" : ""}>${escapeHtml(n)}</option>`).join("");
    } catch (e) { /* ignore; table still works unfiltered */ }
  }

  async function switchAnalysis(id) {
    state.analysisId = id;
    Session.setAnalysisId(id);
    state.page = 1;
    state.segment = params.get("segment") || "";
    exportEl.href = `/api/analyses/${id}/export?format=csv&dataset=customers`;
    await populateSegments();
    await loadTable();
  }

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
    updateSortIndicators();
    await switchAnalysis(active);
  }

  select.addEventListener("change", () => switchAnalysis(select.value));
  loadAnalyses();
})();
