// Home page stats (Phase 11 Task 2: extracted from the inline block in
// index.html so the strict CSP script-src ('self' + CDN, no 'unsafe-inline')
// holds on every page). Relies on the api()/formatNumber() globals from
// main.js, which load before this file (see base.html + the page scripts block).
(function loadHome() {
  (async function () {
    try {
      await api("/api/health");
      document.getElementById("stat-backend").textContent = "Online";
    } catch (e) {
      const el = document.getElementById("stat-backend");
      el.textContent = "Offline";
      el.classList.replace("text-success", "text-danger");
    }
    try {
      const res = await api("/api/uploads/summary");
      document.getElementById("stat-analyses").textContent = formatNumber(res.data.total_uploads);
      document.getElementById("stat-ready").textContent = formatNumber(res.data.ready_for_analysis);
    } catch (e) {
      document.getElementById("stat-analyses").textContent = "0";
      document.getElementById("stat-ready").textContent = "0";
    }
  })();
})();
