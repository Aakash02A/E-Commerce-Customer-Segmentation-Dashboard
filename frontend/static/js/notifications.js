// Toast / alert notifications rendered into #global-alerts.
const Notifications = (function () {
  function show(message, type = "info", dismissible = true) {
    const container = document.getElementById("global-alerts");
    if (!container) return;
    const icons = {
      success: "bi-check-circle-fill",
      danger: "bi-x-octagon-fill",
      warning: "bi-exclamation-triangle-fill",
      info: "bi-info-circle-fill",
    };
    const el = document.createElement("div");
    el.className = `alert alert-${type} alert-dismissible fade show d-flex align-items-center`;
    el.role = "alert";
    el.innerHTML = `<i class="bi ${icons[type] || icons.info} me-2"></i>
      <div>${escapeHtml(message)}</div>
      ${dismissible ? '<button type="button" class="btn-close" data-bs-dismiss="alert"></button>' : ""}`;
    container.prepend(el);
    if (type === "success") setTimeout(() => el.remove(), 5000);
    return el;
  }
  return {
    success: (m) => show(m, "success"),
    error: (m) => show(m, "danger"),
    warning: (m) => show(m, "warning"),
    info: (m) => show(m, "info"),
  };
})();
