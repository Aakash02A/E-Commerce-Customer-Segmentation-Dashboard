// Shared helpers: sidebar toggle, API wrapper, active-session storage.

// Mobile sidebar toggle button, injected once.
(function initSidebarToggle() {
  document.addEventListener("DOMContentLoaded", () => {
    const btn = document.createElement("button");
    btn.className = "sidebar-toggle";
    btn.innerHTML = '<i class="bi bi-list"></i>';
    btn.addEventListener("click", () =>
      document.querySelector(".sidebar").classList.toggle("show")
    );
    document.body.appendChild(btn);
  });
})();

// Track the current working upload across pages via localStorage.
const Session = {
  getUploadId() {
    return localStorage.getItem("active_upload_id");
  },
  setUploadId(id) {
    localStorage.setItem("active_upload_id", id);
  },
  clearUploadId() {
    localStorage.removeItem("active_upload_id");
  },
};

// Consistent JSON API helper. Throws on network or success=false.
async function api(path, options = {}) {
  const resp = await fetch(path, {
    headers: options.body instanceof FormData ? {} : { "Content-Type": "application/json" },
    ...options,
  });
  let payload;
  try {
    payload = await resp.json();
  } catch (e) {
    throw new Error(`Unexpected server response (HTTP ${resp.status})`);
  }
  if (!resp.ok || payload.success === false) {
    const err = new Error(payload.message || `Request failed (HTTP ${resp.status})`);
    err.details = payload.details;
    err.payload = payload;
    err.status = resp.status;
    throw err;
  }
  return payload;
}

function escapeHtml(value) {
  const div = document.createElement("div");
  div.textContent = value == null ? "" : String(value);
  return div.innerHTML;
}

function formatNumber(n, digits = 0) {
  if (n == null || isNaN(n)) return "-";
  return Number(n).toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatCurrency(n) {
  if (n == null || isNaN(n)) return "-";
  return Number(n).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  });
}
