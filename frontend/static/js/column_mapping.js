// Column mapping page: render detected mappings, allow corrections, confirm.
(function () {
  const rowsEl = document.getElementById("mapping-rows");
  const content = document.getElementById("mapping-content");
  const empty = document.getElementById("mapping-empty");
  const confirmBtn = document.getElementById("confirm-mapping");

  let uploadId = Session.getUploadId();
  let standardFields = [];

  if (!uploadId) {
    empty.classList.remove("d-none");
    return;
  }

  init();

  async function init() {
    try {
      const res = await api(`/api/uploads/${uploadId}/mapping`);
      standardFields = res.data.standard_fields;
      renderRows(res.data.detected_mapping, res.data.confirmed_mapping);
      content.classList.remove("d-none");
    } catch (e) {
      Notifications.error(e.message);
      empty.classList.remove("d-none");
    }
  }

  function fieldOptions(selected) {
    const opts = ['<option value="">— Unused —</option>'];
    for (const f of standardFields) {
      const req = f.requirement === "required" ? ' <span class="text-danger">*</span>' : "";
      const sel = f.field === selected ? "selected" : "";
      opts.push(`<option value="${f.field}" ${sel}>${escapeHtml(f.label)}${req}</option>`);
    }
    return opts.join("");
  }

  function confBadge(status) {
    const cls = { high: 2, medium: 1, low: 0, unmapped: 0, conflict: 0 }[status] ?? 0;
    const label = status === "conflict" ? "conflict" : status;
    return `<span class="badge confidence-badge-${cls}">${escapeHtml(label)}</span>`;
  }

  function renderRows(detected, confirmed) {
    rowsEl.innerHTML = detected.map((d) => {
      const current = confirmed ? (confirmed[d.column] ?? "") : (d.suggested_field || "");
      return `<tr data-column="${escapeHtml(d.column)}">
        <td class="fw-semibold">${escapeHtml(d.column)}</td>
        <td><span class="text-cap">${escapeHtml(d.detected_kind)}</span></td>
        <td class="sample-values">${escapeHtml((d.sample_values || []).join(" · "))}</td>
        <td>${confBadge(d.match_status)} <small class="text-secondary">${d.confidence ?? 0}</small></td>
        <td>
          <select class="form-select form-select-sm map-select">${fieldOptions(current)}</select>
          <div class="invalid-feedback d-block map-status small"></div>
        </td>
      </tr>`;
    }).join("");

    rowsEl.querySelectorAll(".map-select").forEach((sel) =>
      sel.addEventListener("change", () => validateLocal()));
    validateLocal();
  }

  // Client-side guard: required fields present + no two columns share a field.
  function collectMapping() {
    const mapping = {};
    rowsEl.querySelectorAll("tr").forEach((tr) => {
      mapping[tr.dataset.column] = tr.querySelector(".map-select").value;
    });
    return mapping;
  }

  function validateLocal() {
    const mapping = collectMapping();
    const required = standardFields.filter((f) => f.requirement === "required").map((f) => f.field);
    const targetCounts = {};
    Object.values(mapping).forEach((t) => { if (t) targetCounts[t] = (targetCounts[t] || 0) + 1; });

    let firstDuplicateRow = null;
    rowsEl.querySelectorAll("tr").forEach((tr) => {
      const target = tr.querySelector(".map-select").value;
      const status = tr.querySelector(".map-status");
      status.textContent = "";
      tr.classList.remove("table-danger");
      if (target && targetCounts[target] > 1) {
        status.textContent = "Another column also uses this field.";
        tr.classList.add("table-warning");
      }
    });

    const missing = required.filter((f) => !Object.values(mapping).includes(f));
    const hint = document.getElementById("mapping-hint");
    if (missing.length) {
      const labels = missing.map((m) => standardFields.find((f) => f.field === m).label).join(", ");
      hint.className = "alert alert-warning py-2";
      hint.innerHTML = `<i class="bi bi-exclamation-triangle"></i> Missing required field(s): <strong>${escapeHtml(labels)}</strong>. Map a column to each before continuing.`;
      confirmBtn.disabled = true;
    } else {
      hint.className = "alert alert-info py-2";
      hint.innerHTML = `<i class="bi bi-info-circle"></i> All required fields are mapped. Review low/medium-confidence columns and confirm.`;
      confirmBtn.disabled = false;
    }
    return { mapping, missing };
  }

  confirmBtn.addEventListener("click", async () => {
    const { mapping, missing } = validateLocal();
    if (missing.length) {
      Notifications.error("Please map all required fields before continuing.");
      return;
    }
    confirmBtn.disabled = true;
    try {
      const res = await api(`/api/uploads/${uploadId}/mapping`, {
        method: "POST",
        body: JSON.stringify({ mapping }),
      });
      Notifications.success(res.message);
      window.location.href = "/validation";
    } catch (e) {
      confirmBtn.disabled = false;
      Notifications.error(e.message);
      if (e.details && e.details.errors) {
        Notifications.error(e.details.errors.join(" "));
      }
    }
  });
})();
