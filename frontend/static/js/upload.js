// Upload page logic: drag/drop, XHR upload with progress, preview, sheet selection.
(function () {
  const dz = document.getElementById("dropzone");
  const input = document.getElementById("file-input");
  const browseBtn = document.getElementById("browse-btn");
  const progressWrap = document.getElementById("upload-progress-wrap");
  const progressBar = document.getElementById("upload-progress");
  const summaryEl = document.getElementById("upload-summary");
  const previewCard = document.getElementById("preview-card");
  const previewTable = document.getElementById("preview-table");
  const sheetSelector = document.getElementById("sheet-selector");
  const sheetSelect = document.getElementById("sheet-select");
  const continueWrap = document.getElementById("continue-wrap");
  const continueBtn = document.getElementById("continue-mapping");

  let currentUploadId = null;

  browseBtn.addEventListener("click", (e) => { e.stopPropagation(); input.click(); });
  dz.addEventListener("click", () => input.click());
  input.addEventListener("change", () => { if (input.files[0]) uploadFile(input.files[0]); });

  ["dragenter", "dragover"].forEach((ev) =>
    dz.addEventListener(ev, (e) => { e.preventDefault(); dz.classList.add("dragover"); }));
  ["dragleave", "drop"].forEach((ev) =>
    dz.addEventListener(ev, (e) => { e.preventDefault(); dz.classList.remove("dragover"); }));
  dz.addEventListener("drop", (e) => {
    const file = e.dataTransfer.files[0];
    if (file) uploadFile(file);
  });

  function uploadFile(file) {
    const ext = (file.name.split(".").pop() || "").toLowerCase();
    if (!["csv", "xlsx"].includes(ext)) {
      Notifications.error(`Unsupported file type ".${ext}". Please upload CSV or XLSX.`);
      return;
    }
    const form = new FormData();
    form.append("file", file);

    progressWrap.classList.remove("d-none");
    setProgress(0);
    const xhr = new XMLHttpRequest();
    xhr.open("POST", "/api/uploads");
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable) setProgress(Math.round((e.loaded / e.total) * 100));
    };
    xhr.onload = () => {
      progressWrap.classList.add("d-none");
      let resp;
      try { resp = JSON.parse(xhr.responseText); } catch { resp = {}; }
      if (xhr.status >= 200 && xhr.status < 300 && resp.success) {
        handleUploaded(resp.data);
      } else {
        Notifications.error(resp.message || "Upload failed.");
      }
    };
    xhr.onerror = () => { progressWrap.classList.add("d-none"); Notifications.error("Network error during upload."); };
    xhr.send(form);
  }

  function setProgress(pct) {
    progressBar.style.width = pct + "%";
    progressBar.textContent = pct + "%";
    if (pct >= 100) progressBar.classList.remove("progress-bar-animated");
    else progressBar.classList.add("progress-bar-animated");
  }

  function handleUploaded(data) {
    currentUploadId = data.upload_id;
    Session.setUploadId(data.upload_id);
    Notifications.success(`Uploaded "${data.filename}" (${data.row_count} rows, ${data.column_count} columns).`);
    renderSummary(data);
    renderPreview(data.preview);
    configureSheets(data);
    continueBtn.href = "/mapping";
    continueWrap.classList.remove("d-none");
  }

  function configureSheets(data) {
    if (data.sheet_names && data.sheet_names.length > 1) {
      sheetSelector.classList.remove("d-none");
      sheetSelect.innerHTML = data.sheet_names
        .map((s) => `<option value="${escapeHtml(s)}" ${s === data.selected_sheet ? "selected" : ""}>${escapeHtml(s)}</option>`)
        .join("");
      sheetSelect.onchange = () => loadSheet(sheetSelect.value);
    } else {
      sheetSelector.classList.add("d-none");
    }
  }

  async function loadSheet(sheet) {
    try {
      const res = await api(`/api/uploads/${currentUploadId}/preview?sheet=${encodeURIComponent(sheet)}`);
      renderPreview(res.data.preview);
      summaryEl.innerHTML = summaryHtml({ row_count: res.data.row_count, filename: "selected sheet" });
    } catch (e) {
      Notifications.error(e.message);
    }
  }

  function summaryHtml(data) {
    return `<ul class="list-unstyled mb-0">
      <li><i class="bi bi-file-earmark-text text-primary"></i> <strong>${escapeHtml(data.filename)}</strong></li>
      <li>Rows detected: <strong>${formatNumber(data.row_count)}</strong></li>
      <li>Columns detected: <strong>${formatNumber(data.column_count)}</strong></li>
      <li class="mt-2 text-success"><i class="bi bi-check-circle"></i> File validated. Column mapping auto-detected.</li>
    </ul>`;
  }

  function renderSummary(data) {
    summaryEl.innerHTML = summaryHtml(data);
  }

  function renderPreview(preview) {
    previewCard.classList.remove("d-none");
    if (!preview || !preview.columns || !preview.columns.length) {
      previewTable.innerHTML = '<p class="text-secondary mb-0">No rows to preview.</p>';
      return;
    }
    const head = preview.columns.map((c) => `<th class="text-nowrap">${escapeHtml(c)}</th>`).join("");
    const body = preview.rows.map((row) =>
      `<tr>${row.map((cell) => `<td>${escapeHtml(cell)}</td>`).join("")}</tr>`).join("");
    previewTable.innerHTML =
      `<table class="table table-sm table-bordered table-striped align-middle mb-0">
        <thead class="sticky-top table-light"><tr>${head}</tr></thead>
        <tbody>${body}</tbody></table>`;
  }
})();
