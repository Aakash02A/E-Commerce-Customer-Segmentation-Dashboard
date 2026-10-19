// Chart.js helpers: a consistent palette and a registry so charts are destroyed
// before being redrawn (switching analyses must never stack overlapping charts).
const Charts = (function () {
  const PALETTE = [
    "#2563eb", "#16a34a", "#f59e0b", "#dc2626", "#7c3aed",
    "#0891b2", "#db2777", "#65a30d", "#ea580c", "#475569",
  ];
  const registry = {};

  function destroy(key) {
    if (registry[key]) {
      registry[key].destroy();
      delete registry[key];
    }
  }

  function defaults() {
    Chart.defaults.font.family =
      '"Segoe UI", system-ui, -apple-system, sans-serif';
    Chart.defaults.color = "#475569";
  }

  // labels/values -> doughnut (segment distribution).
  function doughnut(canvasId, key, labels, values) {
    destroy(key);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    registry[key] = new Chart(ctx, {
      type: "doughnut",
      data: {
        labels,
        datasets: [{
          data: values,
          backgroundColor: labels.map((_, i) => PALETTE[i % PALETTE.length]),
          borderColor: "#fff",
          borderWidth: 2,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: "right", labels: { boxWidth: 14, padding: 12 } },
          tooltip: {
            callbacks: {
              label: (c) => {
                const total = c.dataset.data.reduce((a, b) => a + b, 0) || 1;
                const pct = ((c.raw / total) * 100).toFixed(1);
                return ` ${c.label}: ${c.raw} (${pct}%)`;
              },
            },
          },
        },
      },
    });
    return registry[key];
  }

  // fixed 1..5 bins -> bar (RFM score distribution).
  function scoreBar(canvasId, key, countsByScore) {
    destroy(key);
    const ctx = document.getElementById(canvasId);
    if (!ctx) return null;
    const labels = [1, 2, 3, 4, 5];
    const values = labels.map((s) => countsByScore[s] || 0);
    registry[key] = new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: [{
          label: "Customers",
          data: values,
          backgroundColor: "#2563eb",
          borderRadius: 6,
          maxBarThickness: 48,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          y: { beginAtZero: true, ticks: { precision: 0 }, title: { display: true, text: "Customers" } },
          x: { title: { display: true, text: "Score (1 = low, 5 = high)" } },
        },
      },
    });
    return registry[key];
  }

  return { defaults, doughnut, scoreBar, destroy, PALETTE };
})();
