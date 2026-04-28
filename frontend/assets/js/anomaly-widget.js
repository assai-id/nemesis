/**
 * anomaly-widget.js
 * 
 * Self-contained frontend widget for rendering anomaly results.
 * No build step, no framework — plain DOM manipulation.
 * 
 * Usage:
 *   <div id="anomaly-panel"></div>
 *   <script src="anomaly-widget.js"></script>
 *   <script>
 *     AnomalyWidget.mount("#anomaly-panel", { contractId: "PKT-001" });
 *   </script>
 */

(function (global) {
  "use strict";

  const LABEL_CONFIG = {
    HIGH:   { color: "#ef4444", bg: "#fef2f2", icon: "🔴", text: "RISIKO TINGGI" },
    MEDIUM: { color: "#f59e0b", bg: "#fffbeb", icon: "🟡", text: "PERLU PERHATIAN" },
    LOW:    { color: "#3b82f6", bg: "#eff6ff", icon: "🔵", text: "RISIKO RENDAH" },
    CLEAN:  { color: "#10b981", bg: "#f0fdf4", icon: "🟢", text: "BERSIH" },
  };

  const ANOMALY_LABELS = {
    SINGLE_SOURCE_HIGH_VALUE: "Penunjukan Langsung Bernilai Tinggi",
    PRICE_OUTLIER:            "Harga Tidak Wajar",
    DEADLINE_CRAMMING:        "Batas Waktu Terlalu Sempit",
    VENDOR_CONCENTRATION:     "Konsentrasi Vendor",
    ROUND_NUMBER_PRICE:       "Harga Bulat Mencurigakan",
  };

  // ── Fetch contract score from backend API ──────────────────────────────────

  async function fetchScore(contractId) {
    const res = await fetch(`/api/anomaly/score/${encodeURIComponent(contractId)}`);
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
  }

  async function fetchTopAnomalies(params = {}) {
    const qs = new URLSearchParams({
      limit: params.limit ?? 20,
      label: params.label ?? "",
      satuan_kerja: params.satuanKerja ?? "",
    }).toString();
    const res = await fetch(`/api/anomaly/top?${qs}`);
    if (!res.ok) throw new Error(`API error: ${res.status}`);
    return res.json();
  }

  // ── Score Bar Component ────────────────────────────────────────────────────

  function renderScoreBar(score, label) {
    const cfg = LABEL_CONFIG[label] ?? LABEL_CONFIG.CLEAN;
    const pct = Math.round(score * 100);

    return `
      <div class="nms-score-container">
        <div class="nms-score-header">
          <span class="nms-label-badge" style="background:${cfg.bg};color:${cfg.color};border-color:${cfg.color}">
            ${cfg.icon} ${cfg.text}
          </span>
          <span class="nms-score-value" style="color:${cfg.color}">${pct}/100</span>
        </div>
        <div class="nms-bar-track">
          <div class="nms-bar-fill" 
               style="width:${pct}%;background:${cfg.color}" 
               data-score="${pct}">
          </div>
        </div>
      </div>
    `;
  }

  // ── Anomaly Card Component ─────────────────────────────────────────────────

  function renderAnomalyCard(anomaly) {
    const label = ANOMALY_LABELS[anomaly.type] ?? anomaly.type;
    const severityPct = Math.round(anomaly.score * 100);

    const evidenceHTML = Object.entries(anomaly.evidence)
      .map(([k, v]) => `
        <tr>
          <td class="nms-ev-key">${formatKey(k)}</td>
          <td class="nms-ev-val">${formatValue(k, v)}</td>
        </tr>
      `)
      .join("");

    return `
      <div class="nms-anomaly-card">
        <div class="nms-anomaly-header">
          <span class="nms-anomaly-type">${label}</span>
          <span class="nms-anomaly-score">${severityPct}%</span>
        </div>
        <p class="nms-anomaly-desc">${anomaly.description}</p>
        <details class="nms-evidence">
          <summary>Lihat Data Pendukung</summary>
          <table class="nms-evidence-table">
            <tbody>${evidenceHTML}</tbody>
          </table>
        </details>
      </div>
    `;
  }

  // ── Top Anomalies Table ────────────────────────────────────────────────────

  function renderTopTable(results) {
    if (!results.length) {
      return `<div class="nms-empty">Tidak ada anomali ditemukan dengan filter ini.</div>`;
    }

    const rows = results.map(r => {
      const cfg = LABEL_CONFIG[r.label] ?? LABEL_CONFIG.CLEAN;
      return `
        <tr class="nms-table-row" data-id="${r.contract_id}">
          <td class="nms-td">${r.contract_id}</td>
          <td class="nms-td">${r.nama_paket ?? "—"}</td>
          <td class="nms-td">${r.satuan_kerja ?? "—"}</td>
          <td class="nms-td nms-center">
            <span class="nms-label-badge sm" style="background:${cfg.bg};color:${cfg.color};border-color:${cfg.color}">
              ${cfg.icon} ${r.label}
            </span>
          </td>
          <td class="nms-td nms-right">${Math.round(r.score * 100)}</td>
          <td class="nms-td nms-center">${r.anomaly_count}</td>
        </tr>
      `;
    }).join("");

    return `
      <table class="nms-table">
        <thead>
          <tr>
            <th class="nms-th">ID Paket</th>
            <th class="nms-th">Nama Paket</th>
            <th class="nms-th">Satuan Kerja</th>
            <th class="nms-th nms-center">Status</th>
            <th class="nms-th nms-right">Skor</th>
            <th class="nms-th nms-center">Anomali</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
  }

  // ── Main Mount Function ────────────────────────────────────────────────────

  async function mount(selector, options = {}) {
    const container = document.querySelector(selector);
    if (!container) throw new Error(`AnomalyWidget: element "${selector}" not found`);

    injectStyles();
    container.innerHTML = `<div class="nms-loading">Memuat data anomali…</div>`;

    try {
      let html = "";

      if (options.contractId) {
        // Single contract view
        const data = await fetchScore(options.contractId);
        html = `
          <div class="nms-panel">
            <h3 class="nms-panel-title">Analisis Risiko Kontrak</h3>
            <p class="nms-contract-id">ID: ${data.contract_id}</p>
            ${renderScoreBar(data.score, data.label)}
            ${data.anomalies.length > 0
              ? `<div class="nms-anomaly-list">${data.anomalies.map(renderAnomalyCard).join("")}</div>`
              : `<p class="nms-clean-msg">✅ Tidak ada anomali terdeteksi.</p>`
            }
          </div>
        `;
      } else {
        // Top anomalies list view
        const data = await fetchTopAnomalies(options);
        html = `
          <div class="nms-panel">
            <h3 class="nms-panel-title">Kontrak Berisiko Tinggi</h3>
            <p class="nms-subtitle">${data.total ?? data.results?.length ?? 0} kontrak ditemukan</p>
            ${renderTopTable(data.results ?? data)}
          </div>
        `;
        // Add click handler after render
        setTimeout(() => {
          container.querySelectorAll(".nms-table-row").forEach(row => {
            row.addEventListener("click", () => {
              const id = row.dataset.id;
              if (options.onSelect) options.onSelect(id);
            });
          });
        }, 0);
      }

      container.innerHTML = html;

      // Animate score bar
      requestAnimationFrame(() => {
        container.querySelectorAll(".nms-bar-fill").forEach(bar => {
          bar.style.transition = "width 0.8s cubic-bezier(0.4, 0, 0.2, 1)";
        });
      });

    } catch (err) {
      container.innerHTML = `
        <div class="nms-error">
          <strong>Gagal memuat data:</strong> ${err.message}
        </div>
      `;
    }
  }

  // ── Style Injection ────────────────────────────────────────────────────────

  function injectStyles() {
    if (document.getElementById("nms-styles")) return;
    const style = document.createElement("style");
    style.id = "nms-styles";
    style.textContent = `
      .nms-loading { padding: 1rem; color: #6b7280; font-size: 0.875rem; }
      .nms-error   { padding: 1rem; color: #dc2626; background: #fef2f2; border-radius: 6px; }
      .nms-empty   { padding: 1.5rem; text-align: center; color: #9ca3af; }
      .nms-panel   { font-family: system-ui, -apple-system, sans-serif; }
      .nms-panel-title { font-size: 1.1rem; font-weight: 700; color: #111827; margin: 0 0 0.25rem; }
      .nms-subtitle    { font-size: 0.8rem; color: #6b7280; margin: 0 0 1rem; }
      .nms-contract-id { font-size: 0.75rem; color: #9ca3af; margin: 0 0 1rem; font-family: monospace; }

      .nms-score-container { margin-bottom: 1.5rem; }
      .nms-score-header    { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem; }
      .nms-score-value     { font-weight: 700; font-size: 1.25rem; }
      .nms-bar-track       { height: 8px; background: #f3f4f6; border-radius: 4px; overflow: hidden; }
      .nms-bar-fill        { height: 100%; width: 0; border-radius: 4px; }

      .nms-label-badge     { display: inline-flex; align-items: center; gap: 0.25rem; padding: 0.2rem 0.6rem; border-radius: 999px; font-size: 0.75rem; font-weight: 600; border: 1px solid; }
      .nms-label-badge.sm  { padding: 0.15rem 0.4rem; font-size: 0.7rem; }

      .nms-anomaly-list    { display: flex; flex-direction: column; gap: 0.75rem; }
      .nms-anomaly-card    { border: 1px solid #e5e7eb; border-radius: 8px; padding: 0.875rem; }
      .nms-anomaly-header  { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.4rem; }
      .nms-anomaly-type    { font-weight: 600; font-size: 0.875rem; color: #111827; }
      .nms-anomaly-score   { font-size: 0.75rem; color: #6b7280; }
      .nms-anomaly-desc    { font-size: 0.8rem; color: #374151; margin: 0 0 0.5rem; line-height: 1.5; }

      .nms-evidence        { font-size: 0.75rem; }
      .nms-evidence summary { cursor: pointer; color: #6b7280; user-select: none; }
      .nms-evidence-table  { width: 100%; border-collapse: collapse; margin-top: 0.5rem; }
      .nms-ev-key          { color: #9ca3af; padding: 0.2rem 0.5rem 0.2rem 0; width: 40%; }
      .nms-ev-val          { color: #111827; font-family: monospace; padding: 0.2rem 0; }

      .nms-clean-msg       { color: #10b981; font-size: 0.875rem; }

      .nms-table           { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
      .nms-th              { text-align: left; padding: 0.5rem 0.75rem; border-bottom: 2px solid #e5e7eb; font-weight: 600; color: #6b7280; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.05em; }
      .nms-td              { padding: 0.6rem 0.75rem; border-bottom: 1px solid #f3f4f6; color: #374151; }
      .nms-table-row:hover { background: #f9fafb; cursor: pointer; }
      .nms-center          { text-align: center; }
      .nms-right           { text-align: right; font-weight: 600; }
    `;
    document.head.appendChild(style);
  }

  // ── Utilities ──────────────────────────────────────────────────────────────

  function formatKey(key) {
    return key.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
  }

  function formatValue(key, value) {
    if (typeof value === "number" && key.includes("idr")) {
      return new Intl.NumberFormat("id-ID", {
        style: "currency", currency: "IDR", maximumFractionDigits: 0,
      }).format(value);
    }
    if (typeof value === "number" && key.includes("score")) {
      return value.toFixed(2);
    }
    return String(value);
  }

  // ── Expose API ─────────────────────────────────────────────────────────────

  global.AnomalyWidget = { mount };

})(window);
