/* global fetch, URLSearchParams, window */
(() => {
  const API_BASE_URL = (window['DASHBOARD_API_BASE_URL'] || '/api').replace(
    /\/$/,
    ''
  );

  if (!window['AuditMap']) {
    console.error('AuditMap failed to load.');
    return;
  }

  const state = {
    viewMode: 'dashboard',
    mapFilter: 'kabkota',
    tab: 'all',
    selectedAreaKey: null,
    selectedOwnerKey: null,
    search: '',
    sortBy: 'waste',
    isLegendHidden: false,
    modalRequestId: 0,
    modal: {
      areaType: 'region',
      areaKey: null,
      ownerName: '',
      page: 1,
      pageSize: 25,
      search: '',
      ownerType: '',
      severity: '',
      priorityOnly: false,
    },
  };

  const dom = {
    kpi: document.getElementById('kpi'),
    mapRoot: document.getElementById('map'),
    mapFilters: document.getElementById('mf'),
    tabs: document.getElementById('tabs'),
    legend: document.getElementById('legend'),
    sidebarContent: document.getElementById('sbc'),
    casefile: document.querySelector('.casefile'),
    modalTop: document.getElementById('modalTop'),
    modalBody: document.getElementById('modalBody'),
  };

  if (Object.values(dom).some((element) => !element)) {
    console.error('Dashboard shell is incomplete.');
    return;
  }

  const FILTERS = [
    { key: 'central', label: 'Kementerian/Lembaga' },
    { key: 'provinsi', label: 'Pemprov' },
    { key: 'kabkota', label: 'Pemkot' },
    { key: 'other', label: 'Others' },
  ];

  const TABS = [
    { key: 'all', label: 'Semua' },
    { key: 'kabupaten', label: 'Kabupaten' },
    { key: 'kota', label: 'Kota' },
  ];

  const SEVERITY_FILTER_OPTIONS = [
    { value: '',         label: 'Semua paket' },
    { value: 'priority', label: 'Hanya prioritas (Medium ke atas)' },
    { value: 'low',      label: 'Hanya Low' },
    { value: 'med',      label: 'Hanya Medium' },
    { value: 'high',     label: 'Hanya High' },
    { value: 'absurd',   label: 'Hanya Absurd' },
  ];

  let dashboardData = null;
  let regionsByKey = new Map();
  let provincesByKey = new Map();

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function escapeAttr(value) {
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function escapeJsString(value) {
    return String(value)
      .replace(/\\/g, '\\\\')
      .replace(/'/g, "\\'")
      .replace(/\r/g, '\\r')
      .replace(/\n/g, '\\n');
  }

  function jsArg(value) {
    if (typeof value === 'boolean') {
      return value ? 'true' : 'false';
    }
    if (typeof value === 'number') {
      return String(value);
    }
    return `'${escapeJsString(value)}'`;
  }

  function actionCall(action, ...args) {
    return escapeAttr(`dashboardActions.${action}(${args.map(jsArg).join(',')})`);
  }

  function actionExpr(expression) {
    return escapeAttr(expression);
  }

  function normalizeSourceId(sourceId) {
    if (sourceId === null || sourceId === undefined) {
      return null;
    }

    const normalized = String(sourceId).trim();
    if (!/^\d+$/.test(normalized)) {
      return null;
    }

    const parsed = Number(normalized);
    if (!Number.isSafeInteger(parsed) || parsed <= 0) {
      return null;
    }

    return String(parsed);
  }

  function buildInaprocUrl(sourceId) {
    const kode = normalizeSourceId(sourceId);
    return kode ? `https://data.inaproc.id/rup?kode=${encodeURIComponent(kode)}` : null;
  }

  function isProvinceView() {
    return state.mapFilter === 'provinsi';
  }

  function isCentralOwnerMode() {
    return state.mapFilter === 'central';
  }

  function currentAreaType() {
    return isProvinceView() ? 'province' : 'region';
  }

  function formatCompactCurrency(value) {
    const amount = Number(value) || 0;
    const abs = Math.abs(amount);
    if (abs >= 1e12) return `${(amount / 1e12).toFixed(amount % 1e12 === 0 ? 0 : 1)} T`;
    if (abs >= 1e9) return `${(amount / 1e9).toFixed(amount % 1e9 === 0 ? 0 : 1)} M`;
    if (abs >= 1e6) return `${(amount / 1e6).toFixed(amount % 1e6 === 0 ? 0 : 1)} Jt`;
    if (abs >= 1e3) return `${(amount / 1e3).toFixed(amount % 1e3 === 0 ? 0 : 1)} Rb`;
    return `${amount.toFixed(0)}`;
  }

  function formatCurrencyLong(value) {
    const number = Math.round(Number(value) || 0);
    return `Rp ${number.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.')}`;
  }

  function formatNumber(value) {
    const number = Math.round(Number(value) || 0);
    return number.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  }

  function formatDecimal(value) {
    const amount = Number(value) || 0;
    return amount % 1 === 0 ? formatNumber(amount) : amount.toFixed(2).replace('.', ',');
  }

  function ownerTypeLabel(value) {
    if (value === 'central') return 'Kementerian/Lembaga';
    if (value === 'provinsi') return 'Pemprov';
    if (value === 'kabkota') return 'Pemkot';
    if (value === 'other') return 'Others';
    return 'Tidak diketahui';
  }

  function ownerTypeCount(area, ownerType) {
    return Number(area && area.ownerMix ? area.ownerMix[ownerType] : 0) || 0;
  }

  function areaBadgeLabel(area) {
    if (area.regionType === 'Provinsi') return 'Prov.';
    if (area.regionType === 'Kota') return 'Kota';
    return 'Kab.';
  }

  function areaBadgeClass(area) {
    return area.regionType === 'Kota' ? 'bk' : 'bp';
  }

  function areaSecondaryLine(area) {
    return isProvinceView() ? 'Hanya paket pemprov' : area.provinceName;
  }

  function severityClass(severity) {
    if (severity === 'absurd') return 'sev-absurd';
    if (severity === 'high') return 'sev-high';
    if (severity === 'med') return 'sev-med';
    return 'sev-low';
  }

  function severityLabel(severity) {
    if (severity === 'absurd') return 'Absurd';
    if (severity === 'high') return 'High';
    if (severity === 'med') return 'Medium';
    return 'Low';
  }

  // Theme-aware choropleth + map stroke colors.
  // Light: civic-editorial cream → terra → blood progression on cream basemap.
  // Dark: same warm rust hue family but lifted in L so every step reads on navy.
  const CHOROPLETH_THEMES = {
    light: {
      palette: ['#dccba8', '#c69656', '#a8651e', '#84320d', '#5d1606'],
      zeroColor: '#eeeae1',
      strokeDefault: '#aaa49a',
      strokeSelected: '#84320d',
    },
    dark: {
      palette: ['#d8c794', '#c89656', '#bb6a25', '#cc4a26', '#e2473a'],
      zeroColor: '#243155',
      strokeDefault: '#3d4f78',
      strokeSelected: '#cc6e3f',
    },
  };

  function currentThemeKey() {
    return document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
  }
  function mapTheme() {
    return CHOROPLETH_THEMES[currentThemeKey()];
  }
  // These are read at render time so swapping themes + re-rendering picks up
  // the new values automatically. Kept as getter aliases to minimize churn.
  let CHOROPLETH_PALETTE = mapTheme().palette;
  let CHOROPLETH_ZERO_COLOR = mapTheme().zeroColor;
  let MAP_STROKE_DEFAULT = mapTheme().strokeDefault;
  let MAP_STROKE_SELECTED = mapTheme().strokeSelected;
  function syncMapThemeVars() {
    const t = mapTheme();
    CHOROPLETH_PALETTE = t.palette;
    CHOROPLETH_ZERO_COLOR = t.zeroColor;
    MAP_STROKE_DEFAULT = t.strokeDefault;
    MAP_STROKE_SELECTED = t.strokeSelected;
  }

  function applyChoroplethPalette(legend) {
    if (!legend || !Array.isArray(legend.ranges) || !legend.ranges.length) {
      return { ...(legend || {}), zeroColor: CHOROPLETH_ZERO_COLOR };
    }
    const n = legend.ranges.length;
    const remapped = legend.ranges.map((r, i) => ({
      ...r,
      color: CHOROPLETH_PALETTE[
        Math.min(
          CHOROPLETH_PALETTE.length - 1,
          Math.round((i / Math.max(n - 1, 1)) * (CHOROPLETH_PALETTE.length - 1))
        )
      ],
    }));
    return { ...legend, zeroColor: CHOROPLETH_ZERO_COLOR, ranges: remapped };
  }

  function totalAreaMetrics(area) {
    return {
      totalPackages: Number(area?.totalPackages) || 0,
      totalPriorityPackages: Number(area?.totalPriorityPackages) || 0,
      totalPotentialWaste: Number(area?.totalPotentialWaste) || 0,
      totalBudget: Number(area?.totalBudget) || 0,
    };
  }

  function getActiveSidebarOwnerKey() {
    return isProvinceView() ? 'provinsi' : state.mapFilter;
  }

  function activeSidebarOwnerLabel() {
    return ownerTypeLabel(getActiveSidebarOwnerKey());
  }

  function getAreaMetricsForOwner(area, ownerKey) {
    if (!area) {
      return totalAreaMetrics(null);
    }

    const metrics = area.ownerMetrics && area.ownerMetrics[ownerKey];

    if (metrics) {
      return {
        totalPackages: Number(metrics.totalPackages) || 0,
        totalPriorityPackages: Number(metrics.totalPriorityPackages) || 0,
        totalPotentialWaste: Number(metrics.totalPotentialWaste) || 0,
        totalBudget: Number(metrics.totalBudget) || 0,
      };
    }

    if (isProvinceView() && ownerKey === 'provinsi') {
      return totalAreaMetrics(area);
    }

    return {
      totalPackages: ownerTypeCount(area, ownerKey),
      totalPriorityPackages: 0,
      totalPotentialWaste: 0,
      totalBudget: 0,
    };
  }

  function getSidebarAreaMetrics(area) {
    const ownerKey = getActiveSidebarOwnerKey();
    return ownerKey ? getAreaMetricsForOwner(area, ownerKey) : totalAreaMetrics(area);
  }

  function currentSeverityFilterValue() {
    if (state.modal.priorityOnly) return 'priority';
    return state.modal.severity || '';
  }

  function renderSeverityFilterOptions(selectedValue) {
    return SEVERITY_FILTER_OPTIONS.map(
      (opt) =>
        `<option value="${escapeAttr(opt.value)}"${selectedValue === opt.value ? ' selected' : ''}>${escapeHtml(
          opt.label
        )}</option>`
    ).join('');
  }

  function getOwnerCardKey(ownerType, ownerName) {
    return `${ownerType}::${ownerName}`;
  }

  function getAreaKey(area, areaType = currentAreaType()) {
    return areaType === 'province' ? area.provinceKey : area.regionKey;
  }

  function getAreaByKey(areaType, areaKey) {
    return (areaType === 'province' ? provincesByKey : regionsByKey).get(areaKey) || null;
  }

  function getActiveAreaByKey(areaKey) {
    return getAreaByKey(currentAreaType(), areaKey);
  }

  function getActiveAreas() {
    return isProvinceView() ? dashboardData.provinceView.provinces : dashboardData.regions;
  }

  function getCentralOwnersForSidebar() {
    return dashboardData &&
      dashboardData.ownerLists &&
      Array.isArray(dashboardData.ownerLists.central)
      ? dashboardData.ownerLists.central
      : [];
  }

  function getActiveGeo() {
    return isProvinceView() ? dashboardData.provinceView.geo : dashboardData.geo;
  }

  function getActiveLegend() {
    return isProvinceView() ? dashboardData.provinceView.legend : dashboardData.legend;
  }

  function getFeatureAreaKey(feature) {
    return isProvinceView() ? feature.properties.provinceKey : feature.properties.regionKey;
  }

  function ensureMapStatus() {
    let status = document.getElementById('mapStatus');
    if (!status) {
      status = document.createElement('div');
      status.id = 'mapStatus';
      status.className = 'map-status';
      dom.mapRoot.parentElement.appendChild(status);
    }
    return status;
  }

  function setMapStatus(message, isError) {
    const status = ensureMapStatus();
    status.className = `map-status${isError ? ' error' : ''}`;
    status.textContent = message;
  }

  function clearMapStatus() {
    const status = document.getElementById('mapStatus');
    if (status) {
      status.remove();
    }
  }

  function renderKpiCards(cards) {
    dom.kpi.innerHTML = cards
      .map(
        (item) =>
          `<div class="kc">` +
          `<div class="kl">${escapeHtml(item.label)}</div>` +
          `<div class="kv">${escapeHtml(item.value)}</div>` +
          (item.sublabel ? `<div class="ks">${escapeHtml(item.sublabel)}</div>` : '') +
          `</div>`
      )
      .join('');
  }

  function renderSidebarMessage(message, isError) {
    dom.sidebarContent.innerHTML = `<div class="panel-msg${isError ? ' error' : ''}">${escapeHtml(message)}</div>`;
  }

  function setCaseTitle(title) {
    if (dom.modalTop instanceof HTMLElement) {
      dom.modalTop.textContent = title || 'Berkas';
    }
  }

  function buildSummaryItem(item) {
    const muted = item.muted ? ' class="muted"' : '';
    return `<div${muted}><dt>${escapeHtml(item.label)}</dt><dd>${escapeHtml(item.value)}</dd></div>`;
  }

  function buildCaseSummary({ heroLabel, heroValue, heroSub, secondary, sections }) {
    let html = `<aside class="case-summary"><div class="case-section-label">Ringkasan</div>`;
    html += `<dl class="stack-stats">`;
    html += `<div class="stat-hero">` +
      `<dt>${escapeHtml(heroLabel)}</dt>` +
      `<dd><strong>${escapeHtml(heroValue)}</strong>` +
      (heroSub ? `<span>${escapeHtml(heroSub)}</span>` : '') +
      `</dd></div>`;
    (secondary || []).forEach((item) => { html += buildSummaryItem(item); });
    html += `</dl>`;
    (sections || []).forEach((section) => {
      if (!section.items || !section.items.length) return;
      html += `<div class="case-section-label small">${escapeHtml(section.title)}</div>`;
      html += `<dl class="stack-stats">`;
      section.items.forEach((item) => { html += buildSummaryItem(item); });
      html += `</dl>`;
    });
    html += `</aside>`;
    return html;
  }

  function buildCaseFeature(featured) {
    const body = featured
      ? buildFeaturedReason(featured)
      : `<div class="case-feature-empty">Belum ada paket dengan deskripsi yang menonjol di halaman ini. Coba telusuri tabel atau ubah filter severity untuk melihat detail audit lainnya.</div>`;
    return (
      `<section class="case-feature">` +
      `<details class="case-feature-collapsible" open>` +
      `<summary class="case-section-label case-feature-summary">` +
      `<span>Sorotan</span>` +
      `<span class="case-feature-chevron" aria-hidden="true"></span>` +
      `</summary>` +
      `<div class="case-feature-body">${body}</div>` +
      `</details>` +
      `</section>`
    );
  }

  function pickFeaturedItem(items) {
    if (!Array.isArray(items) || !items.length) return null;
    const severityRank = { absurd: 4, high: 3, med: 2, low: 1 };
    const candidates = items.filter(
      (item) =>
        item &&
        item.audit &&
        item.audit.reason &&
        String(item.audit.reason).trim().length >= 30
    );
    if (!candidates.length) return null;
    return candidates.sort(
      (a, b) =>
        (severityRank[b.audit.severity] || 0) - (severityRank[a.audit.severity] || 0)
    )[0];
  }

  function buildFeaturedReason(item) {
    if (!item) return '';
    const inaproc = buildInaprocUrl(item.sourceId);
    const linkHtml = inaproc
      ? `<a class="featured-link" href="${escapeAttr(inaproc)}" target="_blank" rel="noopener noreferrer">Lihat di inaproc.id <span aria-hidden="true">↗</span></a>`
      : '';
    return (
      `<aside class="featured-reason" aria-label="Sorotan paket prioritas">` +
      `<span class="featured-mark" aria-hidden="true">“</span>` +
      `<div class="featured-content">` +
      `<p class="featured-quote">${escapeHtml(item.audit.reason)}</p>` +
      `<div class="featured-cite">` +
      `<strong>${escapeHtml(item.packageName)}</strong>` +
      `<span class="sev-b ${severityClass(item.audit.severity)}">${escapeHtml(severityLabel(item.audit.severity))}</span>` +
      (item.budget !== null && item.budget !== undefined
        ? `<span class="featured-budget">${escapeHtml(formatCurrencyLong(item.budget))}</span>`
        : '') +
      linkHtml +
      `</div></div></aside>`
    );
  }

  function renderModalState(title, message, isError) {
    setCaseTitle(title);
    dom.modalBody.innerHTML = `<div class="modal-state${isError ? ' error' : ''}">${escapeHtml(message)}</div>`;
  }

  function renderBootstrapLoading() {
    renderKpiCards([
      { label: 'Potensi Pemborosan', value: '…', sublabel: 'Menghitung agregat' },
      { label: 'Paket Teraudit',     value: '…', sublabel: 'Memuat daftar wilayah' },
      { label: 'Pagu',               value: '…', sublabel: 'Menyiapkan peta' },
    ]);
    renderSidebarMessage('Memuat audit pengadaan per wilayah…', false);
    setMapStatus('Memuat peta audit…', false);
  }

  function renderBootstrapError(error) {
    renderKpiCards([
      { label: 'Potensi Pemborosan', value: 'Belum tersedia', sublabel: 'Backend belum siap' },
      { label: 'Paket Teraudit',     value: 'Belum tersedia', sublabel: 'Periksa ingest hasil analyze' },
      { label: 'Pagu',               value: 'Belum tersedia', sublabel: 'Ulangi db:reset bila perlu' },
    ]);
    renderSidebarMessage(`Gagal memuat dashboard audit: ${error}`, true);
    setMapStatus(`Gagal memuat dashboard audit: ${error}`, true);
  }

  function renderListCount(count) {
    const el = document.getElementById('listCount');
    if (!el) return;
    if (typeof count !== 'number') {
      el.textContent = '';
      return;
    }
    el.textContent = `· ${formatNumber(count)} entri`;
  }

  function formatFetchError(error) {
    return error instanceof Error ? error.message : String(error);
  }

  async function fetchJson(path) {
    const response = await fetch(`${API_BASE_URL}${path}`);
    const text = await response.text();
    let payload = null;
    if (text) {
      try {
        payload = JSON.parse(text);
      } catch {
        throw new Error(`Invalid JSON response from ${path}`);
      }
    }
    if (!response.ok) {
      throw new Error(
        payload && payload.error ? payload.error : `Request failed (${response.status})`
      );
    }
    return payload;
  }

  function normalizeDashboardData(payload) {
    if (!payload || typeof payload !== 'object') {
      throw new Error('Bootstrap payload tidak valid.');
    }

    return {
      summary: payload.summary || {
        totalPackages: 0,
        totalPriorityPackages: 0,
        totalPotentialWaste: 0,
        totalBudget: 0,
        unmappedPackages: 0,
        multiLocationPackages: 0,
      },
      legend: applyChoroplethPalette(payload.legend || { zeroColor: CHOROPLETH_ZERO_COLOR, ranges: [] }),
      geo: payload.geo || { type: 'FeatureCollection', features: [] },
      regions: Array.isArray(payload.regions) ? payload.regions : [],
      provinceView: {
        legend: applyChoroplethPalette(
          (payload.provinceView && payload.provinceView.legend) || {
            zeroColor: CHOROPLETH_ZERO_COLOR,
            ranges: [],
          }
        ),
        geo: (payload.provinceView && payload.provinceView.geo) || {
          type: 'FeatureCollection',
          features: [],
        },
        provinces:
          payload.provinceView && Array.isArray(payload.provinceView.provinces)
            ? payload.provinceView.provinces
            : [],
      },
      ownerLists: {
        central:
          payload.ownerLists && Array.isArray(payload.ownerLists.central)
            ? payload.ownerLists.central
            : [],
      },
    };
  }

  function getLegendColor(value) {
    const legend = getActiveLegend();

    if (!legend) {
      return CHOROPLETH_ZERO_COLOR;
    }

    if (!value || value <= 0) {
      return legend.zeroColor || CHOROPLETH_ZERO_COLOR;
    }

    const range = (legend.ranges || []).find((item) => value >= item.min && value <= item.max);
    return range
      ? range.color
      : legend.ranges[legend.ranges.length - 1]?.color || CHOROPLETH_PALETTE[CHOROPLETH_PALETTE.length - 1];
  }

  function areaMatchesCurrentView(area) {
    if (!area) {
      return false;
    }

    if (isProvinceView()) {
      return area.totalPackages > 0;
    }

    if (state.tab === 'kabupaten' && area.regionType !== 'Kabupaten') {
      return false;
    }

    if (state.tab === 'kota' && area.regionType !== 'Kota') {
      return false;
    }

    if (FILTERS.some((filter) => filter.key === state.mapFilter)) {
      return ownerTypeCount(area, state.mapFilter) > 0;
    }

    return true;
  }

  function getFilteredAreasForSidebar() {
    let areas = getActiveAreas().filter((area) => areaMatchesCurrentView(area));

    if (state.search) {
      const query = state.search.toLowerCase();
      const activeOwnerQuery = activeSidebarOwnerLabel().toLowerCase();
      areas = areas.filter((area) => {
        const matchesName =
          area.displayName.toLowerCase().includes(query) ||
          area.provinceName.toLowerCase().includes(query);

        if (isProvinceView()) {
          return matchesName;
        }

        return matchesName || activeOwnerQuery.includes(query);
      });
    }

    const metricsByAreaKey = new Map(
      areas.map((area) => [getAreaKey(area), getSidebarAreaMetrics(area)])
    );
    const sorters = {
      waste: (left, right) =>
        metricsByAreaKey.get(getAreaKey(right)).totalPotentialWaste -
        metricsByAreaKey.get(getAreaKey(left)).totalPotentialWaste,
      priority: (left, right) =>
        metricsByAreaKey.get(getAreaKey(right)).totalPriorityPackages -
        metricsByAreaKey.get(getAreaKey(left)).totalPriorityPackages,
      packages: (left, right) =>
        metricsByAreaKey.get(getAreaKey(right)).totalPackages -
        metricsByAreaKey.get(getAreaKey(left)).totalPackages,
      budget: (left, right) =>
        metricsByAreaKey.get(getAreaKey(right)).totalBudget -
        metricsByAreaKey.get(getAreaKey(left)).totalBudget,
    };

    return areas.sort((left, right) => {
      const primary = (sorters[state.sortBy] || sorters.waste)(left, right);
      return primary !== 0 ? primary : left.displayName.localeCompare(right.displayName, 'id');
    });
  }

  function getFilteredOwnersForSidebar() {
    let owners = getCentralOwnersForSidebar().slice();

    if (state.search) {
      const query = state.search.toLowerCase();
      owners = owners.filter((owner) => owner.ownerName.toLowerCase().includes(query));
    }

    const sorters = {
      waste: (left, right) => right.totalPotentialWaste - left.totalPotentialWaste,
      priority: (left, right) => right.totalPriorityPackages - left.totalPriorityPackages,
      packages: (left, right) => right.totalPackages - left.totalPackages,
      budget: (left, right) => right.totalBudget - left.totalBudget,
    };

    return owners.sort((left, right) => {
      const primary = (sorters[state.sortBy] || sorters.waste)(left, right);
      return primary !== 0 ? primary : left.ownerName.localeCompare(right.ownerName, 'id');
    });
  }

  function renderKpis() {
    const summary = dashboardData.summary;

    renderKpiCards([
      {
        label: 'Potensi Pemborosan Nasional',
        value: `Rp ${formatCompactCurrency(summary.totalPotentialWaste)}`,
        sublabel: `${formatNumber(summary.totalPriorityPackages)} paket prioritas`,
      },
      {
        label: 'Paket Teraudit',
        value: formatNumber(summary.totalPackages),
        sublabel: `${formatNumber(summary.unmappedPackages)} tidak terpetakan`,
      },
      {
        label: 'Pagu',
        value: `Rp ${formatCompactCurrency(summary.totalBudget)}`,
        sublabel: 'Akumulasi seluruh artifact',
      },
    ]);
  }

  function renderLegend() {
    if (state.isLegendHidden) {
      dom.legend.style.padding = '6px 10px';
      dom.legend.innerHTML = `<button type="button" class="lt legend-toggle-btn" onclick="${actionCall('toggleLegend')}">Tampilkan legenda</button>`;
      return;
    }

    dom.legend.style.padding = '';
    const legend = getActiveLegend();
    const title = isProvinceView()
      ? 'Potensi Pemborosan Paket Pemprov per Provinsi'
      : 'Potensi Pemborosan per Kab/Kota';
    const zeroLabel = isProvinceView()
      ? 'Tidak ada paket pemprov terdeteksi'
      : 'Tidak ada potensi terdeteksi';
    const note = isProvinceView()
      ? 'Agregasi provinsi mendeduplikasi paket multi-kab/kota di provinsi yang sama.'
      : 'Peta wilayah menghitung penuh paket multi-lokasi, sehingga agregat wilayah bisa lebih besar dari KPI nasional.';
    const rows = [
      `<div class="lt" style="display:flex; justify-content:space-between; align-items:center;">` +
      `<span>${escapeHtml(title)}</span>` +
      `<button onclick="${actionCall('toggleLegend')}" style="background:none;border:none;color:var(--ink-3);cursor:pointer;margin-left:8px;font-size:14px;line-height:1;padding:2px 6px;font-family:inherit;" title="Sembunyikan legenda" aria-label="Tutup legenda">×</button>` +
      `</div>`,
      `<div class="li"><div class="lsw" style="background:${escapeAttr(legend.zeroColor || CHOROPLETH_ZERO_COLOR)}"></div> ${escapeHtml(
        zeroLabel
      )}</div>`,
    ];

    (legend.ranges || []).forEach((range) => {
      rows.push(
        `<div class="li"><div class="lsw" style="background:${escapeAttr(range.color)}"></div> Rp ${escapeHtml(
          formatCompactCurrency(range.min)
        )} &ndash; Rp ${escapeHtml(formatCompactCurrency(range.max))}</div>`
      );
    });

    rows.push(`<div class="legend-note">${escapeHtml(note)}</div>`);
    dom.legend.innerHTML = rows.join('');
  }

  function renderFilterChips() {
    const html = FILTERS.map((filter) => {
      const active = filter.key === state.mapFilter;
      return `<button type="button" class="fc${active ? ' a' : ''}" role="tab" aria-selected="${active ? 'true' : 'false'}" onclick="${actionCall('setMapFilter', filter.key)}">${escapeHtml(filter.label)}</button>`;
    }).join('');
    dom.mapFilters.innerHTML = html;
    const mirror = document.getElementById('mfsb');
    if (mirror) mirror.innerHTML = html;
  }

  function renderTabs() {
    const provinceView = isProvinceView();
    const centralOwnerMode = isCentralOwnerMode();

    dom.tabs.innerHTML = TABS.map((tab) => {
      const active = provinceView || centralOwnerMode ? tab.key === 'all' : tab.key === state.tab;
      const disabled = (provinceView || centralOwnerMode) && tab.key !== 'all';

      return `<button type="button" class="stb${active ? ' a' : ''}" role="tab" aria-selected="${active ? 'true' : 'false'}"${disabled ? ' disabled aria-disabled="true"' : ''} onclick="${actionCall(
        'setTab',
        disabled ? 'all' : tab.key
      )}">${escapeHtml(tab.label)}</button>`;
    }).join('');
  }

  function sortControl() {
    const placeholder = isCentralOwnerMode()
      ? 'Cari kementerian/lembaga…'
      : isProvinceView()
        ? 'Cari provinsi…'
        : 'Cari kabupaten/kota…';

    return (
      `<div class="sw"><span class="si" aria-hidden="true">⌕</span><input id="sidebarSearch" type="search" placeholder="${escapeAttr(
        placeholder
      )}" value="${escapeAttr(state.search)}" aria-label="${escapeAttr(placeholder)}" oninput="${actionExpr('dashboardActions.setSearch(this.value)')}" /></div>` +
      `<div class="sort-bar"><label for="sidebarSort">Urutkan</label><select id="sidebarSort" onchange="${actionExpr('dashboardActions.setSort(this.value)')}" aria-label="Urutkan wilayah">` +
      `<option value="waste"${state.sortBy === 'waste' ? ' selected' : ''}>Potensi Pemborosan</option>` +
      `<option value="priority"${state.sortBy === 'priority' ? ' selected' : ''}>Paket Prioritas</option>` +
      `<option value="packages"${state.sortBy === 'packages' ? ' selected' : ''}>Total Paket</option>` +
      `<option value="budget"${state.sortBy === 'budget' ? ' selected' : ''}>Total Pagu</option>` +
      `</select></div>`
    );
  }

  function buildOwnerRow(owner, index, maxWaste) {
    const selectedClass =
      state.selectedOwnerKey === getOwnerCardKey(owner.ownerType, owner.ownerName) ? ' a' : '';
    const widthPct = Math.max(4, Math.round((owner.totalPotentialWaste / maxWaste) * 100));
    const ownerTypeShort = owner.ownerType === 'central' ? 'K/L' : ownerTypeLabel(owner.ownerType);

    return (
      `<div class="pi${selectedClass}" role="button" tabindex="0" aria-label="${escapeAttr(
        `Buka berkas ${owner.ownerName}`
      )}" onclick="${actionCall('openOwnerModal', owner.ownerName, owner.ownerType)}" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();this.click()}">` +
      `<span class="pi-num">#${formatNumber(index + 1)}</span>` +
      `<div class="pn">${escapeHtml(owner.ownerName)}</div>` +
      `<span class="tbd bc">${escapeHtml(ownerTypeShort)}</span>` +
      `<div class="pi-meta">${escapeHtml(formatNumber(owner.totalPackages))} paket &middot; ${escapeHtml(
        formatNumber(owner.totalPriorityPackages)
      )} prioritas</div>` +
      `<div class="pi-waste"><span class="ppv">Rp ${escapeHtml(
        formatCompactCurrency(owner.totalPotentialWaste)
      )}</span><span class="ppl">pemborosan</span></div>` +
      `<div class="bw"><div class="bf" style="width:${widthPct}%;background:${escapeAttr(getLegendColor(owner.totalPotentialWaste))}"></div></div>` +
      `</div>`
    );
  }

  function buildAreaRow(area, metrics, index, maxWaste) {
    const areaKey = getAreaKey(area);
    const selectedClass = state.selectedAreaKey === areaKey ? ' a' : '';
    const widthPct = Math.max(4, Math.round((metrics.totalPotentialWaste / maxWaste) * 100));

    return (
      `<div class="pi${selectedClass}" role="button" tabindex="0" aria-label="${escapeAttr(
        `Buka berkas ${area.displayName}`
      )}" onclick="${actionCall('openAreaModal', areaKey)}" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();this.click()}">` +
      `<span class="pi-num">#${formatNumber(index + 1)}</span>` +
      `<div class="pn">${escapeHtml(area.displayName)}</div>` +
      `<span class="tbd ${areaBadgeClass(area)}">${escapeHtml(areaBadgeLabel(area))}</span>` +
      `<div class="pi-meta">${escapeHtml(areaSecondaryLine(area))} &middot; ${escapeHtml(
        formatNumber(metrics.totalPackages)
      )} paket &middot; ${escapeHtml(formatNumber(metrics.totalPriorityPackages))} prioritas</div>` +
      `<div class="pi-waste"><span class="ppv">Rp ${escapeHtml(
        formatCompactCurrency(metrics.totalPotentialWaste)
      )}</span><span class="ppl">pemborosan</span></div>` +
      `<div class="bw"><div class="bf" style="width:${widthPct}%;background:${escapeAttr(getLegendColor(metrics.totalPotentialWaste))}"></div></div>` +
      `</div>`
    );
  }

  function renderSidebarControls() {
    const slot = document.getElementById('sidebarControls');
    if (!slot) return;
    slot.innerHTML = sortControl();
  }

  function renderSidebarContent(updateControls = true) {
    if (!dashboardData) {
      renderSidebarMessage('Data dashboard belum tersedia.', true);
      renderListCount(null);
      return;
    }

    if (updateControls) {
      renderSidebarControls();
    }

    dom.sidebarContent.innerHTML = '';

    let listHtml = '';
    let count = 0;

    if (isCentralOwnerMode()) {
      const owners = getFilteredOwnersForSidebar();
      count = owners.length;

      if (!owners.length) {
        listHtml = `<div class="panel-msg">Tidak ada kementerian/lembaga yang cocok dengan filter saat ini.</div>`;
      } else {
        const maxWaste = Math.max(...owners.map((owner) => owner.totalPotentialWaste), 1);
        listHtml = owners.map((owner, index) => buildOwnerRow(owner, index, maxWaste)).join('');
      }
    } else {
      const areas = getFilteredAreasForSidebar();
      count = areas.length;

      if (!areas.length) {
        listHtml = `<div class="panel-msg">Tidak ada ${escapeHtml(
          isProvinceView() ? 'provinsi' : 'wilayah'
        )} yang cocok dengan filter saat ini.</div>`;
      } else {
        const areaEntries = areas.map((area) => ({
          area,
          metrics: getSidebarAreaMetrics(area),
        }));
        const maxWaste = Math.max(...areaEntries.map(({ metrics }) => metrics.totalPotentialWaste), 1);

        listHtml = areaEntries
          .map(({ area, metrics }, index) => buildAreaRow(area, metrics, index, maxWaste))
          .join('');
      }
    }

    dom.sidebarContent.innerHTML = listHtml;
    renderListCount(count);
  }

  function featureStyle(feature) {
    const areaKey = getFeatureAreaKey(feature);
    const area = getActiveAreaByKey(areaKey);
    const visible = areaMatchesCurrentView(area);
    const selected = state.selectedAreaKey === areaKey;
    const strokeOpacity = (selected ? 1 : 0.22) * (visible ? 0.9 : 0.22);

    return {
      fillColor: area ? getLegendColor(area.totalPotentialWaste) : CHOROPLETH_ZERO_COLOR,
      fillOpacity: selected ? 0.85 : visible ? 0.7 : 0.18,
      strokeColor: selected ? MAP_STROKE_SELECTED : MAP_STROKE_DEFAULT,
      strokeWidth: selected ? 2.2 : 0.7,
      strokeOpacity,
    };
  }

  function popupHtml(area) {
    if (!area) {
      return `<div class="pt">Belum ada data</div>`;
    }

    if (isProvinceView()) {
      return (
        `<div class="pt">${escapeHtml(area.displayName)}</div>` +
        `<div class="popup-sub">Paket Pemprov</div>` +
        `<div class="pr"><span class="l">Potensi Pemborosan</span><span class="v popup-waste-val">Rp ${escapeHtml(
          formatCompactCurrency(area.totalPotentialWaste)
        )}</span></div>` +
        `<div class="pr"><span class="l">Paket Prioritas</span><span class="v">${escapeHtml(
          formatNumber(area.totalPriorityPackages)
        )}</span></div>` +
        `<div class="pr"><span class="l">Total Paket</span><span class="v">${escapeHtml(
          formatNumber(area.totalPackages)
        )}</span></div>` +
        `<div class="pr"><span class="l">Total Pagu</span><span class="v">${escapeHtml(
          formatCompactCurrency(area.totalBudget)
        )}</span></div>` +
        `<div class="pr"><span class="l">Severity High</span><span class="v">${escapeHtml(
          formatNumber(area.severityCounts.high)
        )}</span></div>` +
        `<div class="ppb"><div class="ppbf" style="width:${Math.min(
          100,
          area.totalPriorityPackages > 0
            ? Math.round((area.totalPriorityPackages / Math.max(area.totalPackages, 1)) * 100)
            : 0
        )}%;background:${escapeAttr(getLegendColor(area.totalPotentialWaste))}"></div></div>`
      );
    }

    return (
      `<div class="pt">${escapeHtml(area.displayName)}</div>` +
      `<div class="popup-sub">${escapeHtml(area.provinceName)}</div>` +
      `<div class="pr"><span class="l">Potensi Pemborosan</span><span class="v popup-waste-val">Rp ${escapeHtml(
        formatCompactCurrency(area.totalPotentialWaste)
      )}</span></div>` +
      `<div class="pr"><span class="l">Paket Prioritas</span><span class="v">${escapeHtml(
        formatNumber(area.totalPriorityPackages)
      )}</span></div>` +
      `<div class="pr"><span class="l">Total Paket</span><span class="v">${escapeHtml(
        formatNumber(area.totalPackages)
      )}</span></div>` +
      `<div class="pr"><span class="l">Kementerian/Lembaga</span><span class="v">${escapeHtml(
        formatNumber(ownerTypeCount(area, 'central'))
      )}</span></div>` +
      `<div class="pr"><span class="l">Pemprov</span><span class="v">${escapeHtml(
        formatNumber(ownerTypeCount(area, 'provinsi'))
      )}</span></div>` +
      `<div class="pr"><span class="l">Pemkot</span><span class="v">${escapeHtml(
        formatNumber(ownerTypeCount(area, 'kabkota'))
      )}</span></div>` +
      `<div class="pr"><span class="l">Others</span><span class="v">${escapeHtml(
        formatNumber(ownerTypeCount(area, 'other'))
      )}</span></div>` +
      `<div class="ppb"><div class="ppbf" style="width:${Math.min(
        100,
        area.totalPriorityPackages > 0
          ? Math.round((area.totalPriorityPackages / Math.max(area.totalPackages, 1)) * 100)
          : 0
      )}%;background:${escapeAttr(getLegendColor(area.totalPotentialWaste))}"></div></div>`
    );
  }

  function renderGeoLayer(fitToBounds) {
    const geo = getActiveGeo();

    if (!geo || !Array.isArray(geo.features) || !geo.features.length) {
      setMapStatus('Tidak ada geometri untuk mode peta saat ini.', true);
      return;
    }

    window['AuditMap'].render(
      dom.mapRoot,
      geo,
      {
        getFeatureStyle: featureStyle,
        getPopupHtml: (areaKey) => popupHtml(getActiveAreaByKey(areaKey)),
        onAreaClick: openAreaModal,
        fitBounds: fitToBounds,
        isProvinceView: isProvinceView(),
      },
      clearMapStatus
    );
  }

  function initMap() {
    renderGeoLayer(true);
  }

  function refreshMapStyles() {
    window['AuditMap'].refresh(getActiveGeo(), featureStyle);
  }

  function renderPackageTableRows(items) {
    return items.length
      ? items
        .map((item) => {
          const packageUrl = buildInaprocUrl(item.sourceId);

          return (
            `<tr${packageUrl
              ? ` class="package-row-link" tabindex="0" role="link" aria-label="${escapeAttr(
                `Buka ${item.packageName} di Inaproc`
              )}" onclick="${actionCall('openPackageDetail', item.sourceId)}" onkeydown="${actionExpr(
                `dashboardActions.handlePackageRowKeydown(event, ${jsArg(item.sourceId)})`
              )}"`
              : ''
            }>` +
            `<td class="mono"><span class="id-cell">${escapeHtml(String(item.sourceId || item.id))}<button type="button" class="copy-id-btn" onclick="event.stopPropagation();dashboardActions.copySourceId(${jsArg(String(item.sourceId || item.id))}, this)" aria-label="Salin ID paket" title="Salin ID">⧉</button></span></td>` +
            `<td class="pkg">${escapeHtml(item.packageName)}</td>` +
            `<td><div class="tbl-owner">${escapeHtml(item.ownerName)}</div><div class="tbl-sub">${escapeHtml(
              ownerTypeLabel(item.ownerType)
            )}</div></td>` +
            `<td><div class="tbl-owner">${escapeHtml(item.satker || '-')}</div><div class="tbl-sub">${escapeHtml(
              item.locationRaw || '-'
            )}</div></td>` +
            `<td class="mono col-pagu">${escapeHtml(item.budget === null ? '-' : formatCurrencyLong(item.budget))}</td>` +
            `<td><span class="sev-b ${severityClass(item.audit.severity)}">${escapeHtml(
              severityLabel(item.audit.severity)
            )}</span></td>` +
            `<td class="reason">${escapeHtml(item.audit.reason || '-')}</td>` +
            `</tr>`
          );
        })
        .join('')
      : `<tr><td colspan="7" class="table-empty">Tidak ada paket untuk filter saat ini.</td></tr>`;
  }

  function renderPagination(pagination) {
    const pageSize = pagination.pageSize || state.modal.pageSize || 25;
    const pageSizeOptions = [25, 50, 100, 250]
      .map((n) => `<option value="${n}"${n === pageSize ? ' selected' : ''}>${n}</option>`)
      .join('');

    return (
      `<div class="pager">` +
      `<button class="pager-btn" ${pagination.page <= 1 ? 'disabled' : ''} onclick="${actionCall(
        'changeModalPage',
        pagination.page - 1
      )}">← Sebelumnya</button>` +
      `<div class="pager-info">` +
      `<label class="pager-jump"><span>Halaman</span>` +
      `<input type="number" min="1" max="${pagination.totalPages}" value="${pagination.page}" aria-label="Lompat ke halaman" onchange="${actionExpr('dashboardActions.jumpToPage(parseInt(this.value, 10))')}" />` +
      `<span>/ ${escapeHtml(formatNumber(pagination.totalPages))}</span>` +
      `</label>` +
      `<span class="pager-count">${escapeHtml(formatNumber(pagination.totalItems))} paket</span>` +
      `<label class="pager-size"><span>Per halaman</span>` +
      `<select aria-label="Jumlah baris per halaman" onchange="${actionExpr('dashboardActions.setModalPageSize(parseInt(this.value, 10))')}">${pageSizeOptions}</select>` +
      `</label>` +
      `</div>` +
      `<button class="pager-btn" ${pagination.page >= pagination.totalPages ? 'disabled' : ''} onclick="${actionCall(
        'changeModalPage',
        pagination.page + 1
      )}">Berikutnya →</button>` +
      `</div>`
    );
  }

  function renderRegionModalContent(payload) {
    const region = payload.region;
    const rowsHtml = renderPackageTableRows(payload.items);
    const featured = pickFeaturedItem(payload.items);

    setBreadcrumb(`${region.provinceName} · ${region.displayName}`);
    setPageMeta(
      `${region.displayName} · Audit Pengadaan TA 2026 · Nemesis`,
      `Berkas audit ${region.displayName}, ${region.provinceName}: ${formatNumber(region.totalPriorityPackages)} paket prioritas, potensi pemborosan Rp ${formatCompactCurrency(region.totalPotentialWaste)}.`
    );

    setCaseTitle(region.displayName);

    const ownerCounts = {
      central: ownerTypeCount(region, 'central'),
      provinsi: ownerTypeCount(region, 'provinsi'),
      kabkota: ownerTypeCount(region, 'kabkota'),
      other: ownerTypeCount(region, 'other'),
    };

    dom.modalBody.innerHTML =
      `<div class="case-grid">` +
      buildCaseFeature(featured) +
      buildCaseSummary({
        heroLabel: 'Potensi Pemborosan',
        heroValue: `Rp ${formatCompactCurrency(region.totalPotentialWaste)}`,
        heroSub: `${formatNumber(region.totalPriorityPackages)} paket prioritas`,
        secondary: [
          { label: 'Total Paket',   value: formatNumber(region.totalPackages) },
          { label: 'Pagu Teraudit', value: `Rp ${formatCompactCurrency(region.totalBudget)}` },
        ],
        sections: [
          {
            title: 'Pemilik',
            items: [
              { label: 'Kementerian/Lembaga', value: formatNumber(ownerCounts.central),  muted: ownerCounts.central === 0 },
              { label: 'Pemprov',              value: formatNumber(ownerCounts.provinsi), muted: ownerCounts.provinsi === 0 },
              { label: 'Pemkot',               value: formatNumber(ownerCounts.kabkota),  muted: ownerCounts.kabkota === 0 },
              { label: 'Others',               value: formatNumber(ownerCounts.other),    muted: ownerCounts.other === 0 },
            ],
          },
          {
            title: 'Severity',
            items: [
              { label: 'High',   value: formatNumber(region.severityCounts.high),   muted: region.severityCounts.high === 0 },
              { label: 'Absurd', value: formatNumber(region.severityCounts.absurd), muted: region.severityCounts.absurd === 0 },
            ],
          },
        ],
      }) +
      `</div>` +
      `<div class="modal-filters">` +
      `<input id="modalSearch" type="search" placeholder="Cari paket, lembaga, atau satker…" value="${escapeAttr(
        state.modal.search
      )}" aria-label="Cari paket, lembaga, atau satker" oninput="${actionExpr('dashboardActions.setModalSearch(this.value)')}" />` +
      `<select aria-label="Filter berdasarkan severity" onchange="${actionExpr('dashboardActions.setModalSeverityFilter(this.value)')}">${renderSeverityFilterOptions(
        currentSeverityFilterValue()
      )}</select>` +
      `</div>` +
      `<div class="modal-cnt">Menampilkan ${escapeHtml(formatNumber(payload.items.length))} dari ${escapeHtml(
        formatNumber(payload.pagination.totalItems)
      )} paket pada wilayah ini</div>` +
      `<div class="table-wrap" tabindex="0" role="region" aria-label="Tabel paket"><table class="rtbl"><thead><tr><th>ID</th><th>Nama Paket</th><th>Pemilik</th><th>Satker / Lokasi</th><th>Pagu</th><th>Severity</th><th>Alasan</th></tr></thead><tbody>${rowsHtml}</tbody></table></div>` +
      renderPagination(payload.pagination);
  }

  function renderProvinceModalContent(payload) {
    const province = payload.province;
    const rowsHtml = renderPackageTableRows(payload.items);
    const featured = pickFeaturedItem(payload.items);

    setBreadcrumb(`Provinsi · ${province.displayName}`);
    setPageMeta(
      `${province.displayName} · Audit Pengadaan Pemprov · Nemesis`,
      `Berkas audit pemprov ${province.displayName}: ${formatNumber(province.totalPriorityPackages)} paket prioritas, potensi pemborosan Rp ${formatCompactCurrency(province.totalPotentialWaste)}.`
    );

    setCaseTitle(province.displayName);

    dom.modalBody.innerHTML =
      `<div class="case-grid">` +
      buildCaseFeature(featured) +
      buildCaseSummary({
        heroLabel: 'Potensi Pemborosan',
        heroValue: `Rp ${formatCompactCurrency(province.totalPotentialWaste)}`,
        heroSub: `${formatNumber(province.totalPriorityPackages)} paket prioritas`,
        secondary: [
          { label: 'Paket Pemprov', value: formatNumber(province.totalPackages) },
          { label: 'Pagu Teraudit', value: `Rp ${formatCompactCurrency(province.totalBudget)}` },
        ],
        sections: [
          {
            title: 'Severity',
            items: [
              { label: 'Medium', value: formatNumber(province.severityCounts.med),    muted: province.severityCounts.med === 0 },
              { label: 'High',   value: formatNumber(province.severityCounts.high),   muted: province.severityCounts.high === 0 },
              { label: 'Absurd', value: formatNumber(province.severityCounts.absurd), muted: province.severityCounts.absurd === 0 },
            ],
          },
          {
            title: 'Risk Score',
            items: [
              { label: 'Rata-rata', value: formatDecimal(province.avgRiskScore) },
              { label: 'Maksimum',  value: formatNumber(province.maxRiskScore) },
              { label: 'Flagged',   value: formatNumber(province.totalFlaggedPackages) },
            ],
          },
        ],
      }) +
      `</div>` +
      `<div class="modal-filters">` +
      `<input id="modalSearch" type="search" placeholder="Cari paket, lembaga, atau satker…" value="${escapeAttr(
        state.modal.search
      )}" aria-label="Cari paket, lembaga, atau satker" oninput="${actionExpr('dashboardActions.setModalSearch(this.value)')}" />` +
      `<select aria-label="Filter berdasarkan severity" onchange="${actionExpr('dashboardActions.setModalSeverityFilter(this.value)')}">${renderSeverityFilterOptions(
        currentSeverityFilterValue()
      )}</select>` +
      `</div>` +
      `<div class="modal-cnt">Menampilkan ${escapeHtml(formatNumber(payload.items.length))} dari ${escapeHtml(
        formatNumber(payload.pagination.totalItems)
      )} paket pemprov pada provinsi ini</div>` +
      `<div class="table-wrap" tabindex="0" role="region" aria-label="Tabel paket"><table class="rtbl"><thead><tr><th>ID</th><th>Nama Paket</th><th>Pemilik</th><th>Satker / Lokasi</th><th>Pagu</th><th>Severity</th><th>Alasan</th></tr></thead><tbody>${rowsHtml}</tbody></table></div>` +
      renderPagination(payload.pagination);
  }

  function renderOwnerModalContent(payload) {
    const owner = payload.owner;
    const rowsHtml = renderPackageTableRows(payload.items);
    const featured = pickFeaturedItem(payload.items);

    setBreadcrumb(`${ownerTypeLabel(owner.ownerType)} · ${owner.ownerName}`);
    setPageMeta(
      `${owner.ownerName} · Audit Pengadaan TA 2026 · Nemesis`,
      `Berkas audit ${owner.ownerName}: ${formatNumber(owner.totalPriorityPackages)} paket prioritas, potensi pemborosan Rp ${formatCompactCurrency(owner.totalPotentialWaste)}.`
    );

    setCaseTitle(owner.ownerName);

    dom.modalBody.innerHTML =
      `<div class="case-grid">` +
      buildCaseFeature(featured) +
      buildCaseSummary({
        heroLabel: 'Potensi Pemborosan',
        heroValue: `Rp ${formatCompactCurrency(owner.totalPotentialWaste)}`,
        heroSub: `${formatNumber(owner.totalPriorityPackages)} paket prioritas`,
        secondary: [
          { label: 'Total Paket',   value: formatNumber(owner.totalPackages) },
          { label: 'Pagu Teraudit', value: `Rp ${formatCompactCurrency(owner.totalBudget)}` },
          { label: 'Paket Flagged', value: formatNumber(owner.totalFlaggedPackages) },
        ],
        sections: [
          {
            title: 'Severity',
            items: [
              { label: 'Medium', value: formatNumber(owner.severityCounts.med),    muted: owner.severityCounts.med === 0 },
              { label: 'High',   value: formatNumber(owner.severityCounts.high),   muted: owner.severityCounts.high === 0 },
              { label: 'Absurd', value: formatNumber(owner.severityCounts.absurd), muted: owner.severityCounts.absurd === 0 },
            ],
          },
        ],
      }) +
      `</div>` +
      `<div class="modal-filters">` +
      `<input id="modalSearch" type="search" placeholder="Cari paket atau satker…" value="${escapeAttr(
        state.modal.search
      )}" aria-label="Cari paket atau satker" oninput="${actionExpr('dashboardActions.setModalSearch(this.value)')}" />` +
      `<select aria-label="Filter berdasarkan severity" onchange="${actionExpr('dashboardActions.setModalSeverityFilter(this.value)')}">${renderSeverityFilterOptions(
        currentSeverityFilterValue()
      )}</select>` +
      `</div>` +
      `<div class="modal-cnt">Menampilkan ${escapeHtml(formatNumber(payload.items.length))} dari ${escapeHtml(
        formatNumber(payload.pagination.totalItems)
      )} paket pada pemilik ini</div>` +
      `<div class="table-wrap" tabindex="0" role="region" aria-label="Tabel paket"><table class="rtbl"><thead><tr><th>ID</th><th>Nama Paket</th><th>Pemilik</th><th>Satker / Lokasi</th><th>Pagu</th><th>Severity</th><th>Alasan</th></tr></thead><tbody>${rowsHtml}</tbody></table></div>` +
      renderPagination(payload.pagination);
  }

  function setupTableOverflowAffordance(wrap) {
    if (!(wrap instanceof HTMLElement)) return;
    const update = () => {
      const overflowing = wrap.scrollWidth > wrap.clientWidth + 1;
      const atEnd = wrap.scrollLeft + wrap.clientWidth >= wrap.scrollWidth - 1;
      wrap.classList.toggle('has-overflow', overflowing && !atEnd);
    };
    wrap.addEventListener('scroll', update, { passive: true });
    update();
    // Re-check after fonts/layout settle
    requestAnimationFrame(update);
  }

  function renderModalContent(payload) {
    state.modal.totalPages = payload.pagination.totalPages;

    if (state.modal.areaType === 'owner') {
      renderOwnerModalContent(payload);
    } else if (state.modal.areaType === 'province') {
      renderProvinceModalContent(payload);
    } else {
      renderRegionModalContent(payload);
    }

    dom.modalBody.querySelectorAll('.table-wrap').forEach(setupTableOverflowAffordance);

    if (typeof state.modal.searchSelection === 'number') {
      const newEl = document.getElementById('modalSearch');
      if (newEl instanceof HTMLInputElement) {
        newEl.focus();
        try { newEl.setSelectionRange(state.modal.searchSelection, state.modal.searchSelection); } catch(e){}
      }
      state.modal.searchSelection = null;
    }
  }

  async function loadAreaPackages({ silent = false } = {}) {
    if (
      (state.modal.areaType === 'owner' && (!state.modal.ownerType || !state.modal.ownerName)) ||
      (state.modal.areaType !== 'owner' && !state.modal.areaKey)
    ) {
      return;
    }

    state.modalRequestId += 1;
    const requestId = state.modalRequestId;
    // Only show the full-screen "Memuat…" splash on the very first fetch.
    // For subsequent search / filter / paginate calls, keep the existing
    // table visible — the search input's `is-searching` class already
    // gives subtle visual feedback while the request is in flight.
    if (!silent) {
      renderModalState(
        state.modal.areaType === 'owner' ? 'Memuat pemilik…' : 'Memuat wilayah…',
        state.modal.areaType === 'owner'
          ? 'Mengambil paket dari pemilik terpilih…'
          : 'Mengambil paket dari backend audit…',
        false
      );
    }

    const params = new URLSearchParams({
      page: String(state.modal.page),
      pageSize: String(state.modal.pageSize),
    });

    if (state.modal.search) {
      params.set('search', state.modal.search);
    }

    if (state.modal.areaType === 'region' && state.modal.ownerType) {
      params.set('ownerType', state.modal.ownerType);
    }

    if (state.modal.severity) {
      params.set('severity', state.modal.severity);
    }

    if (state.modal.priorityOnly) {
      params.set('priorityOnly', 'true');
    }

    const path =
      state.modal.areaType === 'owner'
        ? (() => {
          params.set('ownerType', state.modal.ownerType);
          params.set('ownerName', state.modal.ownerName);
          return `/owners/packages?${params.toString()}`;
        })()
        : state.modal.areaType === 'province'
          ? `/provinces/${encodeURIComponent(state.modal.areaKey)}/packages?${params.toString()}`
          : `/regions/${encodeURIComponent(state.modal.areaKey)}/packages?${params.toString()}`;

    try {
      const payload = await fetchJson(path);

      if (requestId !== state.modalRequestId) {
        return;
      }

      renderModalContent(payload);
    } catch (error) {
      if (requestId !== state.modalRequestId) {
        return;
      }

      renderModalState('Gagal memuat paket', formatFetchError(error), true);
    }
  }

  let lastFocusedBeforeCaseFile = null;
  let suppressPopstate = false;

  // ─── Path-based routing for shareable case-file pages ──────────
  function buildCaseFilePath() {
    if (state.modal.areaType === 'owner' && state.modal.ownerType && state.modal.ownerName) {
      return `/owner/${encodeURIComponent(state.modal.ownerType)}/${encodeURIComponent(state.modal.ownerName)}`;
    }
    if (state.modal.areaType === 'province' && state.modal.areaKey) {
      return `/provinsi/${encodeURIComponent(state.modal.areaKey)}`;
    }
    if (state.modal.areaType === 'region' && state.modal.areaKey) {
      return `/wilayah/${encodeURIComponent(state.modal.areaKey)}`;
    }
    return '/';
  }

  function pushCaseFilePath() {
    const newPath = buildCaseFilePath();
    if (!newPath || newPath === location.pathname) return;
    suppressPopstate = true;
    history.pushState({ view: 'casefile' }, '', newPath);
    setTimeout(() => { suppressPopstate = false; }, 50);
  }

  function navigateHome() {
    if (location.pathname === '/' || location.pathname === '') {
      // Already home, just sync view
      setView('dashboard');
      return;
    }
    suppressPopstate = true;
    history.pushState({ view: 'dashboard' }, '', '/');
    setTimeout(() => { suppressPopstate = false; }, 50);
    closeCaseFile();
  }

  function parsePath() {
    const path = location.pathname || '/';
    if (path === '/' || path === '') return null;
    const parts = path.split('/').filter(Boolean).map((p) => {
      try { return decodeURIComponent(p); } catch { return p; }
    });
    if (parts[0] === 'wilayah' && parts[1]) {
      return { kind: 'region', areaKey: parts[1] };
    }
    if (parts[0] === 'provinsi' && parts[1]) {
      return { kind: 'province', areaKey: parts[1] };
    }
    if (parts[0] === 'owner' && parts[1] && parts[2]) {
      return { kind: 'owner', ownerType: parts[1], ownerName: parts[2] };
    }
    return null;
  }

  function syncFromPath() {
    if (suppressPopstate) return;
    const target = parsePath();
    if (!target) {
      if (state.viewMode === 'casefile') closeCaseFile({ skipNav: true });
      return;
    }
    // Set state and load appropriate detail
    if (target.kind === 'owner') {
      if (state.mapFilter !== target.ownerType) state.mapFilter = 'central';
      openOwnerModal(target.ownerName, target.ownerType, { skipNav: true });
    } else if (target.kind === 'province') {
      if (state.mapFilter !== 'provinsi') {
        state.mapFilter = 'provinsi';
        renderFilterChips();
        renderTabs();
        renderSidebarContent();
        renderGeoLayer(false);
      }
      openAreaModal(target.areaKey, { skipNav: true });
    } else if (target.kind === 'region') {
      if (state.mapFilter === 'provinsi' || state.mapFilter === 'central') {
        state.mapFilter = 'kabkota';
        renderFilterChips();
        renderTabs();
        renderSidebarContent();
        renderGeoLayer(false);
      }
      openAreaModal(target.areaKey, { skipNav: true });
    }
  }

  function setView(view) {
    const previous = state.viewMode;
    state.viewMode = view;
    const root = document.getElementById('preact-wrapper');
    if (root) root.dataset.view = view;
    if (view !== previous) {
      window.scrollTo(0, 0);
    }
  }

  function setBreadcrumb(label) {
    const el = document.getElementById('breadcrumbHere');
    if (el) el.textContent = label || 'Berkas';
  }

  function setPageMeta(title, description) {
    if (typeof document !== 'undefined') {
      document.title = title;
      const m = document.querySelector('meta[name="description"]');
      if (m && description) m.setAttribute('content', description);
    }
  }

  function openCaseFileShell() {
    lastFocusedBeforeCaseFile =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    setView('casefile');
    requestAnimationFrame(() => {
      const back = document.querySelector('.breadcrumb-back');
      if (back instanceof HTMLElement) back.focus();
    });
  }

  function closeCaseFile(opts = {}) {
    state.modalRequestId += 1;
    state.modal = {
      areaType: currentAreaType(),
      areaKey: null,
      ownerName: '',
      page: 1,
      pageSize: 25,
      search: '',
      ownerType: '',
      severity: '',
      priorityOnly: false,
    };
    setView('dashboard');
    setBreadcrumb('Berkas');
    setPageMeta(
      'Nemesis · Audit Pengadaan Nasional · TA 2026',
      'Berkas perkara publik atas anomali pengadaan barang/jasa pemerintah Indonesia. Operasi Diponegoro · Abil Sudarman School of AI.'
    );
    if (lastFocusedBeforeCaseFile && document.contains(lastFocusedBeforeCaseFile)) {
      lastFocusedBeforeCaseFile.focus();
    }
    lastFocusedBeforeCaseFile = null;
    if (!opts.skipNav && location.pathname !== '/') {
      suppressPopstate = true;
      history.pushState({ view: 'dashboard' }, '', '/');
      setTimeout(() => { suppressPopstate = false; }, 50);
    }
  }

  function openAreaModal(areaKey, opts = {}) {
    if (window['AuditMap']) window['AuditMap'].closePopup();
    state.selectedAreaKey = areaKey;
    state.selectedOwnerKey = null;
    state.modal = {
      areaType: currentAreaType(),
      areaKey,
      ownerName: '',
      page: 1,
      pageSize: 25,
      search: '',
      ownerType: '',
      severity: '',
      priorityOnly: false,
    };

    refreshMapStyles();
    renderSidebarContent();
    if (!opts.skipNav) pushCaseFilePath();
    openCaseFileShell();
    loadAreaPackages();
  }

  function openOwnerModal(ownerName, ownerType, opts = {}) {
    if (window['AuditMap']) window['AuditMap'].closePopup();
    state.selectedAreaKey = null;
    state.selectedOwnerKey = getOwnerCardKey(ownerType, ownerName);
    state.modal = {
      areaType: 'owner',
      areaKey: null,
      ownerName,
      page: 1,
      pageSize: 25,
      search: '',
      ownerType,
      severity: '',
      priorityOnly: false,
    };

    refreshMapStyles();
    renderSidebarContent();
    if (!opts.skipNav) pushCaseFilePath();
    openCaseFileShell();
    loadAreaPackages();
  }

  function closeRegionModal() {
    closeCaseFile();
  }

  let sidebarSearchTimeout = null;

  function setSearch(value) {
    state.search = value;
    // Debounce client-side filter/sort. Even though it's local,
    // re-rendering the full list on every keystroke causes layout
    // jank on long lists / slower devices.
    if (sidebarSearchTimeout) clearTimeout(sidebarSearchTimeout);
    sidebarSearchTimeout = setTimeout(() => {
      renderSidebarContent(false);
    }, 180);
  }

  function setSort(value) {
    state.sortBy = value;
    renderSidebarContent(true);
  }

  function setTab(value) {
    if (isProvinceView() || isCentralOwnerMode()) {
      state.tab = 'all';
      renderTabs();
      return;
    }

    state.tab = value;
    refreshMapStyles();
    renderTabs();
    renderSidebarContent();
  }

  function setMapFilter(value) {
    const wasProvinceView = isProvinceView();
    const wasCentralOwnerMode = isCentralOwnerMode();
    state.mapFilter = value;
    const viewChanged = wasProvinceView !== isProvinceView();
    const centralOwnerModeChanged = wasCentralOwnerMode !== isCentralOwnerMode();

    if (viewChanged) {
      state.tab = 'all';
      state.selectedAreaKey = null;
      state.selectedOwnerKey = null;
      closeRegionModal();
      renderLegend();
      renderFilterChips();
      renderTabs();
      renderSidebarContent();
      renderGeoLayer(true);
      return;
    }

    if (centralOwnerModeChanged) {
      state.tab = 'all';
      state.selectedAreaKey = null;
      state.selectedOwnerKey = null;

      if (state.modal.areaType === 'owner' && !isCentralOwnerMode()) {
        closeRegionModal();
      }
    }

    refreshMapStyles();
    renderFilterChips();
    renderTabs();
    renderSidebarContent();
  }

  let modalSearchTimeout = null;

  function setModalSearch(value) {
    const el = document.getElementById('modalSearch');
    state.modal.searchSelection = el instanceof HTMLInputElement ? el.selectionStart : null;
    state.modal.search = value;
    state.modal.page = 1;
    // Show searching state immediately for visual feedback
    if (el instanceof HTMLElement) el.classList.add('is-searching');
    if (modalSearchTimeout) clearTimeout(modalSearchTimeout);
    modalSearchTimeout = setTimeout(() => {
      loadAreaPackages({ silent: true });
    }, 300);
  }

  function setModalSeverityFilter(value) {
    if (value === 'priority') {
      state.modal.priorityOnly = true;
      state.modal.severity = '';
    } else {
      state.modal.priorityOnly = false;
      state.modal.severity = value || '';
    }
    state.modal.page = 1;
    loadAreaPackages({ silent: true });
  }

  function changeModalPage(page) {
    state.modal.page = page;
    loadAreaPackages({ silent: true });
  }

  function jumpToPage(page) {
    const total = state.modal.totalPages || 1;
    const clamped = Math.min(Math.max(1, Math.floor(page) || 1), total);
    if (clamped === state.modal.page) return;
    state.modal.page = clamped;
    loadAreaPackages({ silent: true });
  }

  function setModalPageSize(size) {
    const validSizes = [25, 50, 100, 250];
    const next = validSizes.includes(size) ? size : 25;
    if (state.modal.pageSize === next) return;
    state.modal.pageSize = next;
    state.modal.page = 1;
    loadAreaPackages({ silent: true });
  }

  let lastFocusedBeforeMethods = null;

  function openMethods() {
    const overlay = document.getElementById('methodsOverlay');
    if (!overlay) return;
    lastFocusedBeforeMethods =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    overlay.classList.add('open');
    overlay.setAttribute('aria-hidden', 'false');
    document.body.style.overflow = 'hidden';
    requestAnimationFrame(() => {
      const closer = overlay.querySelector('.methods-close');
      if (closer instanceof HTMLElement) closer.focus();
    });
  }

  function closeMethods() {
    const overlay = document.getElementById('methodsOverlay');
    if (!overlay) return;
    overlay.classList.remove('open');
    overlay.setAttribute('aria-hidden', 'true');
    if (state.viewMode !== 'casefile') {
      document.body.style.overflow = '';
    }
    if (lastFocusedBeforeMethods && document.contains(lastFocusedBeforeMethods)) {
      lastFocusedBeforeMethods.focus();
    }
    lastFocusedBeforeMethods = null;
  }

  function populateMethodsCount() {
    const el = document.getElementById('methodsTotalPackages');
    if (!el || !dashboardData) return;
    el.textContent = formatNumber(dashboardData.summary.totalPackages || 0);
  }

  function copyShareLink() {
    const btn = document.getElementById('btnShare');
    const url = location.href;
    const flash = (label) => {
      if (!(btn instanceof HTMLElement)) return;
      const original = btn.innerHTML;
      btn.innerHTML = label;
      btn.classList.add('copied');
      setTimeout(() => {
        btn.innerHTML = original;
        btn.classList.remove('copied');
      }, 1600);
    };
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(url).then(
        () => flash('<span aria-hidden="true">✓</span> Tersalin'),
        () => flash('<span aria-hidden="true">!</span> Gagal')
      );
    } else {
      flash('<span aria-hidden="true">✓</span> ' + url);
    }
  }

  function copySourceId(sourceId, btn) {
    const text = String(sourceId || '');
    if (!text) return;
    const flash = (label) => {
      if (!(btn instanceof HTMLElement)) return;
      const original = btn.innerHTML;
      btn.innerHTML = label;
      btn.classList.add('copied');
      setTimeout(() => {
        btn.innerHTML = original;
        btn.classList.remove('copied');
      }, 1400);
    };

    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(text).then(
        () => flash('✓'),
        () => flash('!')
      );
    } else {
      // Fallback for non-secure contexts
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.position = 'absolute';
      ta.style.left = '-9999px';
      document.body.appendChild(ta);
      ta.select();
      try {
        document.execCommand('copy');
        flash('✓');
      } catch {
        flash('!');
      }
      document.body.removeChild(ta);
    }
  }

  function openPackageDetail(sourceId) {
    const url = buildInaprocUrl(sourceId);
    if (!url) {
      return;
    }

    window.open(url, '_blank', 'noopener,noreferrer');
  }

  function handlePackageRowKeydown(event, sourceId) {
    if (!event) {
      return;
    }

    if (event.key !== 'Enter' && event.key !== ' ' && event.key !== 'Spacebar') {
      return;
    }

    event.preventDefault();
    openPackageDetail(sourceId);
  }

  function bindEvents() {
    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        const methodsOverlay = document.getElementById('methodsOverlay');
        if (methodsOverlay && methodsOverlay.classList.contains('open')) {
          closeMethods();
          return;
        }
        if (state.viewMode === 'casefile') {
          navigateHome();
        }
        return;
      }
      // Pagination keyboard shortcuts when on case file and not typing
      if (state.viewMode !== 'casefile') return;
      const target = event.target;
      if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement || target instanceof HTMLSelectElement) return;
      if (event.key === 'ArrowLeft' && state.modal.page > 1) {
        event.preventDefault();
        changeModalPage(state.modal.page - 1);
      } else if (event.key === 'ArrowRight' && state.modal.totalPages && state.modal.page < state.modal.totalPages) {
        event.preventDefault();
        changeModalPage(state.modal.page + 1);
      }
    });

    const methodsOverlay = document.getElementById('methodsOverlay');
    if (methodsOverlay) {
      methodsOverlay.addEventListener('click', (event) => {
        if (event.target === methodsOverlay) closeMethods();
      });
    }

    window.addEventListener('popstate', syncFromPath);
  }

  async function bootstrap() {
    renderBootstrapLoading();

    // On phone-sized viewports, start with the map legend collapsed so
    // it doesn't cover the choropleth on first paint. Users can re-open
    // it via the "Tampilkan legenda" toggle.
    if (typeof window !== 'undefined' && window.innerWidth <= 640) {
      state.isLegendHidden = true;
    }

    try {
      dashboardData = normalizeDashboardData(await fetchJson('/bootstrap'));
      regionsByKey = new Map(dashboardData.regions.map((region) => [region.regionKey, region]));
      provincesByKey = new Map(
        dashboardData.provinceView.provinces.map((province) => [province.provinceKey, province])
      );
      renderKpis();
      renderLegend();
      initMap();
      renderFilterChips();
      renderTabs();
      renderSidebarContent();
      populateMethodsCount();
      // After data is ready, restore view from URL pathname (deep-link / share)
      syncFromPath();
    } catch (error) {
      renderBootstrapError(formatFetchError(error));
    }
  }

  function toggleLegend() {
    state.isLegendHidden = !state.isLegendHidden;
    renderLegend();
  }

  let mapVisible = true;

  function toggleMap() {
    mapVisible = !mapVisible;
    document.body.classList.toggle('map-hidden', !mapVisible);

    const hideBtn = document.getElementById('toggleMapBtn');
    const showBtn = document.getElementById('toggleMapBtnShow');
    if (hideBtn) hideBtn.setAttribute('aria-pressed', mapVisible ? 'false' : 'true');
    if (showBtn) showBtn.setAttribute('aria-pressed', mapVisible ? 'true' : 'false');

    if (mapVisible) {
      // Map became visible again — let MapLibre recompute its canvas size.
      setTimeout(() => window.dispatchEvent(new Event('resize')), 50);
    }
  }

  function toggleTheme() {
    const root = document.documentElement;
    const next = root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
    root.setAttribute('data-theme', next);
    try { localStorage.setItem('nemesis-theme', next); } catch (_) {}
    const meta = document.getElementById('metaThemeColor');
    if (meta) meta.setAttribute('content', next === 'dark' ? '#111d35' : '#fafaf6');

    // Re-color choropleth + swap basemap to match the new theme.
    syncMapThemeVars();
    if (dashboardData) {
      dashboardData.legend = applyChoroplethPalette(dashboardData.legend);
      if (dashboardData.provinceView) {
        dashboardData.provinceView.legend = applyChoroplethPalette(
          dashboardData.provinceView.legend
        );
      }
    }
    renderLegend();
    if (dashboardData) renderSidebarContent(false);
    if (window['AuditMap']) {
      if (window['AuditMap'].setTheme) window['AuditMap'].setTheme();
      if (dashboardData) refreshMapStyles();
    }
  }

  window['dashboardActions'] = {
    changeModalPage,
    closeMethods,
    closeRegionModal,
    copyShareLink,
    copySourceId,
    handlePackageRowKeydown,
    jumpToPage,
    navigateHome,
    openAreaModal,
    openMethods,
    openOwnerModal,
    openPackageDetail,
    setMapFilter,
    setModalPageSize,
    setModalSearch,
    setModalSeverityFilter,
    setSearch,
    setSort,
    setTab,
    toggleLegend,
    toggleMap,
    toggleTheme,
  };

  bindEvents();
  bootstrap();
})();

export { };
