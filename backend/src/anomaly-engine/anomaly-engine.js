/**
 * anomaly-engine.js
 * 
 * Procurement anomaly detection engine for Nemesis.
 * Implements heuristic scoring for common red flags in SIRUP data.
 * 
 * Each detector returns an array of AnomalyResult objects.
 * The composite scorer aggregates them into a risk score per contract.
 */

"use strict";

// ─── Constants ────────────────────────────────────────────────────────────────

const RISK_WEIGHTS = {
  SINGLE_SOURCE_HIGH_VALUE:  0.30,
  PRICE_OUTLIER:             0.25,
  DEADLINE_CRAMMING:         0.15,
  VENDOR_CONCENTRATION:      0.20,
  ROUND_NUMBER_PRICE:        0.10,
};

const THRESHOLDS = {
  HIGH_VALUE_IDR:            500_000_000,    // 500 juta IDR
  ROUND_NUMBER_TOLERANCE:    0.001,          // 0.1% tolerance for "round" detection
  DEADLINE_DAYS_WARNING:     7,              // less than N days left on posting
  PRICE_OUTLIER_ZSCORE:      2.5,           // Z-score threshold
  CONCENTRATION_RATIO:       0.7,            // vendor wins 70%+ of category spend
};

// ─── Types ────────────────────────────────────────────────────────────────────

/**
 * @typedef {Object} AnomalyResult
 * @property {string} type         - Anomaly type key
 * @property {number} score        - Risk contribution (0.0 - 1.0)
 * @property {string} description  - Human-readable explanation
 * @property {Object} evidence     - Raw data supporting the finding
 */

/**
 * @typedef {Object} ContractRecord
 * @property {string}  id
 * @property {string}  satuan_kerja
 * @property {string}  nama_paket
 * @property {number}  pagu            - Budget ceiling (IDR)
 * @property {number}  hps             - Estimated price (IDR)
 * @property {string}  metode_pemilihan - Procurement method
 * @property {string}  nama_penyedia   - Vendor name
 * @property {string}  tgl_pengumuman  - Announcement date (ISO)
 * @property {string}  tgl_selesai     - Completion deadline (ISO)
 * @property {string}  kode_klpd       - Agency code
 */

// ─── Individual Detectors ─────────────────────────────────────────────────────

/**
 * Detect single-source (penunjukan langsung) contracts above value threshold.
 * High-value direct appointments bypass competition entirely.
 * 
 * @param {ContractRecord} contract
 * @returns {AnomalyResult|null}
 */
function detectSingleSourceHighValue(contract) {
  const isSingleSource = /penunjukan langsung/i.test(contract.metode_pemilihan);
  const isHighValue = contract.pagu >= THRESHOLDS.HIGH_VALUE_IDR;

  if (!isSingleSource || !isHighValue) return null;

  const severity = Math.min(contract.pagu / (THRESHOLDS.HIGH_VALUE_IDR * 10), 1.0);

  return {
    type: "SINGLE_SOURCE_HIGH_VALUE",
    score: severity,
    description: `Penunjukan langsung senilai Rp ${formatIDR(contract.pagu)} — melewati kompetisi terbuka.`,
    evidence: {
      method: contract.metode_pemilihan,
      value_idr: contract.pagu,
      threshold_idr: THRESHOLDS.HIGH_VALUE_IDR,
    },
  };
}

/**
 * Detect prices that are suspiciously "round" numbers.
 * Real-world cost estimates are rarely perfectly round.
 * This is a weak signal but useful in combination with others.
 * 
 * @param {ContractRecord} contract
 * @returns {AnomalyResult|null}
 */
function detectRoundNumberPrice(contract) {
  const value = contract.hps || contract.pagu;
  if (!value || value < 10_000_000) return null; // ignore small contracts

  // Check if value is divisible by a large round number
  const divisors = [1_000_000_000, 500_000_000, 100_000_000, 50_000_000];
  
  for (const div of divisors) {
    const remainder = value % div;
    const ratio = remainder / value;
    if (ratio < THRESHOLDS.ROUND_NUMBER_TOLERANCE) {
      return {
        type: "ROUND_NUMBER_PRICE",
        score: 0.4,
        description: `Nilai HPS persis Rp ${formatIDR(value)} — angka bulat sempurna tanpa estimasi biaya nyata.`,
        evidence: { value_idr: value, divisor: div },
      };
    }
  }

  return null;
}

/**
 * Detect contracts posted with very short deadlines (deadline cramming).
 * Short windows limit who can realistically compete.
 * 
 * @param {ContractRecord} contract
 * @returns {AnomalyResult|null}
 */
function detectDeadlineCramming(contract) {
  if (!contract.tgl_pengumuman || !contract.tgl_selesai) return null;

  const announced = new Date(contract.tgl_pengumuman);
  const deadline = new Date(contract.tgl_selesai);
  const daysDiff = (deadline - announced) / (1000 * 60 * 60 * 24);

  if (daysDiff < 0 || daysDiff >= THRESHOLDS.DEADLINE_DAYS_WARNING) return null;

  const severity = Math.max(0, 1 - (daysDiff / THRESHOLDS.DEADLINE_DAYS_WARNING));

  return {
    type: "DEADLINE_CRAMMING",
    score: severity,
    description: `Waktu penawaran hanya ${Math.round(daysDiff)} hari — terlalu sempit untuk persaingan sehat.`,
    evidence: {
      announced: contract.tgl_pengumuman,
      deadline: contract.tgl_selesai,
      days_available: Math.round(daysDiff),
    },
  };
}

/**
 * Detect price outliers within the same procurement category and agency.
 * Requires a comparison dataset (array of sibling contracts).
 * 
 * @param {ContractRecord}   contract
 * @param {ContractRecord[]} peers    - Contracts in same category/agency
 * @returns {AnomalyResult|null}
 */
function detectPriceOutlier(contract, peers) {
  if (!peers || peers.length < 5) return null;

  const values = peers.map(c => c.hps || c.pagu).filter(v => v > 0);
  if (values.length < 5) return null;

  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const variance = values.reduce((sum, v) => sum + Math.pow(v - mean, 2), 0) / values.length;
  const stddev = Math.sqrt(variance);
  
  if (stddev === 0) return null;

  const contractValue = contract.hps || contract.pagu;
  const zScore = Math.abs(contractValue - mean) / stddev;

  if (zScore < THRESHOLDS.PRICE_OUTLIER_ZSCORE) return null;

  const direction = contractValue > mean ? "di atas" : "di bawah";
  const severity = Math.min(zScore / 10, 1.0);

  return {
    type: "PRICE_OUTLIER",
    score: severity,
    description: `Nilai kontrak ${direction} rata-rata kategori sebesar ${zScore.toFixed(1)} standar deviasi.`,
    evidence: {
      contract_value: contractValue,
      peer_mean: Math.round(mean),
      peer_stddev: Math.round(stddev),
      z_score: parseFloat(zScore.toFixed(2)),
      peer_count: peers.length,
    },
  };
}

/**
 * Detect vendor concentration: one vendor wins too much of a category's spend.
 * Requires aggregated vendor data for the category.
 * 
 * @param {string} vendorName
 * @param {string} categoryCode
 * @param {Object} vendorStats - { [vendor]: { total_value, contract_count } }
 * @returns {AnomalyResult|null}
 */
function detectVendorConcentration(vendorName, categoryCode, vendorStats) {
  if (!vendorStats || Object.keys(vendorStats).length < 3) return null;

  const totalSpend = Object.values(vendorStats).reduce((sum, v) => sum + v.total_value, 0);
  const vendorSpend = vendorStats[vendorName]?.total_value || 0;

  if (totalSpend === 0) return null;

  const concentrationRatio = vendorSpend / totalSpend;

  if (concentrationRatio < THRESHOLDS.CONCENTRATION_RATIO) return null;

  return {
    type: "VENDOR_CONCENTRATION",
    score: concentrationRatio,
    description: `${vendorName} menguasai ${(concentrationRatio * 100).toFixed(1)}% dari belanja kategori ini — potensi monopoli de facto.`,
    evidence: {
      vendor: vendorName,
      category: categoryCode,
      vendor_share: parseFloat(concentrationRatio.toFixed(3)),
      vendor_spend_idr: vendorSpend,
      total_category_spend_idr: totalSpend,
    },
  };
}

// ─── Composite Scorer ─────────────────────────────────────────────────────────

/**
 * Run all applicable detectors and produce a composite risk score.
 * 
 * @param {ContractRecord}   contract
 * @param {Object}           context
 * @param {ContractRecord[]} context.peers        - Sibling contracts for outlier detection
 * @param {Object}           context.vendorStats  - Aggregated vendor data
 * @returns {{ score: number, label: string, anomalies: AnomalyResult[] }}
 */
function scoreContract(contract, context = {}) {
  const { peers = [], vendorStats = {} } = context;

  const detections = [
    detectSingleSourceHighValue(contract),
    detectRoundNumberPrice(contract),
    detectDeadlineCramming(contract),
    detectPriceOutlier(contract, peers),
    detectVendorConcentration(contract.nama_penyedia, contract.kode_klpd, vendorStats),
  ].filter(Boolean);

  // Weighted composite score
  let compositeScore = 0;
  for (const anomaly of detections) {
    const weight = RISK_WEIGHTS[anomaly.type] ?? 0.1;
    compositeScore += anomaly.score * weight;
  }

  // Normalize to 0–1
  compositeScore = Math.min(compositeScore, 1.0);

  const label = compositeScore >= 0.7 ? "HIGH"
              : compositeScore >= 0.4 ? "MEDIUM"
              : compositeScore >= 0.15 ? "LOW"
              : "CLEAN";

  return {
    contract_id: contract.id,
    score: parseFloat(compositeScore.toFixed(4)),
    label,
    anomaly_count: detections.length,
    anomalies: detections,
  };
}

// ─── Batch Processor ─────────────────────────────────────────────────────────

/**
 * Score a batch of contracts. Builds peer groups automatically.
 * 
 * @param {ContractRecord[]} contracts
 * @returns {Array} Sorted by score descending
 */
function scoreBatch(contracts) {
  // Build peer groups by agency + method
  const peerGroups = {};
  for (const c of contracts) {
    const key = `${c.kode_klpd}:${c.metode_pemilihan}`;
    if (!peerGroups[key]) peerGroups[key] = [];
    peerGroups[key].push(c);
  }

  // Build vendor stats per agency
  const vendorStatsByAgency = {};
  for (const c of contracts) {
    if (!vendorStatsByAgency[c.kode_klpd]) vendorStatsByAgency[c.kode_klpd] = {};
    const stats = vendorStatsByAgency[c.kode_klpd];
    if (!stats[c.nama_penyedia]) stats[c.nama_penyedia] = { total_value: 0, contract_count: 0 };
    stats[c.nama_penyedia].total_value += c.pagu || 0;
    stats[c.nama_penyedia].contract_count += 1;
  }

  return contracts
    .map(c => {
      const peerKey = `${c.kode_klpd}:${c.metode_pemilihan}`;
      return scoreContract(c, {
        peers: peerGroups[peerKey] || [],
        vendorStats: vendorStatsByAgency[c.kode_klpd] || {},
      });
    })
    .sort((a, b) => b.score - a.score);
}

// ─── Utilities ────────────────────────────────────────────────────────────────

function formatIDR(value) {
  return new Intl.NumberFormat("id-ID", {
    style: "currency",
    currency: "IDR",
    maximumFractionDigits: 0,
  }).format(value);
}

// ─── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  scoreContract,
  scoreBatch,
  detectors: {
    detectSingleSourceHighValue,
    detectRoundNumberPrice,
    detectDeadlineCramming,
    detectPriceOutlier,
    detectVendorConcentration,
  },
  RISK_WEIGHTS,
  THRESHOLDS,
};
