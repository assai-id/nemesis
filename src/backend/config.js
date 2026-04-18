import path from 'node:path';
import dotenv from 'dotenv';
import { fileURLToPath } from 'node:url';

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(__dirname, '../..');

function resolveFromRoot(value, fallback) {
  const target = value || fallback;
  return path.isAbsolute(target) ? target : path.join(ROOT_DIR, target);
}

const port = Number(process.env.PORT || 3000);

if (!Number.isInteger(port) || port <= 0) {
  throw new Error('PORT must be a positive integer.');
}

const DATA_DIR = resolveFromRoot(process.env.DATA_DIR, 'data');
const DATASET_DIR = resolveFromRoot(process.env.AUDIT_DATASET_DIR, 'dataset');
const GEO_ROOT_PATH = resolveFromRoot(process.env.GEO_ROOT_PATH, path.join('seed', 'geo'));
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';
const DB_PATH = resolveFromRoot(process.env.SQLITE_PATH, path.join('data', 'dashboard.sqlite'));
const GEOJSON_PATH = resolveFromRoot(
  process.env.GEOJSON_PATH,
  path.join(GEO_ROOT_PATH, '03-districts')
);
const PROVINCE_GEOJSON_PATH = resolveFromRoot(
  process.env.PROVINCE_GEOJSON_PATH,
  path.join(GEO_ROOT_PATH, '02-provinces', 'province-only')
);
const AUDIT_DATASET_YEAR = String(process.env.AUDIT_DATASET_YEAR || '2026').trim();
const DEFAULT_REGION_PAGE_SIZE = 25;
const MAX_REGION_PAGE_SIZE = 100;

export {
  ROOT_DIR,
  DATA_DIR,
  DATASET_DIR,
  GEO_ROOT_PATH,
  port as PORT,
  CORS_ORIGIN,
  DB_PATH,
  GEOJSON_PATH,
  PROVINCE_GEOJSON_PATH,
  DATASET_DIR as AUDIT_DATASET_DIR,
  AUDIT_DATASET_YEAR,
  DEFAULT_REGION_PAGE_SIZE,
  MAX_REGION_PAGE_SIZE,
};
