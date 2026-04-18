import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import hpp from 'hpp';
import { CORS_ORIGIN } from './config.js';
import {
  getBootstrapPayload,
  getOwnerPackages,
  getRegionPackages,
  getProvincePackages,
} from './services/dashboard.service.js';

function resolveCorsOrigin() {
  if (CORS_ORIGIN === '*') {
    return '*';
  }

  return CORS_ORIGIN.split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

export async function createApp() {
  const { openDatabase } = await import('./db.js');
  const db = openDatabase();
  const app = express();

  // Security Middlewares
  app.use(helmet({
    contentSecurityPolicy: {
      useDefaults: true,
      directives: {
        "default-src": ["'self'"],
        "script-src": ["'self'", "'unsafe-inline'", "'unsafe-eval'", "https://unpkg.com"],
        "script-src-attr": ["'unsafe-inline'"],
        "connect-src": ["'self'", "ws:", "wss:", "http:", "https:"],
        "worker-src": ["'self'", "blob:"],
        "child-src": ["'self'", "blob:"],
        "img-src": ["'self'", "data:", "blob:", "https:"],
        "style-src": ["'self'", "'unsafe-inline'", "https://unpkg.com", "https://fonts.googleapis.com"],
        "font-src": ["'self'", "https://fonts.gstatic.com"],
      },
    },
  }));
  app.use(
    cors({
      origin: resolveCorsOrigin(),
    })
  );
  app.use(express.json({ limit: '1mb' }));
  app.use(hpp()); // HTTP Parameter Pollution prevention

  // Rate Limiting
  const limiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100, // limit each IP to 100 requests per windowMs
    standardHeaders: true,
    legacyHeaders: false,
  });
  app.use('/api', limiter);

  // API Routes
  app.get('/api/health', (_req, res) => {
    res.json({ status: 'ok' });
  });

  app.get('/api/bootstrap', (_req, res) => {
    res.json(getBootstrapPayload(db));
  });

  app.get('/api/regions/:regionKey/packages', (req, res) => {
    const payload = getRegionPackages(db, req.params.regionKey, req.query);
    if (!payload) {
      return res.status(404).json({ error: 'Region not found' });
    }
    res.json(payload);
  });

  app.get('/api/provinces/:provinceKey/packages', (req, res) => {
    const payload = getProvincePackages(db, req.params.provinceKey, req.query);
    if (!payload) {
      return res.status(404).json({ error: 'Province not found' });
    }
    res.json(payload);
  });

  app.get('/api/owners/packages', (req, res) => {
    const ownerType = String(req.query.ownerType || '').trim();
    const ownerName = String(req.query.ownerName || '').trim();

    if (!ownerType || !ownerName) {
      return res.status(400).json({ error: 'ownerType and ownerName are required' });
    }

    const payload = getOwnerPackages(db, req.query);
    if (!payload) {
      return res.status(404).json({ error: 'Owner not found' });
    }
    res.json(payload);
  });

  // Error boundary
  app.use((err, _req, res, _next) => {
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  });

  return { app, db };
}
