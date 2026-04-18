const crypto = require("crypto");
const zlib = require("zlib");
const { promisify } = require("util");

const brotliCompress = promisify(zlib.brotliCompress);
const gzip = promisify(zlib.gzip);

function createVariant(buffer, encoding, etagBase) {
  return {
    buffer,
    encoding,
    etag: `W/"${etagBase}-${encoding}"`,
  };
}

// Build all wire-format variants once so steady-state requests avoid re-stringifying and recompressing.
async function buildCompressedVariants(jsonBuffer) {
  const [brotliBuffer, gzipBuffer] = await Promise.all([
    brotliCompress(jsonBuffer, {
      params: {
        [zlib.constants.BROTLI_PARAM_QUALITY]: 5,
      },
    }),
    gzip(jsonBuffer, {
      level: zlib.constants.Z_BEST_SPEED,
    }),
  ]);

  const etagBase = crypto.createHash("sha1").update(jsonBuffer).digest("hex");

  return {
    identity: createVariant(jsonBuffer, "identity", etagBase),
    br: createVariant(brotliBuffer, "br", etagBase),
    gzip: createVariant(gzipBuffer, "gzip", etagBase),
    lastModified: new Date().toUTCString(),
  };
}

// Express already parses Accept-Encoding quality values, so we only need to pick the best supported variant.
function selectEncoding(req) {
  if (req.acceptsEncodings("br")) {
    return "br";
  }

  if (req.acceptsEncodings("gzip")) {
    return "gzip";
  }

  return "identity";
}

function createBootstrapResponseCache(db, getBootstrapPayload) {
  let cachedEntry = null;
  let pendingBuild = null;
  let generation = 0;

  async function buildEntry() {
    const payload = getBootstrapPayload(db);
    const jsonBuffer = Buffer.from(JSON.stringify(payload));
    return buildCompressedVariants(jsonBuffer);
  }

  // Share one in-flight build across concurrent requests so a cold cache only does the work once.
  async function getEntry() {
    if (cachedEntry) {
      return cachedEntry;
    }

    if (!pendingBuild) {
      const currentGeneration = generation;
      pendingBuild = buildEntry()
        .then((entry) => {
          if (currentGeneration === generation) {
            cachedEntry = entry;
          }

          return entry;
        })
        .finally(() => {
          pendingBuild = null;
        });
    }

    return pendingBuild;
  }

  // Bump the generation so any stale in-flight build result is discarded instead of replacing newer state.
  function invalidate() {
    generation += 1;
    cachedEntry = null;
  }

  async function prime() {
    await getEntry();
  }

  // Serve the prebuilt buffer directly to bypass per-request JSON serialization and compression work.
  async function send(req, res) {
    const entry = await getEntry();
    const encoding = selectEncoding(req);
    const variant = entry[encoding];

    res.set("Content-Type", "application/json; charset=utf-8");
    res.set("Cache-Control", "public, max-age=0, must-revalidate");
    res.set("ETag", variant.etag);
    res.set("Last-Modified", entry.lastModified);
    res.vary("Accept-Encoding");

    if (req.fresh) {
      res.status(304).end();
      return;
    }

    if (encoding === "identity") {
      res.removeHeader("Content-Encoding");
    } else {
      res.set("Content-Encoding", encoding);
    }

    res.set("Content-Length", String(variant.buffer.length));
    res.status(200).send(variant.buffer);
  }

  return {
    invalidate,
    prime,
    send,
  };
}

module.exports = {
  createBootstrapResponseCache,
};