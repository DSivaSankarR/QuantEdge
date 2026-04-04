/**
 * QuantEdge — Kite Data Engine (Cloudflare Worker v2.0)
 * ═══════════════════════════════════════════════════════
 *
 * ARCHITECTURE:
 *   NSE Equity data  → Zerodha Kite API (real-time, official)
 *   Macro/Forex/Index → Yahoo Finance fallback
 *   Fundamentals     → Yahoo Finance (Kite doesn't provide)
 *   Response format  → Yahoo-compatible (zero frontend changes needed)
 *
 * ROUTES:
 *   GET /                          → Health check + Kite connection status
 *   GET /login                     → Redirect to Zerodha login
 *   GET /auth?request_token=X      → Exchange token (Zerodha callback)
 *   GET /debug                     → KV + env diagnostics
 *   GET /?symbol=X                 → OHLCV price data (Kite or Yahoo fallback)
 *   GET /?symbol=X&type=fundamentals → Fundamentals via Yahoo
 *
 * ENV VARS (Cloudflare Worker → Settings → Variables):
 *   KITE_API_KEY    → your api key (plain text)
 *   KITE_API_SECRET → your api secret (encrypted)
 *
 * KV BINDING (Cloudflare Worker → Settings → KV Namespaces):
 *   Binding name: KITE_STORE
 *   Stores: access_token (written on /auth, expires at midnight IST)
 *
 * DAILY WORKFLOW:
 *   1. Visit /login each morning → complete Zerodha login
 *   2. Token auto-saves to KV → QuantEdge uses live data all day
 *   3. Token auto-expires midnight IST → repeat next morning
 * ═══════════════════════════════════════════════════════
 */

// ── Constants ──
const CACHE_TTL_PRICE       = 300;    // 5 minutes — price/OHLCV cache
const CACHE_TTL_INSTRUMENTS = 86400;  // 24 hours  — instrument token cache
const KITE_BASE             = 'https://api.kite.trade';

// ── CORS headers (allow QuantEdge frontend to call this worker) ──
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type':                 'application/json'
};

// ── Symbols that must go to Yahoo (not on NSE) ──
const YAHOO_ONLY = new Set(['^GSPC', '^VIX', 'CL=F', 'USDINR=X', 'INR=X', '^BSESN', '^NSEI', '^NSEBANK']);

// ── Yahoo Finance headers (to avoid bot detection) ──
const YAHOO_HEADERS = {
  'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept':          '*/*',
  'Accept-Language': 'en-US,en;q=0.9',
  'Origin':          'https://finance.yahoo.com',
  'Referer':         'https://finance.yahoo.com/'
};

// ═══════════════════════════════════════════════════════
// MAIN REQUEST HANDLER
// ═══════════════════════════════════════════════════════

export default {
  async fetch(request, env) {

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 200, headers: CORS });
    }

    const url  = new URL(request.url);
    const path = url.pathname;

    // ── Route: Health check ──
    if (path === '/' && !url.searchParams.get('symbol')) {
      const kvToken  = env.KITE_STORE ? await env.KITE_STORE.get('access_token') : null;
      const hasToken = !!(kvToken);
      return jsonResponse({
        status:    'QuantEdge Kite Engine v2.0',
        kite:      hasToken ? 'connected' : 'not connected — complete daily login',
        login_url: hasToken ? null : `https://kite.zerodha.com/connect/login?api_key=${env.KITE_API_KEY}&v=3`,
        timestamp: new Date().toISOString()
      });
    }

    // ── Route: Login redirect ──
    if (path === '/login') {
      const loginUrl = `https://kite.zerodha.com/connect/login?api_key=${env.KITE_API_KEY}&v=3`;
      return Response.redirect(loginUrl, 302);
    }

    // ── Route: Auth callback (Zerodha redirects here after login) ──
    if (path === '/auth') {
      return handleAuth(request, env);
    }

    // ── Route: Debug diagnostics ──
    if (path === '/debug') {
      const kvBound = !!(env.KITE_STORE);
      let kvToken   = null;
      let kvErr     = null;
      if (kvBound) {
        try { kvToken = await env.KITE_STORE.get('access_token'); }
        catch(e) { kvErr = e.message; }
      }
      return jsonResponse({
        kv_bound:        kvBound,
        kv_has_token:    !!(kvToken),
        kv_token_prefix: kvToken ? kvToken.slice(0, 8) + '...' : null,
        kv_error:        kvErr,
        api_key_set:     !!(env.KITE_API_KEY),
        api_secret_set:  !!(env.KITE_API_SECRET),
        timestamp:       new Date().toISOString()
      }, null, 2);
    }

    // ── Route: Data endpoint ──
    const rawSym   = url.searchParams.get('symbol')   || '';
    const type     = url.searchParams.get('type')      || 'chart';
    const interval = url.searchParams.get('interval')  || '1d';
    const range    = url.searchParams.get('range')     || '2y';

    if (!rawSym) {
      return jsonResponse({ error: 'symbol parameter required' }, 400);
    }

    // Fundamentals always go to Yahoo (Kite doesn't provide them)
    if (type === 'fundamentals') {
      return fetchYahooFundamentals(rawSym);
    }

    // Clean the symbol — strip .NS / .BO suffixes, decode %5E → ^
    const clean   = rawSym.toUpperCase().replace(/\.NS$|\.BO$/i, '').replace(/%5E/gi, '^');
    const isForex = rawSym.includes('=') || rawSym.includes('%3D');
    const isIndex = clean.startsWith('^');

    // Macro/global/index symbols → Yahoo only
    if (YAHOO_ONLY.has(clean) || isForex || isIndex) {
      return fetchYahooChart(rawSym, interval, range);
    }

    // NSE equity → try Kite first, fall back to Yahoo if token missing/expired
    const accessToken = env.KITE_STORE ? await env.KITE_STORE.get('access_token') : null;
    if (!accessToken) {
      // No token — fall back silently to Yahoo
      return fetchYahooChart(rawSym, interval, range);
    }

    return fetchKiteHistorical(clean, interval, range, env, accessToken);
  }
};

// ═══════════════════════════════════════════════════════
// KITE: FETCH HISTORICAL OHLCV
// ═══════════════════════════════════════════════════════

async function fetchKiteHistorical(symbol, interval, range, env, accessToken) {
  const cache    = caches.default;
  const cacheKey = new Request(`https://cache.quantedge/kite/${symbol}/${interval}/${range}`);

  // Check Cloudflare edge cache first
  const cached = await cache.match(cacheKey);
  if (cached) {
    const body = await cached.json();
    return new Response(JSON.stringify(body), { status: 200, headers: CORS });
  }

  try {
    // Step 1: Get instrument token (lightweight quote endpoint)
    const token = await getInstrumentToken(symbol, env, accessToken);
    if (!token) {
      // Symbol not found on Kite → fall back to Yahoo
      return fetchYahooChart(symbol, interval, range);
    }

    // Step 2: Build date range
    const { from, to } = buildDateRange(range);

    // Step 3: Map Yahoo interval → Kite interval
    const kiteInterval = mapInterval(interval);

    // Step 4: Fetch historical candles from Kite
    const kiteUrl = `${KITE_BASE}/instruments/historical/${token}/${kiteInterval}?from=${from}&to=${to}&oi=0`;
    const r = await fetch(kiteUrl, {
      headers: {
        'X-Kite-Version': '3',
        'Authorization':  `token ${env.KITE_API_KEY}:${accessToken}`
      }
    });

    // 401/403 = token expired → delete from KV + fall back to Yahoo
    if (r.status === 401 || r.status === 403) {
      if (env.KITE_STORE) await env.KITE_STORE.delete('access_token');
      return fetchYahooChart(symbol, interval, range);
    }

    if (!r.ok) {
      return fetchYahooChart(symbol, interval, range);
    }

    const kiteData = await r.json();
    if (!kiteData?.data?.candles?.length) {
      return fetchYahooChart(symbol, interval, range);
    }

    // Step 5: Convert Kite format → Yahoo-compatible format
    // dataSource: 'kite' is embedded in meta so frontend badge shows KITE ✓
    const responseData = convertKiteToYahoo(symbol, kiteData.data.candles);

    // Cache the response at edge for 5 minutes
    const resp = new Response(JSON.stringify(responseData), {
      status: 200,
      headers: { ...CORS, 'Cache-Control': `max-age=${CACHE_TTL_PRICE}` }
    });
    await cache.put(cacheKey, resp.clone());

    return new Response(JSON.stringify(responseData), { status: 200, headers: CORS });

  } catch(err) {
    // Any unexpected error → fall back to Yahoo silently
    return fetchYahooChart(symbol, interval, range);
  }
}

// ═══════════════════════════════════════════════════════
// KITE: INSTRUMENT TOKEN LOOKUP
// Uses the lightweight /quote endpoint (not the 50k-row CSV)
// Instrument tokens are cached 24 hours — they rarely change
// ═══════════════════════════════════════════════════════

async function getInstrumentToken(symbol, env, accessToken) {
  const cache    = caches.default;
  const cacheKey = new Request(`https://cache.quantedge/instruments/${symbol}`);

  // Return cached token if available
  const cached = await cache.match(cacheKey);
  if (cached) {
    const body = await cached.json();
    return body.token;
  }

  try {
    // Use /quote endpoint — returns instrument_token directly, much lighter than CSV
    const r = await fetch(`${KITE_BASE}/quote?i=NSE:${symbol}`, {
      headers: {
        'X-Kite-Version': '3',
        'Authorization':  `token ${env.KITE_API_KEY}:${accessToken}`
      }
    });

    if (!r.ok) return null;

    const data  = await r.json();
    const token = data?.data?.[`NSE:${symbol}`]?.instrument_token;
    if (!token) return null;

    // Cache this token for 24 hours
    const resp = new Response(JSON.stringify({ token: String(token), symbol }), {
      status: 200,
      headers: { 'Content-Type': 'application/json', 'Cache-Control': `max-age=${CACHE_TTL_INSTRUMENTS}` }
    });
    await cache.put(cacheKey, resp.clone());

    return String(token);

  } catch(_) {
    return null;
  }
}

// ═══════════════════════════════════════════════════════
// KITE: FORMAT CONVERTER
// Converts Kite candle array → Yahoo Finance chart format
// Frontend reads this transparently — no changes needed there
// dataSource: 'kite' in meta → triggers KITE ✓ badge in UI
// ═══════════════════════════════════════════════════════

function convertKiteToYahoo(symbol, candles) {
  const timestamps = [], opens = [], highs = [], lows = [], closes = [], volumes = [];

  for (const [date, o, h, l, c, v] of candles) {
    timestamps.push(Math.floor(new Date(date).getTime() / 1000));
    opens.push(o);
    highs.push(h);
    lows.push(l);
    closes.push(c);
    volumes.push(v);
  }

  const lastClose = closes[closes.length - 1] || 0;
  const prevClose = closes[closes.length - 2] || lastClose;

  return {
    chart: {
      result: [{
        meta: {
          symbol:                 symbol,
          exchangeName:           'NSE',
          fullExchangeName:       'NSE',
          instrumentType:         'EQUITY',
          regularMarketPrice:     lastClose,
          regularMarketChange:    lastClose - prevClose,
          regularMarketChangePct: prevClose > 0 ? ((lastClose - prevClose) / prevClose * 100) : 0,
          dataSource:             'kite'   // ← This triggers KITE ✓ badge in frontend
        },
        timestamp: timestamps,
        indicators: {
          quote: [{ open: opens, high: highs, low: lows, close: closes, volume: volumes }]
        }
      }],
      error: null
    }
  };
}

// ═══════════════════════════════════════════════════════
// YAHOO: OHLCV CHART DATA (fallback for macro + when Kite unavailable)
// ═══════════════════════════════════════════════════════

async function fetchYahooChart(rawSym, interval, range) {
  const cache    = caches.default;
  const cacheKey = new Request(`https://cache.quantedge/yahoo/${rawSym}/${interval}/${range}`);

  const cached = await cache.match(cacheKey);
  if (cached) {
    const body = await cached.json();
    return new Response(JSON.stringify(body), { status: 200, headers: CORS });
  }

  const isIndex = rawSym.startsWith('%5E') || rawSym.startsWith('^');
  const isForex = rawSym.includes('=')    || rawSym.includes('%3D');
  const clean   = rawSym.toUpperCase().replace(/\.NS$|\.BO$/i, '').replace(/%5E/gi, '^');

  // Build symbol list to try
  const trySyms = (isIndex || isForex)
    ? [clean]
    : [clean + '.NS', clean + '.BO'];

  for (const sym of trySyms) {
    for (const host of ['query1', 'query2']) {
      try {
        const r = await fetch(
          `https://${host}.finance.yahoo.com/v8/finance/chart/${sym}?interval=${interval}&range=${range}&includePrePost=false`,
          { headers: YAHOO_HEADERS }
        );
        if (!r.ok) continue;
        const data = await r.json();
        if (data?.chart?.result?.[0]) {
          const resp = new Response(JSON.stringify(data), {
            status: 200,
            headers: { ...CORS, 'Cache-Control': `max-age=${CACHE_TTL_PRICE}` }
          });
          await cache.put(cacheKey, resp.clone());
          return new Response(JSON.stringify(data), { status: 200, headers: CORS });
        }
      } catch(_) {}
    }
  }

  return jsonResponse({ error: `No chart data found for ${rawSym}` }, 404);
}

// ═══════════════════════════════════════════════════════
// YAHOO: FUNDAMENTALS (PE, ROE, Revenue Growth, Debt/Equity etc.)
// ═══════════════════════════════════════════════════════

async function fetchYahooFundamentals(rawSym) {
  const isIndex = rawSym.startsWith('%5E') || rawSym.startsWith('^');
  const isForex = rawSym.includes('=')    || rawSym.includes('%3D');
  const clean   = rawSym.toUpperCase().replace(/\.NS$|\.BO$/i, '').replace(/%5E/gi, '^');
  const syms    = (isIndex || isForex) ? [clean] : [clean + '.NS', clean + '.BO'];

  // Get Yahoo cookie + crumb (required for v10 API)
  let cookie = '';
  try {
    const r = await fetch('https://fc.yahoo.com', { headers: YAHOO_HEADERS, redirect: 'manual' });
    cookie = (r.headers.get('set-cookie') || '').split(';')[0];
  } catch(_) {}

  let crumb = '';
  try {
    const r = await fetch('https://query1.finance.yahoo.com/v1/test/getcrumb', {
      headers: { ...YAHOO_HEADERS, ...(cookie ? { Cookie: cookie } : {}) }
    });
    if (r.ok) crumb = (await r.text()).trim();
  } catch(_) {}

  const modules    = 'financialData,defaultKeyStatistics,summaryDetail';
  const crumbParam = crumb ? `&crumb=${encodeURIComponent(crumb)}` : '';
  const reqHdrs    = { ...YAHOO_HEADERS, ...(cookie ? { Cookie: cookie } : {}) };

  for (const sym of syms) {
    for (const host of ['query1', 'query2']) {
      try {
        const r = await fetch(
          `https://${host}.finance.yahoo.com/v10/finance/quoteSummary/${sym}?modules=${modules}${crumbParam}`,
          { headers: reqHdrs }
        );
        if (!r.ok) continue;
        const data = await r.json();
        if (data?.quoteSummary?.result?.[0]) {
          return new Response(JSON.stringify(data), { status: 200, headers: CORS });
        }
      } catch(_) {}
    }
  }

  return jsonResponse({ error: `No fundamentals found for ${rawSym}` }, 404);
}

// ═══════════════════════════════════════════════════════
// AUTH: DAILY TOKEN EXCHANGE
// Zerodha redirects to /auth?request_token=X&status=success
// We exchange request_token → access_token and store in KV
// IMPORTANT: Set redirect URL in Kite app to:
//   https://quantedge-kite.siva-d-sankar.workers.dev/auth
// ═══════════════════════════════════════════════════════

async function handleAuth(request, env) {
  const url          = new URL(request.url);
  const requestToken = url.searchParams.get('request_token');
  const status       = url.searchParams.get('status');

  if (status !== 'success' || !requestToken) {
    return new Response(
      generateAuthPage('error', null, 'Login failed or cancelled. Please try again.'),
      { status: 400, headers: { 'Content-Type': 'text/html' } }
    );
  }

  try {
    // Compute checksum: SHA256(api_key + request_token + api_secret)
    const checksum = await computeChecksum(env.KITE_API_KEY, requestToken, env.KITE_API_SECRET);

    const r = await fetch(`${KITE_BASE}/session/token`, {
      method: 'POST',
      headers: {
        'X-Kite-Version': '3',
        'Content-Type':   'application/x-www-form-urlencoded'
      },
      body: new URLSearchParams({
        api_key:       env.KITE_API_KEY,
        request_token: requestToken,
        checksum:      checksum
      })
    });

    if (!r.ok) {
      const errBody = await r.text();
      return new Response(
        generateAuthPage('error', null, `Token exchange failed: ${r.status} — ${errBody}`),
        { status: 400, headers: { 'Content-Type': 'text/html' } }
      );
    }

    const data        = await r.json();
    const accessToken = data?.data?.access_token;

    if (!accessToken) {
      return new Response(
        generateAuthPage('error', null, 'No access token in response from Kite.'),
        { status: 400, headers: { 'Content-Type': 'text/html' } }
      );
    }

    // Save token to KV — expires at midnight IST (approx 18.5 hours from 9:30 AM IST)
    // KV TTL: 24 hours is safe, Kite invalidates it at their end at midnight anyway
    await env.KITE_STORE.put('access_token', accessToken, { expirationTtl: 86400 });

    return new Response(
      generateAuthPage('success', accessToken, null),
      { status: 200, headers: { 'Content-Type': 'text/html' } }
    );

  } catch(err) {
    return new Response(
      generateAuthPage('error', null, `Unexpected error: ${err.message}`),
      { status: 500, headers: { 'Content-Type': 'text/html' } }
    );
  }
}

// ═══════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════

function mapInterval(yahooInterval) {
  const map = {
    '1d':  'day',
    '1wk': 'week',
    '1mo': 'month',
    '5m':  '5minute',
    '15m': '15minute',
    '30m': '30minute',
    '60m': '60minute'
  };
  return map[yahooInterval] || 'day';
}

function buildDateRange(range) {
  const to   = new Date();
  const from = new Date();
  const days = {
    '1d': 1, '5d': 5, '1mo': 30, '3mo': 90,
    '6mo': 180, '1y': 365, '2y': 730, '5y': 1825, '10y': 3650
  }[range] || 730;
  from.setDate(from.getDate() - days);
  return {
    from: from.toISOString().slice(0, 10),
    to:   to.toISOString().slice(0, 10)
  };
}

async function computeChecksum(apiKey, requestToken, apiSecret) {
  const data    = apiKey + requestToken + apiSecret;
  const msgBuf  = new TextEncoder().encode(data);
  const hashBuf = await crypto.subtle.digest('SHA-256', msgBuf);
  return Array.from(new Uint8Array(hashBuf))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), { status, headers: CORS });
}

function generateAuthPage(status, accessToken, error) {
  if (status === 'success') {
    return `<!DOCTYPE html>
<html>
<head>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>QuantEdge — Kite Connected</title>
  <style>
    body{font-family:Arial,sans-serif;background:#0a0c0f;color:#e8edf5;padding:24px;max-width:480px;margin:0 auto;text-align:center}
    h1{color:#2dd4bf;font-size:22px;margin-bottom:8px}
    .card{background:#12161c;border:1px solid rgba(45,212,191,.3);border-radius:12px;padding:20px;margin:20px 0}
    p{color:#94a3b8;font-size:14px;line-height:1.6;margin:8px 0}
    .action{background:rgba(45,212,191,.1);border:1px solid rgba(45,212,191,.25);border-radius:8px;padding:12px;margin-top:16px;font-size:13px;color:#2dd4bf}
    .btn{display:inline-block;background:#2dd4bf;color:#0a0c0f;padding:12px 28px;border-radius:8px;text-decoration:none;font-weight:bold;font-size:15px;margin-top:16px}
    .note{color:#94a3b8;font-size:11px;margin-top:16px}
  </style>
</head>
<body>
  <h1>✅ Kite Connected</h1>
  <div class="card">
    <p style="font-size:18px;color:#e8edf5;font-weight:bold">Token saved automatically</p>
    <p>No manual steps needed.<br>QuantEdge is now using live NSE data.</p>
    <div class="action">Open QuantEdge → Run Scan → Live data active ⚡</div>
  </div>
  <a href="https://dsivasankarr.github.io/QuantEdge" class="btn">Open QuantEdge →</a>
  <p class="note">⏰ Token auto-expires midnight IST. Login again tomorrow morning.</p>
</body>
</html>`;
  } else {
    return `<!DOCTYPE html>
<html>
<head>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>QuantEdge — Auth Error</title>
  <style>
    body{font-family:Arial,sans-serif;background:#0a0c0f;color:#e8edf5;padding:24px;max-width:480px;margin:0 auto}
    h1{color:#fb7185}
    p{color:#94a3b8;font-size:14px;line-height:1.6}
    a{color:#2dd4bf}
  </style>
</head>
<body>
  <h1>❌ Authentication Error</h1>
  <p>${error || 'Unknown error occurred.'}</p>
  <p><a href="/login">Try again →</a></p>
</body>
</html>`;
  }
}
