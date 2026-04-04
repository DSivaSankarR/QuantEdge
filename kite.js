/**
 * QuantEdge — Kite Data Engine (Cloudflare Worker v1.0)
 * ═══════════════════════════════════════════════════════
 *
 * ARCHITECTURE:
 *   Equity data  → Zerodha Kite API (NSE official, real-time)
 *   Macro data   → Yahoo Finance fallback (S&P500, Crude, USDINR)
 *   Response     → Identical format to yahoo.js (zero frontend changes)
 *
 * ROUTES:
 *   GET /              → Health check
 *   GET /?symbol=X     → Historical OHLCV (chart mode, default)
 *   GET /?symbol=X&type=fundamentals → Fundamentals (Yahoo fallback)
 *   GET /?symbol=X&type=quote        → Live quote
 *
 * ENV VARS (set in Cloudflare Worker → Settings → Variables):
 *   KITE_API_KEY    → isb6s7pfdzqg8n8a
 *   KITE_API_SECRET → your secret (encrypted)
 *   (KITE_ACCESS_TOKEN no longer needed — stored automatically in KV)
 *
 * KV NAMESPACE BINDING (Cloudflare Worker → Settings → KV):
 *   Binding name: KITE_STORE
 *   Token key:    access_token  (written automatically on /auth)
 *
 * CACHE:
 *   Cloudflare edge cache — 5 min for price data 
 *   Instrument tokens cached 24 hours
 *
 * ═══════════════════════════════════════════════════════
 */

const CACHE_TTL_PRICE       = 300;    // 5 min — price data
const CACHE_TTL_INSTRUMENTS = 86400;  // 24 hours — instrument tokens
const KITE_BASE             = 'https://api.kite.trade';

// ── CORS headers ──
const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type':                 'application/json'
};

// ── Macro symbols handled by Yahoo fallback (not on NSE) ──
const YAHOO_FALLBACK_SYMBOLS = new Set(['^GSPC', '^VIX', 'CL=F', 'USDINR=X', 'INR=X', '^BSESN']);

// ── Yahoo Finance base headers (for fallback) ──
const YAHOO_HEADERS = {
  'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept':          '*/*',
  'Accept-Language': 'en-US,en;q=0.9',
  'Origin':          'https://finance.yahoo.com',
  'Referer':         'https://finance.yahoo.com/'
};

// ═══════════════════════════════════════════════════════════
// MAIN HANDLER
// ═══════════════════════════════════════════════════════════

export default {
  async fetch(request, env) {

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 200, headers: CORS });
    }

    const url    = new URL(request.url);
    const path   = url.pathname;

    // ── Health check ──
    if (path === '/' && !url.searchParams.get('symbol')) {
      // Read token from KV (not env var — stored automatically on /auth)
      const kvToken   = env.KITE_STORE ? await env.KITE_STORE.get('access_token') : null;
      const hasToken  = !!(kvToken);
      return new Response(JSON.stringify({
        status:     'QuantEdge Kite Engine v2.0',
        kite:       hasToken ? 'connected' : 'not connected — complete daily login',
        login_url:  hasToken ? null : `https://kite.zerodha.com/connect/login?api_key=${env.KITE_API_KEY}&v=3`,
        timestamp:  new Date().toISOString()
      }), { status: 200, headers: CORS });
    }

    // ── Auth callback — exchange request_token for access_token ──
    if (path === '/auth') {
      return handleAuth(request, env);
    }

    // ── Debug endpoint — shows exact KV and env state ──
    if (path === '/debug') {
      const kvBound   = !!(env.KITE_STORE);
      let   kvToken   = null;
      let   kvErr     = null;
      if (kvBound) {
        try { kvToken = await env.KITE_STORE.get('access_token'); }
        catch(e) { kvErr = e.message; }
      }
      return new Response(JSON.stringify({
        kv_bound:        kvBound,
        kv_has_token:    !!(kvToken),
        kv_token_prefix: kvToken ? kvToken.slice(0,8)+'...' : null,
        kv_error:        kvErr,
        api_key_set:     !!(env.KITE_API_KEY),
        api_secret_set:  !!(env.KITE_API_SECRET),
        timestamp:       new Date().toISOString()
      }, null, 2), { status: 200, headers: CORS });
    }

    // ── Login redirect — convenience endpoint ──
    if (path === '/login') {
      const loginUrl = `https://kite.zerodha.com/connect/login?api_key=${env.KITE_API_KEY}&v=3`;
      return Response.redirect(loginUrl, 302);
    }

    // ── Main data endpoint ──
    const rawSym   = url.searchParams.get('symbol')  || '';
    const type     = url.searchParams.get('type')     || 'chart';
    const interval = url.searchParams.get('interval') || '1d';
    const range    = url.searchParams.get('range')    || '2y';

    if (!rawSym) {
      return new Response(JSON.stringify({ error: 'symbol required' }), { status: 400, headers: CORS });
    }

    // ── Route: fundamentals always go to Yahoo (Kite doesn't provide) ──
    if (type === 'fundamentals') {
      return fetchYahooFundamentals(rawSym, env);
    }

    // ── Route: macro/global symbols → Yahoo fallback ──
    const clean   = rawSym.toUpperCase().replace(/\.NS$|\.BO$/i,'').replace('%5E','^');
    const isForex = rawSym.includes('=') || rawSym.includes('%3D');
    const isIndex = rawSym.startsWith('^') || rawSym.startsWith('%5E');

    if (YAHOO_FALLBACK_SYMBOLS.has(clean) || isForex) {
      return fetchYahooChart(rawSym, interval, range, env);
    }

    // ── Route: NSE equity → Kite API ──
    // Read access token from KV — written automatically on /auth
    // Falls back to Yahoo if token is missing (e.g., first run or midnight expiry)
    const accessToken = env.KITE_STORE ? await env.KITE_STORE.get('access_token') : null;
    if (!accessToken) {
      return fetchYahooChart(rawSym, interval, range, env);
    }

    return fetchKiteHistorical(clean, interval, range, env, accessToken);
  }
};

// ═══════════════════════════════════════════════════════════
// KITE: HISTORICAL DATA
// Fetches OHLCV from Kite and converts to Yahoo-compatible format
// ═══════════════════════════════════════════════════════════

async function fetchKiteHistorical(symbol, interval, range, env, accessToken) {
  const cache    = caches.default;
  const cacheKey = new Request(`https://cache.quantedge/kite/${symbol}/${interval}/${range}`);

  // Check edge cache
  const cached = await cache.match(cacheKey);
  if (cached) {
    const body = await cached.json();
    return new Response(JSON.stringify(body), { status: 200, headers: CORS });
  }

  try {
    // Step 1: Get instrument token for symbol
    const token = await getInstrumentToken(symbol, env, accessToken);
    if (!token) {
      // Token not found → fall back to Yahoo
      return fetchYahooChart(symbol, interval, range, env);
    }

    // Step 2: Calculate date range
    const { from, to } = getDateRange(range);

    // Step 3: Map interval
    const kiteInterval = mapInterval(interval);

    // Step 4: Fetch historical data from Kite
    const kiteUrl = `${KITE_BASE}/instruments/historical/${token}/${kiteInterval}?from=${from}&to=${to}&oi=0`;
    const r = await fetch(kiteUrl, {
      headers: {
        'X-Kite-Version': '3',
        'Authorization':  `token ${env.KITE_API_KEY}:${accessToken}`
      }
    });

    // Handle 403 = token expired — delete from KV + fall back to Yahoo
    if (r.status === 403 || r.status === 401) {
      if (env.KITE_STORE) await env.KITE_STORE.delete('access_token');
      return fetchYahooChart(symbol, interval, range, env);
    }
    if (!r.ok) {
      return fetchYahooChart(symbol, interval, range, env);
    }

    const kiteData = await r.json();
    if (!kiteData?.data?.candles?.length) {
      return fetchYahooChart(symbol, interval, range, env);
    }

    // Step 5: Convert Kite format → Yahoo format (frontend sees no difference)
    const responseData = convertKiteToYahoo(symbol, kiteData.data.candles);

    // Cache the response
    const resp = new Response(JSON.stringify(responseData), {
      status: 200,
      headers: { ...CORS, 'Cache-Control': `max-age=${CACHE_TTL_PRICE}` }
    });
    await cache.put(cacheKey, resp.clone());

    return new Response(JSON.stringify(responseData), { status: 200, headers: CORS });

  } catch (err) {
    // Any error → fall back to Yahoo silently
    return fetchYahooChart(symbol, interval, range, env);
  }
}

// ═══════════════════════════════════════════════════════════
// KITE: INSTRUMENT TOKEN LOOKUP
// Maps NSE symbol → numeric instrument token
// Cached 24 hours — tokens rarely change
// ═══════════════════════════════════════════════════════════

async function getInstrumentToken(symbol, env, accessToken) {
  const cache    = caches.default;
  const cacheKey = new Request(`https://cache.quantedge/instruments/${symbol}`);

  // Check cache first
  const cached = await cache.match(cacheKey);
  if (cached) {
    const body = await cached.json();
    return body.token;
  }

  try {
    // Fetch full NSE instrument list
    const r = await fetch(`${KITE_BASE}/instruments/NSE`, {
      headers: {
        'X-Kite-Version': '3',
        'Authorization':  `token ${env.KITE_API_KEY}:${accessToken}`
      }
    });

    if (!r.ok) return null;

    const csv   = await r.text();
    const lines = csv.trim().split('\n');

    // Parse CSV: instrument_token,exchange_token,tradingsymbol,...
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(',');
      if (!cols[2]) continue;
      const sym = cols[2].trim().replace(/"/g,'');
      if (sym === symbol) {
        const token     = cols[0].trim();
        const tokenData = { token, symbol };
        // Cache this token for 24 hours
        const resp = new Response(JSON.stringify(tokenData), {
          status: 200,
          headers: { 'Content-Type': 'application/json', 'Cache-Control': `max-age=${CACHE_TTL_INSTRUMENTS}` }
        });
        await cache.put(cacheKey, resp.clone());
        return token;
      }
    }
    return null; // Symbol not found
  } catch(_) {
    return null;
  }
}

// ═══════════════════════════════════════════════════════════
// AUTH: DAILY TOKEN EXCHANGE
// Called automatically when Zerodha redirects back after login
// Redirect URL must be set to: https://quantedge-kite.siva-d-sankar.workers.dev/auth
// ═══════════════════════════════════════════════════════════

async function handleAuth(request, env) {
  const url           = new URL(request.url);
  const requestToken  = url.searchParams.get('request_token');
  const status        = url.searchParams.get('status');

  if (status !== 'success' || !requestToken) {
    return new Response(generateAuthPage('error', null, 'Login failed or cancelled. Please try again.'), {
      status: 400,
      headers: { 'Content-Type': 'text/html' }
    });
  }

  try {
    // Exchange request_token for access_token
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
      const errText = await r.text();
      return new Response(generateAuthPage('error', null, `Token exchange failed: ${errText}`), {
        status: 500,
        headers: { 'Content-Type': 'text/html' }
      });
    }

    const data = await r.json();
    const accessToken = data?.data?.access_token;

    if (!accessToken) {
      return new Response(generateAuthPage('error', null, 'No access token received.'), {
        status: 500,
        headers: { 'Content-Type': 'text/html' }
      });
    }

    // Store access_token in KV automatically — no manual copy needed
    if (env.KITE_STORE) {
      await env.KITE_STORE.put('access_token', accessToken, {
        expirationTtl: 86400  // auto-expire after 24 hours (Kite token lifetime)
      });
      // Verify it was stored
      const verify = await env.KITE_STORE.get('access_token');
      if (!verify) {
        return new Response(generateAuthPage('error', null, 'KV write failed — token not stored. Check KV binding.'), {
          status: 500, headers: { 'Content-Type': 'text/html' }
        });
      }
    } else {
      // KV not bound — show error instead of false success
      return new Response(generateAuthPage('error', null,
        'KV namespace KITE_STORE is not bound to this worker. Go to Cloudflare → quantedge-kite → Settings → Variables → KV Namespace Bindings → Add KITE_STORE.'), {
        status: 500, headers: { 'Content-Type': 'text/html' }
      });
    }

    return new Response(generateAuthPage('success', accessToken, null), {
      status: 200,
      headers: { 'Content-Type': 'text/html' }
    });

  } catch (err) {
    return new Response(generateAuthPage('error', null, err.message), {
      status: 500,
      headers: { 'Content-Type': 'text/html' }
    });
  }
}

// ═══════════════════════════════════════════════════════════
// YAHOO: CHART FALLBACK
// Used for: macro symbols, fundamentals, when Kite is unavailable
// Identical to original yahoo.js logic
// ═══════════════════════════════════════════════════════════

async function fetchYahooChart(rawSym, interval, range, env) {
  const cache    = caches.default;
  const cacheKey = new Request(`https://cache.quantedge/yahoo/${rawSym}/${interval}/${range}`);

  const cached = await cache.match(cacheKey);
  if (cached) {
    const body = await cached.json();
    return new Response(JSON.stringify(body), { status: 200, headers: CORS });
  }

  const isIndex  = rawSym.startsWith('%5E') || rawSym.startsWith('^');
  const isForex  = rawSym.includes('=') || rawSym.includes('%3D');
  const clean    = rawSym.toUpperCase().replace(/\.NS$|\.BO$/i,'').replace('%5E','^');
  const nseSym   = (isIndex || isForex) ? clean : clean + '.NS';
  const trySyms  = (isIndex || isForex) ? [clean] : [nseSym, clean + '.BO'];

  for (const sym of trySyms) {
    for (const host of ['query1','query2']) {
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
  return new Response(JSON.stringify({ error: `No data for ${rawSym}` }), { status: 404, headers: CORS });
}

async function fetchYahooFundamentals(rawSym, env) {
  const isIndex  = rawSym.startsWith('%5E') || rawSym.startsWith('^');
  const isForex  = rawSym.includes('=') || rawSym.includes('%3D');
  const clean    = rawSym.toUpperCase().replace(/\.NS$|\.BO$/i,'').replace('%5E','^');
  const nseSym   = (isIndex || isForex) ? clean : clean + '.NS';

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
  const syms       = (isIndex || isForex) ? [clean] : [nseSym, clean + '.BO'];

  for (const sym of syms) {
    for (const host of ['query1','query2']) {
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
  return new Response(JSON.stringify({ error: `No fundamentals for ${rawSym}` }), { status: 404, headers: CORS });
}

// ═══════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════

function convertKiteToYahoo(symbol, candles) {
  /**
   * Converts Kite candle format to Yahoo Finance chart format
   * Kite:  [[date, open, high, low, close, volume], ...]
   * Yahoo: { chart: { result: [{ indicators: { quote: [{ open, high, low, close, volume }] },
   *                              timestamp: [], meta: { regularMarketPrice, symbol } }] } }
   */
  const timestamps = [], opens = [], highs = [], lows = [], closes = [], volumes = [];

  for (const [date, o, h, l, c, v] of candles) {
    timestamps.push(Math.floor(new Date(date).getTime() / 1000));
    opens.push(o); highs.push(h); lows.push(l); closes.push(c); volumes.push(v);
  }

  const lastClose = closes[closes.length - 1] || 0;
  const prevClose = closes[closes.length - 2] || lastClose;

  return {
    chart: {
      result: [{
        meta: {
          symbol:                symbol,
          exchangeName:          'NSE',
          instrumentType:        'EQUITY',
          regularMarketPrice:    lastClose,
          regularMarketChange:   lastClose - prevClose,
          regularMarketChangePct: prevClose > 0 ? ((lastClose - prevClose) / prevClose * 100) : 0,
          dataSource:            'kite'
        },
        timestamp,
        indicators: {
          quote: [{ open: opens, high: highs, low: lows, close: closes, volume: volumes }]
        }
      }],
      error: null
    }
  };
}

function mapInterval(yahooInterval) {
  // Maps Yahoo interval format to Kite interval format
  const map = {
    '1d': 'day', '1wk': 'week', '1mo': 'month',
    '5m': '5minute', '15m': '15minute', '30m': '30minute', '60m': '60minute'
  };
  return map[yahooInterval] || 'day';
}

function getDateRange(range) {
  // Converts Yahoo range string to from/to dates for Kite
  const to   = new Date();
  const from = new Date();
  const map  = { '1d':1,'5d':5,'1mo':30,'3mo':90,'6mo':180,'1y':365,'2y':730,'5y':1825,'10y':3650 };
  const days = map[range] || 730;
  from.setDate(from.getDate() - days);
  return {
    from: from.toISOString().slice(0,10),
    to:   to.toISOString().slice(0,10)
  };
}

// ── TOKEN EXPIRY DETECTION (Bonus) ──
// Kite returns 403 on expired token — handled in fetchKiteHistorical
// Frontend can detect KITE_STORE is empty = needs reconnect
// Call GET / to check: { kite: 'connected' | 'not connected' }
// If 'not connected' → show reconnect signal → redirect to /login

async function computeChecksum(apiKey, requestToken, apiSecret) {
  // SHA256(api_key + request_token + api_secret) — required by Kite
  const data    = apiKey + requestToken + apiSecret;
  const msgBuf  = new TextEncoder().encode(data);
  const hashBuf = await crypto.subtle.digest('SHA-256', msgBuf);
  return Array.from(new Uint8Array(hashBuf)).map(b => b.toString(16).padStart(2,'0')).join('');
}

function generateAuthPage(status, accessToken, error) {
  // Clean mobile-friendly page shown after Zerodha login redirect
  if (status === 'success') {
    return `<!DOCTYPE html><html>
<head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>QuantEdge — Kite Connected</title>
<style>
  body{font-family:Arial,sans-serif;background:#0a0c0f;color:#e8edf5;padding:24px;max-width:480px;margin:0 auto;text-align:center}
  h1{color:#2dd4bf;font-size:22px;margin-bottom:8px}
  .card{background:#12161c;border:1px solid rgba(45,212,191,.3);border-radius:12px;padding:20px;margin:20px 0}
  p{color:#94a3b8;font-size:14px;line-height:1.6;margin:8px 0}
  .action{background:rgba(45,212,191,.1);border:1px solid rgba(45,212,191,.25);
          border-radius:8px;padding:12px;margin-top:16px;font-size:13px;color:#2dd4bf}
  .note{color:#94a3b8;font-size:11px;margin-top:16px}
</style></head>
<body>
<h1>✅ Kite Connected</h1>
<div class="card">
  <p style="font-size:18px;color:#e8edf5;font-weight:bold">Token saved automatically</p>
  <p>No manual steps needed.<br>QuantEdge is now using live NSE data.</p>
  <div class="action">
    Open QuantEdge → Run Scan → Live data active ⚡
  </div>
</div>
<a href="https://dsivasankarr.github.io/QuantEdge"
   style="display:inline-block;background:#2dd4bf;color:#0a0c0f;padding:12px 28px;
          border-radius:8px;text-decoration:none;font-weight:bold;font-size:15px;margin-top:8px">
  Open QuantEdge →
</a>
<p class="note">⏰ Token auto-expires midnight IST. Login again tomorrow morning.</p>
</body></html>`;
  } else {
    return `<!DOCTYPE html><html>
<head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>QuantEdge — Auth Error</title>
<style>body{font-family:Arial,sans-serif;background:#0a0c0f;color:#e8edf5;padding:24px;max-width:480px;margin:0 auto}
h1{color:#fb7185}p{color:#94a3b8;font-size:14px}</style></head>
<body>
<h1>❌ Authentication Error</h1>
<p>${error || 'Unknown error occurred.'}</p>
<p><a href="/login" style="color:#2dd4bf">Try again</a></p>
</body></html>`;
  }
}
