/**
 * QuantEdge Cloudflare Worker — kite.js v4.5
 *
 * Changelog v4.5 (07-Jun-2026) — Intraday monitoring + Deduplication:
 *   FIX1. Intraday re-scan: pipeline now fires 4× per trading day
 *         09:30 IST (market open), 11:30 IST, 13:30 IST, 14:30 IST.
 *         wrangler.toml must add crons: "0 6 * * 2-6", "0 8 * * 2-6", "0 9 * * 2-6"
 *   FIX2. Telegram deduplication: pipeDispatchTelegram() now loads
 *         qe_pipe_alerted_today {date, symbols[]} from KV before dispatch.
 *         Any symbol already alerted today is skipped with DEDUP_SKIP audit log.
 *         On successful send, symbol is persisted back to KV immediately.
 *         KV key auto-expires after 24h (expirationTtl: 86400).
 *         Date comparison uses IST (Asia/Kolkata) — resets correctly at midnight IST.
 *
 *   ROOT CAUSE: Yahoo Finance v10/quoteSummary requires crumb authentication
 *   since late 2024. Both browser and Worker calls fail (crumb/IP blocked).
 *   FIX: Worker now fetches Screener.in directly (no CORS restriction server-side).
 *   parseScreenerFundamentals() parses PE, ROE, RevGr, ProfGr, D/E via regex
 *   (no DOMParser — not available in Cloudflare Workers).
 *   Returns { fundamentals: { pe, roe, revGr, profGr, de, mcap } } to browser.
 *   Browser _fetchFundamentalsYahoo() updated to read new response shape.
 *
 * Changelog v4.3 (05-Jun-2026) — Fundamental data bug fix:
 *   BUGFIX: type=fundamentals handler returned Kite quote data (last_price,
 *           volume, ohlc) — not the fundamental fields (pe, roe, revGr,
 *           profGr, de) the browser parser expected. Browser parsed
 *           json.quoteSummary which was undefined → null → silent fallback
 *           → all fundamental cards showed N/A on every scan.
 *   FIX:    Handler now proxies Yahoo Finance quoteSummary API
 *           (financialData + defaultKeyStatistics + summaryDetail modules).
 *           Returns { quoteSummary } matching exact shape _fetchFundamentalsYahoo()
 *           already parses — zero browser-side changes required.
 *           Tries query1.finance.yahoo.com then query2 as fallback.
 *           No Kite token required for this endpoint.
 *
 * Changelog v4.2 (04-Jun-2026) — Signal Integrity + Macro Snapshot:
 *   FIX2. Telegram signal integrity gate added to pipeDispatchTelegram():
 *         Gate criteria: DS >= 60, Supertrend bullish, ADX >= 18.
 *         Failed candidates dispatched as WATCH_ONLY — never as BUY-eligible.
 *         watchOnly flag stored in KV signal for browser to read.
 *         Gate pass/fail logged to pipeline audit at S9_TELEGRAM stage.
 *   FIX3. Pipeline regime snapshot: computePipelineRegime() derives structural
 *         regime (bull/sideways/bear) from Nifty closes at pipeline run time.
 *         pipelineRegime embedded in every qe_pipe_signals KV entry.
 *         Browser runDeepOnCandidates() uses pipelineRegime instead of
 *         current browser _regime — deep analysis is now deterministic.
 *
 * Changelog v4.1 (02-Jun-2026) — Critical Fixes:
 *   CF1. OHLCV history range: 180 → 365 days (EMA200 now has reliable 260 trading days)
 *   CF2. OHLCV cap: max 80 symbols, sorted by volume desc before cap (highest liquidity first)
 *        Per-symbol fetch timeout: 12s AbortController on both quote + historical calls
 *        New KV stat: ohlcvQueue + ohlcvCapped in lastRun summary
 *   CF3. Sector map: expanded from ~120 → ~350 symbols across 22 sectors
 *        Reduces OTHER bucket, improves sector concentration control accuracy
 *
 * Changelog v4.0 (01-Jun-2026):
 *   All v3.1 routes preserved UNCHANGED.
 *   New additions — SERVER-SIDE DISCOVERY PIPELINE:
 *
 *   ARCHITECTURE:
 *     Universe (KV) → Bhav Copy ingest → OHLCV batch fetch + compute
 *     → Stream A filters → RS Engine → Sector Engine → Merge Engine
 *     → Survivorship Tracking → Audit System → KV signal store
 *     → Telegram Dispatch → Browser reads KV (Part 3)
 *
 *   NEW CRON SCHEDULES:
 *     04:00 UTC Mon–Fri (09:30 IST) — Bhav Copy ingest + pipeline trigger
 *     04:30 UTC Mon–Fri (10:00 IST) — Pipeline completion check + Telegram dispatch
 *
 *   NEW KV KEYS (qe_pipe_* namespace — zero collision with qe_db_*):
 *     qe_pipe_run_id          — current pipeline run UUID
 *     qe_pipe_status          — pipeline status JSON {phase, pct, startedAt, ...}
 *     qe_pipe_bhav_date       — last bhav copy date ingested (YYYY-MM-DD)
 *     qe_pipe_bhav_raw        — raw bhav copy symbol→close map JSON
 *     qe_pipe_ohlcv_{symbol}  — per-symbol computed OHLCV indicators JSON (TTL 24h)
 *     qe_pipe_stream_a        — Stream A filter output: symbols that passed (JSON array)
 *     qe_pipe_rs_ranked       — RS-ranked candidates after Stream A (JSON array)
 *     qe_pipe_sector_map      — sector assignment map JSON
 *     qe_pipe_candidates      — final merged candidates for deep analysis (JSON array)
 *     qe_pipe_signals         — today's completed signals for browser to read (JSON array)
 *     qe_pipe_survivorship    — survivorship log: all eliminated stocks with reason (JSON array)
 *     qe_pipe_audit           — pipeline audit log for current run (JSON array, max 500 entries)
 *     qe_pipe_last_run        — last successful run summary JSON
 *     qe_pipe_nifty_closes    — cached Nifty 50 daily closes for RS calc (JSON array)
 *     qe_pipe_nifty_ts        — nifty closes cache timestamp
 *
 *   NEW ROUTES:
 *     GET  /pipe/trigger      — manually trigger full pipeline run
 *     GET  /pipe/status       — current pipeline run status + progress
 *     GET  /pipe/signals      — read completed signals from KV (browser polls this)
 *     GET  /pipe/candidates   — read candidates list (pre-deep-analysis)
 *     GET  /pipe/audit        — pipeline audit log for last run
 *     GET  /pipe/survivorship — eliminated stocks with rejection reason
 *     POST /pipe/deep-result  — browser posts deep analysis result per symbol
 *
 *   KV KEYS (all — v3.1 existing + v4.0 new):
 *     kite_access_token       — Kite OAuth token (daily)
 *     kite_token_timestamp    — token refresh time
 *     api_secret              — Kite API secret
 *     tg_bot_token            — Telegram bot token
 *     tg_chat_id              — Telegram chat ID
 *     HMAC_SECRET             — signal signing secret
 *     qe_db_universe          — dynamic NSE universe (JSON array of symbols)
 *     qe_db_universe_ts       — universe build timestamp (ms)
 *     qe_db_universe_count    — universe stock count
 *     qe_signals              — active signals (legacy v2.0 — preserved)
 *     qe_gtt_log              — GTT placement audit log
 *     qe_watchlist            — watchlist
 *     qe_rejection_log        — rejection analytics
 *     [all qe_pipe_* keys listed above]
 */

const KITE_API_BASE  = "https://api.kite.trade";
const API_KEY        = "x9atdliuwa1evccb";
const KV_TOKEN_KEY   = "kite_access_token";
const QE_URL         = "https://dsivasankarr.github.io/QuantEdge";
const SIGNAL_TTL_MS  = 15 * 60 * 1000; // 15 minutes

// ─── CORS ─────────────────────────────────────────────────────────────────────
const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function cors(body, status = 200, extra = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...CORS, ...extra },
  });
}
function corsErr(msg, status = 400) {
  return cors({ status: "error", message: msg }, status);
}

// ─── Token helpers ────────────────────────────────────────────────────────────
async function getToken(env) {
  const token = await env.KITE_STORE.get(KV_TOKEN_KEY);
  if (!token) throw new Error("Access token not found. Please login at /login");
  return token;
}
function kiteAuthHeader(token) {
  return `token ${API_KEY}:${token}`;
}

// ─── Kite API proxy ───────────────────────────────────────────────────────────
async function kiteRequest(method, path, body, token) {
  const url     = `${KITE_API_BASE}${path}`;
  const headers = {
    "X-Kite-Version": "3",
    Authorization: kiteAuthHeader(token),
  };
  let fetchOptions = { method, headers };
  if (body && method !== "GET") {
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    fetchOptions.body = new URLSearchParams(body).toString();
  }
  const resp = await fetch(url, fetchOptions);
  const data = await resp.json();
  return { ok: resp.ok, status: resp.status, data };
}

// ─── Yahoo Finance proxy ──────────────────────────────────────────────────────
async function proxyYahooFinance(symbol, interval, range) {
  const iv  = interval || "1d";
  const rng = range    || "1y";
  const headers = { "User-Agent": "Mozilla/5.0", "Accept": "application/json" };
  const urls = [
    `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=${iv}&range=${rng}&includePrePost=false`,
    `https://query2.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=${iv}&range=${rng}&includePrePost=false`,
  ];
  for (const yfUrl of urls) {
    try {
      const ctrl  = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 12000);
      const res   = await fetch(yfUrl, { signal: ctrl.signal, headers });
      clearTimeout(timer);
      if (!res.ok) continue;
      const data = await res.json();
      if (!data.chart || !data.chart.result || !data.chart.result[0]) continue;
      return new Response(JSON.stringify(data), {
        status: 200,
        headers: { "Content-Type": "application/json", ...CORS }
      });
    } catch (_) { continue; }
  }
  return new Response(JSON.stringify({
    chart: { result: null, error: "Yahoo Finance unavailable for " + symbol }
  }), { status: 200, headers: { "Content-Type": "application/json", ...CORS } });
}

// ═══════════════════════════════════════════════════════════════════════════════
// TELEGRAM HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

async function getTgCreds(env) {
  const token = await env.KITE_STORE.get("tg_bot_token");
  const chat  = await env.KITE_STORE.get("tg_chat_id");
  return { token, chat, ok: !!(token && chat) };
}

async function sendTelegram(env, text, replyMarkup) {
  const { token, chat, ok } = await getTgCreds(env);
  if (!ok) return false;
  const body = { chat_id: chat, text, parse_mode: "HTML" };
  if (replyMarkup) body.reply_markup = replyMarkup;
  try {
    const resp = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify(body),
    });
    return resp.ok;
  } catch (_) { return false; }
}

async function answerCallback(env, callbackQueryId, text) {
  const { token } = await getTgCreds(env);
  if (!token) return;
  await fetch(`https://api.telegram.org/bot${token}/answerCallbackQuery`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ callback_query_id: callbackQueryId, text, show_alert: false }),
  });
}

async function editTgMessage(env, chatId, messageId, text) {
  const { token } = await getTgCreds(env);
  if (!token) return;
  await fetch(`https://api.telegram.org/bot${token}/editMessageText`, {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({ chat_id: chatId, message_id: messageId,
                              text, parse_mode: "HTML" }),
  });
}

// ─── HMAC verification ────────────────────────────────────────────────────────
async function verifyHmac(env, signalId, symbol, entry, expiry, providedHmac) {
  try {
    const secret  = await env.KITE_STORE.get("HMAC_SECRET") || "QE_DB_v2_SIGNAL_SECRET";
    const enc     = new TextEncoder();
    const data    = `${signalId}|${symbol}|${entry}|${expiry}`;
    const key     = await crypto.subtle.importKey(
      "raw", enc.encode(secret),
      { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
    );
    const sig     = await crypto.subtle.sign("HMAC", key, enc.encode(data));
    const computed = Array.from(new Uint8Array(sig))
      .map(b => ("00" + b.toString(16)).slice(-2)).join("").slice(0, 16);
    return computed === providedHmac;
  } catch (_) { return false; }
}

// ─── HMAC sign helper (pipeline uses this to sign outbound signals) ───────────
async function signPayload(env, signalId, symbol, entry, expiry) {
  try {
    const secret = await env.KITE_STORE.get("HMAC_SECRET") || "QE_DB_v2_SIGNAL_SECRET";
    const enc    = new TextEncoder();
    const data   = `${signalId}|${symbol}|${entry}|${expiry}`;
    const key    = await crypto.subtle.importKey(
      "raw", enc.encode(secret),
      { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
    );
    const sig    = await crypto.subtle.sign("HMAC", key, enc.encode(data));
    return Array.from(new Uint8Array(sig))
      .map(b => ("00" + b.toString(16)).slice(-2)).join("").slice(0, 16);
  } catch (_) { return ""; }
}

// ─── GTT placement helper (shared by UI and Telegram callback) ────────────────
async function placeGTT(env, symbol, entry, sl, t1, t2, quantity, cmp) {
  const token      = await getToken(env);
  const entryF     = parseFloat(entry).toFixed(2);
  const cmpF       = parseFloat(cmp || entry).toFixed(2);

  const condition = JSON.stringify({
    exchange:       "NSE",
    tradingsymbol:  symbol.toUpperCase(),
    trigger_values: [parseFloat(entryF)],
    last_price:     parseFloat(cmpF),
  });

  const orders = JSON.stringify([{
    exchange:         "NSE",
    tradingsymbol:    symbol.toUpperCase(),
    transaction_type: "BUY",
    quantity:         parseInt(quantity, 10),
    order_type:       "LIMIT",
    product:          "CNC",
    price:            parseFloat(entryF),
  }]);

  const { ok, data } = await kiteRequest(
    "POST", "/gtt/triggers",
    { type: "single", condition, orders },
    token
  );

  if (!ok) throw new Error(data.message || "GTT creation failed");
  const triggerId = data.data.trigger_id;

  await appendGttLog(env, {
    timestamp:  new Date().toISOString(),
    symbol:     symbol.toUpperCase(),
    entry:      parseFloat(entryF),
    sl:         sl    ? parseFloat(sl)   : null,
    t1:         t1    ? parseFloat(t1)   : null,
    t2:         t2    ? parseFloat(t2)   : null,
    quantity:   parseInt(quantity, 10),
    trigger_id: triggerId,
    source:     "telegram_approval",
  });

  return triggerId;
}

async function appendGttLog(env, entry) {
  try {
    const raw = await env.KITE_STORE.get("qe_gtt_log");
    const log = raw ? JSON.parse(raw) : [];
    log.unshift(entry);
    await env.KITE_STORE.put("qe_gtt_log", JSON.stringify(log.slice(0, 200)));
  } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRIORITY 1 — TELEGRAM CALLBACK HANDLER
// ═══════════════════════════════════════════════════════════════════════════════
async function handleTelegramCallback(request, env) {
  let update;
  try { update = await request.json(); } catch (_) { return cors({ ok: true }); }

  const cq = update.callback_query;
  if (!cq) return cors({ ok: true });

  const callbackQueryId = cq.id;
  const messageId       = cq.message && cq.message.message_id;
  const chatId          = cq.message && cq.message.chat && cq.message.chat.id;

  let payload;
  try { payload = JSON.parse(cq.data); } catch (_) {
    await answerCallback(env, callbackQueryId, "Invalid signal data.");
    return cors({ ok: true });
  }

  const { action, signalId, symbol, entry, sl, t1, t2, qty, cmp, expiry, hmac } = payload;

  if (!expiry || Date.now() > expiry) {
    await answerCallback(env, callbackQueryId, "⏱ Signal expired. Run a new scan.");
    await editTgMessage(env, chatId, messageId,
      `⏱ <b>Signal Expired — ${symbol}</b>\nRun a fresh Discovery scan for new signals.`);
    return cors({ ok: true });
  }

  const valid = await verifyHmac(env, signalId, symbol, entry, expiry, hmac);
  if (!valid) {
    await answerCallback(env, callbackQueryId, "❌ Invalid signal signature.");
    return cors({ ok: true });
  }

  if (action === "BUY") {
    try {
      const raw = await env.KITE_STORE.get("qe_gtt_log");
      const log = raw ? JSON.parse(raw) : [];
      const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
      const dup = log.some(function(g) {
        return g.symbol === symbol.toUpperCase() &&
               new Date(g.timestamp).getTime() > cutoff;
      });
      if (dup) {
        await answerCallback(env, callbackQueryId,
          `⚠️ Duplicate: GTT already placed for ${symbol} within 7 days.`);
        return cors({ ok: true });
      }

      const triggerId = await placeGTT(env, symbol, entry, sl, t1, t2, qty, cmp);

      await answerCallback(env, callbackQueryId, `✅ GTT placed for ${symbol}!`);
      await editTgMessage(env, chatId, messageId,
        `✅ <b>GTT Placed — ${symbol}</b>\n\n`
        + `Entry: ₹${entry} | SL: ₹${sl}\n`
        + `T1: ₹${t1} | Qty: ${qty}\n`
        + `Trigger ID: <code>${triggerId}</code>\n`
        + `<i>Source: Discovery Engine v3.0</i>`
      );

      await sendTelegram(env,
        `✅ <b>GTT Confirmed — ${symbol}</b>\n`
        + `Entry: ₹${entry} | SL: ₹${sl} | T1: ₹${t1}\n`
        + `Qty: ${qty} | Trigger: #${triggerId}`
      );

    } catch (e) {
      await answerCallback(env, callbackQueryId, `❌ GTT failed: ${e.message}`);
      await editTgMessage(env, chatId, messageId,
        `❌ <b>GTT Failed — ${symbol}</b>\n${e.message}`
      );
    }
  }

  else if (action === "WATCH") {
    try {
      const raw = await env.KITE_STORE.get("qe_watchlist") || "[]";
      const wl  = JSON.parse(raw);
      if (!wl.find(function(w) { return w.symbol === symbol; })) {
        wl.unshift({ symbol, entry, sl, t1, addedAt: new Date().toISOString(), signalId });
        await env.KITE_STORE.put("qe_watchlist", JSON.stringify(wl.slice(0, 50)));
      }
    } catch (_) {}

    await answerCallback(env, callbackQueryId, `👀 ${symbol} added to watchlist`);
    await editTgMessage(env, chatId, messageId,
      `👀 <b>Watching — ${symbol}</b>\n`
      + `Entry: ₹${entry} | SL: ₹${sl}\n`
      + `<i>Will alert on breakout or score improvement</i>`
    );
  }

  else if (action === "REJECT") {
    try {
      const raw = await env.KITE_STORE.get("qe_rejection_log") || "[]";
      const rl  = JSON.parse(raw);
      rl.unshift({ symbol, signalId, rejectedAt: new Date().toISOString() });
      await env.KITE_STORE.put("qe_rejection_log", JSON.stringify(rl.slice(0, 200)));
    } catch (_) {}

    await answerCallback(env, callbackQueryId, `❌ ${symbol} rejected`);
    await editTgMessage(env, chatId, messageId,
      `❌ <b>Rejected — ${symbol}</b>\n<i>Logged for analytics</i>`
    );
  }

  return cors({ ok: true });
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRIORITY 2 — DAILY AUTH REMINDER (8:45am IST = 03:15 UTC)
// ═══════════════════════════════════════════════════════════════════════════════
async function sendAuthReminder(env) {
  const tokenTs = await env.KITE_STORE.get("kite_token_timestamp");
  if (tokenTs) {
    const tokenAge = Date.now() - parseInt(tokenTs);
    if (tokenAge < 3 * 60 * 60 * 1000) return;
  }

  const loginUrl = "https://quantedge-kite.siva-d-sankar.workers.dev/login";
  await sendTelegram(env,
    `🔑 <b>QuantEdge — Daily Kite Authorisation</b>\n\n`
    + `Markets open in ~30 minutes.\n`
    + `Tap below to connect Kite for today's session.\n\n`
    + `<a href="${loginUrl}">🔑 Authorise Kite Now</a>\n\n`
    + `<i>Required daily — Zerodha security policy</i>`
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRIORITY 3 — SCHEDULED SCAN TRIGGER (9:15am IST = 03:45 UTC)
// Now triggers the server-side pipeline instead of just sending a deep-link.
// Bridge: if pipeline is not yet built (v4.0 deploy day), falls back to deep-link.
// ═══════════════════════════════════════════════════════════════════════════════
async function triggerDiscoveryScan(env) {
  const token = await env.KITE_STORE.get(KV_TOKEN_KEY);

  if (!token) {
    await sendTelegram(env,
      `⚠️ <b>QuantEdge Discovery — Blocked</b>\n\n`
      + `Kite not authorised for today.\n`
      + `<a href="https://quantedge-kite.siva-d-sankar.workers.dev/login">🔑 Login first</a>, `
      + `then run Discovery manually.`
    );
    return;
  }

  // v4.0: trigger server-side pipeline
  await sendTelegram(env,
    `🔭 <b>QuantEdge Discovery — Pipeline Starting</b>\n\n`
    + `Kite connected ✅\n`
    + `Server-side pipeline triggered at market open.\n`
    + `Phase 1: Bhav Copy ingest → Stream A filters → RS ranking\n\n`
    + `<i>Candidates will appear in QuantEdge Discovery panel (~10 min)</i>`
  );

  // Fire-and-forget: run pipeline in background
  // ctx.waitUntil not available here — use direct call
  // Pipeline will write results to KV; browser polls /pipe/signals
  try {
    await runFullPipeline(env);
  } catch (e) {
    await sendTelegram(env,
      `⚠️ <b>Pipeline Error</b>\n\n${e.message}\n\n`
      + `<a href="${QE_URL}">Open QuantEdge to run manually</a>`
    );
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRIORITY 4 — POSITION MONITOR (every 30 min, market hours)
// ═══════════════════════════════════════════════════════════════════════════════
async function monitorPositions(env) {
  let token;
  try { token = await getToken(env); } catch (_) { return; }

  const { ok, data } = await kiteRequest("GET", "/gtt/triggers", null, token);
  if (!ok) return;

  const activeKiteGTTs = (data.data || []).filter(function(g) { return g.status === "active"; });

  const raw    = await env.KITE_STORE.get("qe_gtt_log");
  const ourLog = raw ? JSON.parse(raw) : [];

  const kiteIds = new Set(activeKiteGTTs.map(function(g) { return String(g.id); }));
  const alerts  = [];

  for (let i = 0; i < ourLog.length; i++) {
    const logged = ourLog[i];
    if (!logged.trigger_id) continue;
    const triggered = !kiteIds.has(String(logged.trigger_id));
    if (triggered && !logged.alerted) {
      alerts.push(logged);
      logged.alerted   = true;
      logged.alertedAt = new Date().toISOString();
    }
    const age = (Date.now() - new Date(logged.timestamp).getTime()) / (1000 * 60 * 60 * 24);
    if (age > 25 && !logged.staleAlerted) {
      alerts.push(Object.assign({}, logged, { stale: true }));
      logged.staleAlerted = true;
    }
  }

  if (alerts.length) {
    await env.KITE_STORE.put("qe_gtt_log", JSON.stringify(ourLog.slice(0, 200)));
    for (let j = 0; j < alerts.length; j++) {
      const a = alerts[j];
      if (a.stale) {
        await sendTelegram(env,
          `⏰ <b>Stale Position — ${a.symbol}</b>\n\n`
          + `GTT open for >25 days.\n`
          + `Entry: ₹${a.entry} | SL: ₹${a.sl} | T1: ₹${a.t1}\n`
          + `Trigger ID: #${a.trigger_id}\n\n`
          + `Consider reviewing this position.`
        );
      } else {
        await sendTelegram(env,
          `🎯 <b>GTT Triggered — ${a.symbol}</b>\n\n`
          + `Your GTT order has been activated on Kite.\n`
          + `Entry: ₹${a.entry} | Qty: ${a.quantity}\n`
          + `Check Kite for execution status.\n`
          + `<a href="https://kite.zerodha.com/orders">View in Kite →</a>`
        );
      }
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRIORITY 5 — DAILY SUMMARY (4:00pm IST = 10:30 UTC)
// ═══════════════════════════════════════════════════════════════════════════════
async function sendDailySummary(env) {
  let token;
  try { token = await getToken(env); } catch (_) {
    await sendTelegram(env, `📊 <b>QuantEdge Daily Summary</b>\n\n⚠️ Kite not connected today.`);
    return;
  }

  const { ok, data } = await kiteRequest("GET", "/gtt/triggers", null, token);
  const activeGTTs   = ok ? (data.data || []).filter(function(g) { return g.status === "active"; }) : [];

  const raw    = await env.KITE_STORE.get("qe_gtt_log");
  const gttLog = raw ? JSON.parse(raw) : [];
  const today  = new Date().toISOString().slice(0, 10);
  const todayGTTs = gttLog.filter(function(g) {
    return g.timestamp && g.timestamp.startsWith(today);
  });

  const rjRaw  = await env.KITE_STORE.get("qe_rejection_log");
  const rejLog = rjRaw ? JSON.parse(rjRaw) : [];
  const todayRejections = rejLog.filter(function(r) {
    return r.rejectedAt && r.rejectedAt.startsWith(today);
  });

  // v4.0: include pipeline summary
  let pipelineSummary = "";
  try {
    const pipeRaw = await env.KITE_STORE.get("qe_pipe_last_run");
    if (pipeRaw) {
      const pr = JSON.parse(pipeRaw);
      if (pr.runDate === today) {
        pipelineSummary = `\n🔭 <b>Discovery Pipeline</b>\n`
          + `  Universe: ${pr.universeCount} → RS passed: ${pr.rsPassCount}\n`
          + `  Stream A passed: ${pr.streamACount} → Candidates: ${pr.candidateCount}\n`
          + `  Signals dispatched: ${pr.signalCount}\n`;
      }
    }
  } catch (_) {}

  const capitalDeployed = todayGTTs.reduce(function(sum, g) {
    return sum + (g.entry * g.quantity);
  }, 0);

  const msg = `📊 <b>QuantEdge Daily Summary — ${today}</b>\n\n`
    + `🔭 GTTs placed today: <b>${todayGTTs.length}</b>\n`
    + `❌ Signals rejected: <b>${todayRejections.length}</b>\n`
    + `📋 Total active GTTs: <b>${activeGTTs.length}</b>\n`
    + `💰 Capital deployed today: <b>₹${capitalDeployed.toLocaleString("en-IN")}</b>\n`
    + pipelineSummary
    + "\n"
    + (todayGTTs.length
      ? todayGTTs.map(function(g) {
          return `  • ${g.symbol} @ ₹${g.entry} × ${g.quantity} = ₹${(g.entry * g.quantity).toLocaleString("en-IN")}`;
        }).join("\n") + "\n\n"
      : "  No new positions today.\n\n")
    + `<i>QuantEdge Discovery Engine v3.0</i>`;

  await sendTelegram(env, msg);
}

// ─── /kv/get  (read KV from frontend) ────────────────────────────────────────
async function handleKvGet(url, env) {
  const key = url.searchParams.get("key");
  if (!key) return corsErr("Missing key");
  // Extended allowed list — v4.0 pipe keys added
  const allowed = [
    "qe_db_universe", "qe_db_universe_ts", "qe_watchlist",
    "qe_pipe_signals", "qe_pipe_candidates", "qe_pipe_status",
    "qe_pipe_audit",   "qe_pipe_survivorship", "qe_pipe_last_run",
  ];
  if (!allowed.includes(key)) return corsErr("Key not readable", 403);
  try {
    const value = await env.KITE_STORE.get(key);
    return cors({ key, value: value || null });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// ─── /tg/register  (store TG credentials from QuantEdge UI) ──────────────────
async function handleTgRegister(request, env) {
  let body;
  try { body = await request.json(); } catch (_) { return corsErr("Invalid JSON"); }
  const { bot_token, chat_id } = body;
  if (!bot_token || !chat_id) return corsErr("Required: bot_token, chat_id");
  await env.KITE_STORE.put("tg_bot_token", bot_token);
  await env.KITE_STORE.put("tg_chat_id",   String(chat_id));
  return cors({ status: "success", message: "Telegram credentials stored in KV" });
}

// ─── /signal/store  (store signal payload for callback verification) ─────────
async function handleSignalStore(request, env) {
  let body;
  try { body = await request.json(); } catch (_) { return corsErr("Invalid JSON"); }
  const { signalId, symbol, entry, sl, t1, t2, qty, cmp, expiry, hmac } = body;
  if (!signalId || !symbol || !expiry) return corsErr("Required: signalId, symbol, expiry");
  if (Date.now() > expiry) return corsErr("Signal already expired", 400);
  await env.KITE_STORE.put(
    `qe_signal_${signalId}`,
    JSON.stringify({ signalId, symbol, entry, sl, t1, t2, qty, cmp, expiry, hmac }),
    { expirationTtl: 1800 }
  );
  return cors({ status: "success", signalId });
}

// ═══════════════════════════════════════════════════════════════════════════════
// UNIVERSE MANAGER (v3.1 — preserved exactly)
// ═══════════════════════════════════════════════════════════════════════════════
async function buildUniverse(env) {
  let token;
  try {
    token = await getToken(env);
  } catch (e) {
    return { ok: false, error: "Kite token not available: " + e.message, count: 0 };
  }

  let csv;
  try {
    const resp = await fetch(`${KITE_API_BASE}/instruments/NSE`, {
      headers: {
        "X-Kite-Version": "3",
        "Authorization": kiteAuthHeader(token),
      },
    });
    if (!resp.ok) {
      return { ok: false, error: "Kite instruments fetch failed: HTTP " + resp.status, count: 0 };
    }
    csv = await resp.text();
  } catch (e) {
    return { ok: false, error: "Kite instruments fetch error: " + e.message, count: 0 };
  }

  const lines = csv.split("\n");
  if (lines.length < 2) {
    return { ok: false, error: "Empty instruments CSV returned", count: 0 };
  }

  const headers = lines[0].split(",").map(function(h) { return h.trim().replace(/"/g, ""); });
  const colTradingsymbol  = headers.indexOf("tradingsymbol");
  const colInstrumentType = headers.indexOf("instrument_type");
  const colLastPrice      = headers.indexOf("last_price");
  const colExchange       = headers.indexOf("exchange");

  if (colTradingsymbol < 0 || colInstrumentType < 0) {
    return { ok: false, error: "CSV missing required columns. Got: " + headers.join(","), count: 0 };
  }

  const symbols = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const cols = line.split(",");
    if (cols.length < headers.length) continue;

    const instrType = (cols[colInstrumentType] || "").trim().replace(/"/g, "");
    const exchange  = colExchange >= 0 ? (cols[colExchange] || "").trim().replace(/"/g, "") : "NSE";
    const symbol    = (cols[colTradingsymbol] || "").trim().replace(/"/g, "");
    const lastPrice = colLastPrice >= 0 ? parseFloat(cols[colLastPrice]) : 0;

    if (instrType !== "EQ") continue;
    if (exchange !== "NSE") continue;
    if (!symbol) continue;
    if (lastPrice > 0 && lastPrice < 100) continue;
    if (/[-&]/.test(symbol) && symbol !== "BAJAJ-AUTO") continue;

    symbols.push(symbol);
  }

  if (symbols.length < 50) {
    return { ok: false, error: "Too few symbols after filter: " + symbols.length, count: 0 };
  }

  const ts = Date.now();
  try {
    await env.KITE_STORE.put("qe_db_universe",       JSON.stringify(symbols));
    await env.KITE_STORE.put("qe_db_universe_ts",    String(ts));
    await env.KITE_STORE.put("qe_db_universe_count", String(symbols.length));
  } catch (e) {
    return { ok: false, error: "KV write failed: " + e.message, count: symbols.length };
  }

  return {
    ok:      true,
    count:   symbols.length,
    builtAt: new Date(ts).toISOString(),
    sample:  symbols.slice(0, 10),
  };
}

async function handleUniverseRefresh(env) {
  const result = await buildUniverse(env);
  if (!result.ok) {
    return corsErr("Universe build failed: " + result.error, 500);
  }
  return cors({
    status:   "success",
    count:    result.count,
    built_at: result.builtAt,
    sample:   result.sample,
    message:  `Universe built: ${result.count} NSE EQ stocks (price > ₹100)`,
  });
}

async function handleUniverseStatus(env) {
  try {
    const ts    = await env.KITE_STORE.get("qe_db_universe_ts");
    const count = await env.KITE_STORE.get("qe_db_universe_count");
    const hasUniverse = !!(await env.KITE_STORE.get("qe_db_universe"));
    const ageMs   = ts ? Date.now() - parseInt(ts) : null;
    const ageDays = ageMs ? Math.floor(ageMs / (1000 * 60 * 60 * 24)) : null;
    return cors({
      status:       "success",
      has_universe: hasUniverse,
      count:        count ? parseInt(count) : 0,
      built_at:     ts ? new Date(parseInt(ts)).toISOString() : null,
      age_days:     ageDays,
      stale:        ageDays === null ? true : ageDays > 7,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
// SERVER-SIDE DISCOVERY PIPELINE — v4.0
// ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
//
// Pipeline stages (all server-side):
//   Stage 1 — Universe load          (KV read)
//   Stage 2 — NSE Bhav Copy ingest   (Kite /quote bulk OR instruments CSV last_price)
//   Stage 3 — OHLCV fetch + compute  (Kite historical per symbol, batched)
//   Stage 4 — Stream A filters       (EMA stack, RSI, ADX, volume, ATR coil)
//   Stage 5 — RS Engine              (percentile rank vs Nifty, 3-period weighted)
//   Stage 6 — Sector Engine          (sector concentration limit + sector RS)
//   Stage 7 — Merge Engine           (rank by combined RS + StreamA score, top N)
//   Stage 8 — Survivorship write     (all eliminated symbols logged with reason)
//   Stage 9 — Audit write            (full trace per pipeline run)
//   Stage 10 — KV signal store       (candidates JSON written for browser to read)
//   Stage 11 — Telegram dispatch     (top signals sent with BUY/WATCH/REJECT buttons)
//
// Design constraints:
//   - Total pipeline CPU budget: Cloudflare Workers paid = 30s per invocation
//   - Universe is ~800–1400 symbols; OHLCV batch is ONLY run on Stream A candidates
//   - Stream A pre-filter uses Bhav Copy (last_price) only — zero historical API calls
//   - Historical fetch only on symbols that pass Stream A (typically 40–80 symbols)
//   - Batch size: 5 symbols per fetch batch, 300ms delay between batches
//   - Worker timeout defence: pipeline writes checkpoint to KV at each stage
//
// ═══════════════════════════════════════════════════════════════════════════════

// ─── Pipeline constants ───────────────────────────────────────────────────────
const PIPE_BATCH_SIZE    = 5;     // symbols per OHLCV batch
const PIPE_BATCH_DELAY   = 300;   // ms between OHLCV batches
const PIPE_MIN_CANDLES   = 200;   // minimum daily bars required (EMA200 needs ~200 bars)
const PIPE_RS_THRESHOLD  = 55;    // RS percentile cutoff for Stream A pass
const PIPE_MAX_SECTOR_N  = 3;     // max candidates per sector in final output
const PIPE_TOP_N         = 20;    // max candidates to write to KV for browser
const PIPE_SIGNAL_TOP    = 5;     // max signals dispatched via Telegram per run
// Critical Fix 1: 365 calendar days ≈ 260 trading days — sufficient for reliable EMA200
const PIPE_OHLCV_RANGE   = 365;   // days of history to fetch (1 year, EMA200-safe)
const PIPE_NIFTY_TTL_MS  = 4 * 60 * 60 * 1000; // nifty cache valid 4h
// Critical Fix 2: Cap OHLCV processing to protect Worker CPU budget
// Universe after Stream A Fast is typically 80–200 symbols.
// At ~1.5s per symbol (quote + historical), 100 symbols ≈ 150s — exceeds cron budget.
// Cap at 80: sorted by volume descending so highest-liquidity stocks are processed first.
// Symbols beyond cap are survivorship-logged as OHLCV_CAP_EXCEEDED.
const PIPE_MAX_OHLCV_CAP = 80;    // max symbols entering OHLCV fetch
// Per-symbol fetch timeout — prevents a single slow Kite response stalling the batch
const PIPE_SYMBOL_TIMEOUT_MS = 12000; // 12s per symbol (quote + historical)

// ─── Sector map (NSE tradingsymbols → sector label) ──────────────────────────
// Covers ~350 liquid NSE stocks. Unlisted symbols default to "OTHER".
// Expanded in v4.0 (Critical Fix 3) to reduce OTHER bucket and improve
// sector concentration control accuracy.
const SECTOR_MAP = {
  // ── BANKING ────────────────────────────────────────────────────────────────
  HDFCBANK:"BANK", ICICIBANK:"BANK", SBIN:"BANK", KOTAKBANK:"BANK", AXISBANK:"BANK",
  INDUSINDBK:"BANK", BANKBARODA:"BANK", PNB:"BANK", CANBK:"BANK", UNIONBANK:"BANK",
  IDFCFIRSTB:"BANK", BANDHANBNK:"BANK", AUBANK:"BANK", FEDERALBNK:"BANK",
  YESBANK:"BANK", IDBI:"BANK", RBLBANK:"BANK", DCBBANK:"BANK",
  KARNATAKBANK:"BANK", CSBBANK:"BANK", SOUTHBANK:"BANK", KARURVYSYA:"BANK",
  TMVHL:"BANK", UJJIVANSFB:"BANK", ESAFSFB:"BANK", EQUITASBNK:"BANK",
  // ── NBFC / FINSERV ─────────────────────────────────────────────────────────
  BAJFINANCE:"NBFC", BAJAJFINSV:"NBFC", CHOLAFIN:"NBFC", RECLTD:"NBFC", PFC:"NBFC",
  IRFC:"NBFC", HUDCO:"NBFC", PNBHOUSING:"NBFC", IIFL:"NBFC", MUTHOOTFIN:"NBFC",
  MANAPPURAM:"NBFC", MAHINDCIE:"NBFC", M_MFIN:"NBFC", CREDITACC:"NBFC",
  SUNDARMFIN:"NBFC", LTFH:"NBFC", SHRIRAMFIN:"NBFC", MASFIN:"NBFC",
  HDFCAMC:"FINSERV", NIPPONLIFE:"FINSERV", ANGELONE:"FINSERV", CDSL:"FINSERV",
  BSE:"FINSERV", MCX:"FINSERV", CAMS:"FINSERV", MOTILALOS:"FINSERV",
  "360ONE":"FINSERV", NUVAMA:"FINSERV", KFINTECH:"FINSERV", UTIAMC:"FINSERV",
  HDFCLIFE:"INSURANCE", SBILIFE:"INSURANCE", ICICIPRULI:"INSURANCE",
  ICICIGI:"INSURANCE", STARHEALTH:"INSURANCE", GICRE:"INSURANCE",
  NIACL:"INSURANCE", LICI:"INSURANCE",
  // ── IT & TECH ──────────────────────────────────────────────────────────────
  TCS:"IT", INFY:"IT", HCLTECH:"IT", WIPRO:"IT", TECHM:"IT", LTIM:"IT",
  MPHASIS:"IT", COFORGE:"IT", PERSISTENT:"IT", KPITTECH:"IT", TATAELXSI:"IT",
  LTTS:"IT", CYIENT:"IT", MASTEK:"IT", INTELLECT:"IT", NEWGEN:"IT", AFFLE:"IT",
  TANLA:"IT", ROUTE:"IT", OFSS:"IT", HEXAWARE:"IT", BIRLASOFT:"IT",
  NIITTECH:"IT", ZENSAR:"IT", SONATSOFTW:"IT", RATEGAIN:"IT", NETWEB:"IT",
  TATACOMM:"IT", GTLINFRA:"IT",
  // ── PHARMA ─────────────────────────────────────────────────────────────────
  SUNPHARMA:"PHARMA", DRREDDY:"PHARMA", CIPLA:"PHARMA", DIVISLAB:"PHARMA",
  ZYDUSLIFE:"PHARMA", LUPIN:"PHARMA", ALKEM:"PHARMA", TORNTPHARM:"PHARMA",
  NATCOPHARM:"PHARMA", GRANULES:"PHARMA", AUROPHARMA:"PHARMA", IPCALAB:"PHARMA",
  GLENMARK:"PHARMA", ABBOTINDIA:"PHARMA", LAURUSLABS:"PHARMA", JBCHEPHARM:"PHARMA",
  SUVEN:"PHARMA", PFIZER:"PHARMA", SANOFI:"PHARMA", AJANTPHARM:"PHARMA",
  GLAXO:"PHARMA", JUBLPHARMA:"PHARMA", GLAND:"PHARMA", ERIS:"PHARMA",
  LAXMI_N_INC:"PHARMA", SEQUENT:"PHARMA", SOLARA:"PHARMA",
  // ── HEALTHCARE ─────────────────────────────────────────────────────────────
  APOLLOHOSP:"HEALTHCARE", FORTIS:"HEALTHCARE", NARAYANA:"HEALTHCARE",
  LALPATHLAB:"HEALTHCARE", METROPOLIS:"HEALTHCARE", MAXHEALTH:"HEALTHCARE",
  THYROCARE:"HEALTHCARE", KRSNAA:"HEALTHCARE", VIJAYA:"HEALTHCARE",
  MEDANTA:"HEALTHCARE", ASTER:"HEALTHCARE", RAINBOW:"HEALTHCARE",
  // ── AUTO & EV ──────────────────────────────────────────────────────────────
  MARUTI:"AUTO", TATAMOTORS:"AUTO", EICHERMOT:"AUTO", "BAJAJ-AUTO":"AUTO",
  HEROMOTOCO:"AUTO", ASHOKLEY:"AUTO", MOTHERSON:"AUTO", SUNDRMFAST:"AUTO",
  BHARATFORG:"AUTO", SANSERA:"AUTO", BALKRISIND:"AUTO", MRF:"AUTO",
  BOSCHLTD:"AUTO", TIINDIA:"AUTO", CRAFTSMAN:"AUTO", GABRIEL:"AUTO",
  SUPRAJIT:"AUTO", ENDURANCE:"AUTO", MNFL:"AUTO", SCHAEFFLER:"AUTO",
  EXIDEIND:"AUTO", AMARAJABAT:"AUTO", LUMAXTECH:"AUTO", SUBROS:"AUTO",
  SHREECEM:"AUTO", SPARKMINDA:"AUTO", VARROC:"AUTO",
  // ── CAPITAL GOODS ──────────────────────────────────────────────────────────
  LT:"CAPGOODS", SIEMENS:"CAPGOODS", ABB:"CAPGOODS",
  THERMAX:"CAPGOODS", CUMMINSIND:"CAPGOODS",
  ELECON:"CAPGOODS", RATNAMANI:"CAPGOODS", WELCORP:"CAPGOODS",
  AHLUCONT:"CAPGOODS", KSB:"CAPGOODS", GRINDWELL:"CAPGOODS",
  SKFINDIA:"CAPGOODS", TIMKEN:"CAPGOODS", VOLTAMP:"CAPGOODS",
  KECL:"CAPGOODS", KALPATPOWR:"CAPGOODS", BHEL:"CAPGOODS",
  RAILVIKAS:"CAPGOODS", RVNL:"CAPGOODS", IRCON:"CAPGOODS",
  NBCC:"CAPGOODS", WABCO:"CAPGOODS", ELGIEQUIP:"CAPGOODS",
  // ── DEFENCE ────────────────────────────────────────────────────────────────
  BEL:"DEFENCE", HAL:"DEFENCE", COCHINSHIP:"DEFENCE", MAZAGON:"DEFENCE",
  GRSE:"DEFENCE", SOLARINDS:"DEFENCE", MTAR:"DEFENCE", HBLENGINE:"DEFENCE",
  DATAPATTNS:"DEFENCE", PARAS:"DEFENCE", ZEN:"DEFENCE", IDEAFORGE:"DEFENCE",
  ROSSARI:"DEFENCE", MIDHANI:"DEFENCE",
  // ── ENERGY / OIL & GAS ─────────────────────────────────────────────────────
  ONGC:"ENERGY", BPCL:"ENERGY", IOC:"ENERGY", HINDPETRO:"ENERGY",
  GAIL:"ENERGY", IGL:"ENERGY", MGL:"ENERGY", PETRONET:"ENERGY",
  OIL:"ENERGY", MRPL:"ENERGY", CHENNPETRO:"ENERGY",
  // ── POWER ──────────────────────────────────────────────────────────────────
  POWERGRID:"POWER", NTPC:"POWER", TATAPOWER:"POWER", CESC:"POWER",
  TORNTPOWER:"POWER", JSWENERGY:"POWER", SUZLON:"POWER", INOXWIND:"POWER",
  SJVN:"POWER", NHPC:"POWER", RECLTD:"POWER", GREENPWR:"POWER",
  ADANIPOWER:"POWER", ADANIGREEN:"POWER",
  // ── METALS & MINING ────────────────────────────────────────────────────────
  TATASTEEL:"METALS", JSWSTEEL:"METALS", HINDALCO:"METALS",
  COALINDIA:"METALS", NMDC:"METALS", VEDL:"METALS",
  NALCO:"METALS", MOIL:"METALS", SAIL:"METALS", JINDALSTEL:"METALS",
  WELSPUNIND:"METALS", APLAPOLLO:"METALS", ASHAPURMIN:"METALS",
  RAMKRISHNA:"METALS", GPPL:"METALS", HINDCOPPER:"METALS",
  // ── FMCG / CONSUMER ────────────────────────────────────────────────────────
  HINDUNILVR:"FMCG", NESTLEIND:"FMCG", DABUR:"FMCG", MARICO:"FMCG",
  GODREJCP:"FMCG", EMAMILTD:"FMCG", BRITANNIA:"FMCG", TATACONSUM:"FMCG",
  COLPAL:"FMCG", VBL:"FMCG", RADICO:"FMCG", UBL:"FMCG",
  MCDOWELL_N:"FMCG", ITC:"FMCG", GODFRYPHLP:"FMCG", VSTIND:"FMCG",
  PATANJALI:"FMCG", BAJAJCON:"FMCG", JYOTHYLAB:"FMCG",
  // ── RETAIL ─────────────────────────────────────────────────────────────────
  TRENT:"RETAIL", DMART:"RETAIL", PAGEIND:"RETAIL", MANYAVAR:"RETAIL",
  METRO:"RETAIL", BATAINDIA:"RETAIL", SHOPERSTOP:"RETAIL",
  VMART:"RETAIL", ZUDIO:"RETAIL",
  // ── HOSPITALITY / TRAVEL ───────────────────────────────────────────────────
  INDHOTEL:"HOSPITALITY", LEMONTREE:"HOSPITALITY", CHALET:"HOSPITALITY",
  EIHOTEL:"HOSPITALITY", MAHINDHOLIDAY:"HOSPITALITY",
  IRCTC:"TRAVEL", THOMASCOOK:"TRAVEL", SPICEJET:"TRAVEL",
  INDIGO:"TRAVEL", GMRAIRPORT:"TRAVEL",
  // ── INTERNET / PLATFORM ────────────────────────────────────────────────────
  ZOMATO:"INTERNET", NYKAA:"INTERNET", POLICYBZR:"INTERNET",
  INDIAMART:"INTERNET", NAUKRI:"INTERNET", JUSTDIAL:"INTERNET",
  CARTRADE:"INTERNET", PAYTM:"INTERNET", DELHIVERY:"INTERNET",
  MAPMYINDIA:"INTERNET",
  // ── TELECOM ────────────────────────────────────────────────────────────────
  BHARTIARTL:"TELECOM", TATACOMM:"TELECOM", RAILTEL:"TELECOM",
  HFCL:"TELECOM", STLTECH:"TELECOM",
  // ── CEMENT ─────────────────────────────────────────────────────────────────
  ULTRACEMCO:"CEMENT", JKCEMENT:"CEMENT",
  RAMCOCEM:"CEMENT", DALMIA:"CEMENT", AMBUJACEMENT:"CEMENT",
  ACCLTD:"CEMENT", HEIDELBERG:"CEMENT", BIRLACORPN:"CEMENT",
  NCLIND:"CEMENT", JKLAKSHMI:"CEMENT",
  // ── REALTY ─────────────────────────────────────────────────────────────────
  OBEROIRLTY:"REALTY", GODREJPROP:"REALTY", DLF:"REALTY",
  PRESTIGE:"REALTY", BRIGADE:"REALTY", SOBHA:"REALTY",
  PHOENIXLTD:"REALTY", SUNTECK:"REALTY", MAHLIFE:"REALTY",
  KOLTEPATIL:"REALTY", LODHA:"REALTY", SIGNATURE:"REALTY",
  // ── CHEMICALS ──────────────────────────────────────────────────────────────
  PIIND:"CHEM", DEEPAKNTR:"CHEM", ALKYLAMINE:"CHEM",
  TATACHEM:"CHEM", NAVINFLUOR:"CHEM", UPL:"CHEM",
  AARTI:"CHEM", VINATI:"CHEM", FINEORG:"CHEM", GALAXYSURF:"CHEM",
  CLEAN_SCI:"CHEM", NOCIL:"CHEM", SUDARSCHEM:"CHEM",
  BALAJI_AM:"CHEM", THIRUMALCHM:"CHEM",
  // ── ELECTRONICS & MANUFACTURING ────────────────────────────────────────────
  DIXON:"ELEC", KAYNES:"ELEC", SYRMA:"ELEC", AMBER:"ELEC", PGEL:"ELEC",
  HAVELLS:"ELEC", POLYCAB:"ELEC", VOLTAS:"ELEC",
  BLUESTARCO:"ELEC", VGUARD:"ELEC",
  AVALON:"ELEC", ABSLAMC:"ELEC",
  // ── PAINTS / BUILDING MATERIALS ────────────────────────────────────────────
  ASIANPAINT:"PAINTS", PIDILITIND:"PAINTS", BERGER:"PAINTS",
  KANSAINER:"PAINTS", INDIGO_P:"PAINTS",
  SUPREMEIND:"BUILDMAT", ASTRAL:"BUILDMAT", PRINCEPIPE:"BUILDMAT",
  FINOLEX:"BUILDMAT", CENTURYPLY:"BUILDMAT", GREENPANEL:"BUILDMAT",
  KAJARIACER:"BUILDMAT", ORIENTBELL:"BUILDMAT",
  // ── AGRI / FERTILISERS ─────────────────────────────────────────────────────
  CHAMBLFERT:"AGRI", COROMANDEL:"AGRI", GNFC:"AGRI",
  GSFC:"AGRI", NFL:"AGRI", PARADEEP:"AGRI",
  KAVERI:"AGRI", AVANTIFEED:"AGRI",
  // ── LOGISTICS ──────────────────────────────────────────────────────────────
  CONCOR:"LOGISTICS", BLUEDART:"LOGISTICS", TCI:"LOGISTICS",
  VRL:"LOGISTICS", ALLCARGO:"LOGISTICS", MAHSCOOTER:"LOGISTICS",
  GATI:"LOGISTICS",
  // ── CONGLOMERATE ───────────────────────────────────────────────────────────
  RELIANCE:"CONGLOMERATE", ADANIENT:"CONGLOMERATE",
  // ── LUXURY / LIFESTYLE ─────────────────────────────────────────────────────
  TITAN:"LUXURY", CERA:"LUXURY", PCJEWELLER:"LUXURY",
  SENCO:"LUXURY", KALYAN:"LUXURY",
  // ── MEDIA ──────────────────────────────────────────────────────────────────
  ZEEL:"MEDIA", SUNTV:"MEDIA", PVR:"MEDIA", INOX:"MEDIA",
  SAREGAMA:"MEDIA", TIPS:"MEDIA",
  // ── SMALLCAP SPECIAL SITUATIONS (in universe, need sector for cap control) ─
  ACUTAAS:"SMALLCAP", AEROFLEX:"SMALLCAP", ATHERENERG:"SMALLCAP",
  SPANDANA:"SMALLCAP", APTUS:"SMALLCAP", CAMPUS:"SMALLCAP",
  AAVAS:"SMALLCAP", HOMEFIRST:"SMALLCAP", SPORTKING:"SMALLCAP",
};

// ─── Pipeline audit logger ────────────────────────────────────────────────────
// Appends to in-memory audit array during a pipeline run.
// Written to KV at end of pipeline.
function makePipeAudit() {
  const entries = [];
  function log(phase, symbol, action, detail) {
    entries.push({
      ts:     new Date().toISOString(),
      phase:  phase,
      symbol: symbol || "",
      action: action,
      detail: detail || "",
    });
  }
  function getAll() { return entries; }
  return { log: log, getAll: getAll };
}

// ─── Pipeline status writer ───────────────────────────────────────────────────
async function writePipeStatus(env, phase, pct, extra) {
  const status = Object.assign({
    phase:     phase,
    pct:       pct,
    updatedAt: new Date().toISOString(),
  }, extra || {});
  try {
    await env.KITE_STORE.put("qe_pipe_status", JSON.stringify(status));
  } catch (_) {}
}

// ─── Survivorship logger ──────────────────────────────────────────────────────
// Records every eliminated symbol with the stage and reason it was removed.
// Written to qe_pipe_survivorship at end of pipeline.
function makeSurvivorshipLog() {
  const eliminated = [];
  function drop(symbol, stage, reason) {
    eliminated.push({
      symbol: symbol,
      stage:  stage,
      reason: reason,
      ts:     new Date().toISOString(),
    });
  }
  function getAll() { return eliminated; }
  return { drop: drop, getAll: getAll };
}

// ─── Unique run ID generator ──────────────────────────────────────────────────
function genRunId() {
  const arr = new Uint8Array(8);
  crypto.getRandomValues(arr);
  return Array.from(arr).map(function(b) {
    return ("00" + b.toString(16)).slice(-2);
  }).join("");
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 1 — UNIVERSE LOAD
// ═══════════════════════════════════════════════════════════════════════════════
async function pipeLoadUniverse(env, audit) {
  const raw = await env.KITE_STORE.get("qe_db_universe");
  if (!raw) {
    audit.log("S1_UNIVERSE", "", "ERROR", "qe_db_universe not found in KV — run universe refresh first");
    return null;
  }
  let symbols;
  try {
    symbols = JSON.parse(raw);
  } catch (_) {
    audit.log("S1_UNIVERSE", "", "ERROR", "Failed to parse qe_db_universe JSON");
    return null;
  }
  if (!Array.isArray(symbols) || symbols.length < 50) {
    audit.log("S1_UNIVERSE", "", "ERROR", "Universe too small: " + (symbols ? symbols.length : 0));
    return null;
  }
  audit.log("S1_UNIVERSE", "", "LOADED", "Universe: " + symbols.length + " symbols");
  return symbols;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 2 — NSE BHAV COPY INGEST
// Uses Kite bulk quote to get last_price, prev_close, volume for all universe symbols.
// Bhav Copy = today's close, volume, and price change — used for Stream A pre-filter.
// No historical data here — this is the "fast" pass to eliminate ~80% of universe.
//
// Kite /quote accepts up to 500 instruments per call in format "NSE:SYMBOL".
// We batch into chunks of 400 to stay well within limit.
// ═══════════════════════════════════════════════════════════════════════════════
const BHAV_BATCH_SIZE = 400;

async function pipeBhavCopy(env, token, symbols, audit, survive) {
  audit.log("S2_BHAV", "", "START", "Fetching bhav copy for " + symbols.length + " symbols");

  const bhav = {}; // symbol → { last_price, prev_close, volume, change_pct }

  for (let i = 0; i < symbols.length; i += BHAV_BATCH_SIZE) {
    const batch = symbols.slice(i, i + BHAV_BATCH_SIZE);
    const istr  = batch.map(function(s) { return "NSE:" + s; }).join(",");

    try {
      const resp = await fetch(
        `${KITE_API_BASE}/quote?i=${encodeURIComponent(istr)}`,
        {
          headers: {
            "X-Kite-Version": "3",
            "Authorization":  kiteAuthHeader(token),
          },
        }
      );

      if (!resp.ok) {
        audit.log("S2_BHAV", "", "WARN",
          "Bulk quote batch " + (i / BHAV_BATCH_SIZE + 1) + " failed: HTTP " + resp.status);
        // Do not abort — partial data is usable
        continue;
      }

      const data = await resp.json();
      const quotes = (data && data.data) ? data.data : {};

      for (let bi = 0; bi < batch.length; bi++) {
        const sym = batch[bi];
        const q   = quotes["NSE:" + sym];
        if (!q) continue;

        const last  = q.last_price || 0;
        const ohlc  = q.ohlc || {};
        const prev  = ohlc.close || last; // previous session close
        const vol   = q.volume || 0;
        const chgPct = prev > 0 ? ((last - prev) / prev) * 100 : 0;

        bhav[sym] = {
          last_price: last,
          prev_close: prev,
          volume:     vol,
          change_pct: chgPct,
        };
      }
    } catch (e) {
      audit.log("S2_BHAV", "", "WARN", "Bulk quote batch error: " + e.message);
    }

    // Small delay between bulk quote batches
    if (i + BHAV_BATCH_SIZE < symbols.length) {
      await new Promise(function(r) { setTimeout(r, 200); });
    }
  }

  const covered = Object.keys(bhav).length;
  audit.log("S2_BHAV", "", "DONE",
    "Bhav: " + covered + "/" + symbols.length + " symbols with live quote");

  // Symbols with no quote get eliminated here (unlisted, halted, etc.)
  const noQuote = symbols.filter(function(s) { return !bhav[s]; });
  for (let ni = 0; ni < noQuote.length; ni++) {
    survive.drop(noQuote[ni], "S2_BHAV", "No quote returned from Kite");
  }

  // Store bhav to KV for audit / debugging
  try {
    await env.KITE_STORE.put("qe_pipe_bhav_raw", JSON.stringify(bhav));
    await env.KITE_STORE.put("qe_pipe_bhav_date", new Date().toISOString().slice(0, 10));
  } catch (_) {}

  return bhav;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 3 — STREAM A FILTERS (bhav-only pass — no historical API calls)
//
// Filters applied purely from Bhav Copy data.
// Purpose: eliminate ~80–90% of universe before expensive OHLCV fetch.
//
// Criteria (ALL must pass):
//   A1. Price ≥ ₹100                    (already enforced in universe, re-check)
//   A2. Volume > 200,000 shares/day      (minimum liquidity)
//   A3. |change_pct| <= 15%              (circuit filter — avoid halt/breakout anomaly)
//   A4. last_price > 0                   (valid trading price)
//
// Note: EMA/RSI/ADX checks require historical data — those are Stage 5 Stream A
// post-OHLCV filters. This stage is intentionally lightweight.
// ═══════════════════════════════════════════════════════════════════════════════
const STREAM_A_MIN_PRICE  = 100;
const STREAM_A_MIN_VOL    = 200000;
const STREAM_A_MAX_CHANGE = 15;   // %, absolute

function pipeStreamAFast(bhav, symbols, audit, survive) {
  audit.log("S3_STREAM_A_FAST", "", "START",
    "Applying bhav-only filters to " + symbols.length + " symbols");

  const passed  = [];
  let rejPrice  = 0;
  let rejVol    = 0;
  let rejChange = 0;

  for (let i = 0; i < symbols.length; i++) {
    const sym = symbols[i];
    const b   = bhav[sym];
    if (!b) continue; // already eliminated in S2

    if (b.last_price < STREAM_A_MIN_PRICE) {
      survive.drop(sym, "S3_STREAM_A_FAST", "Price < ₹" + STREAM_A_MIN_PRICE + " (₹" + b.last_price.toFixed(2) + ")");
      rejPrice++;
      continue;
    }

    if (b.volume < STREAM_A_MIN_VOL) {
      survive.drop(sym, "S3_STREAM_A_FAST", "Volume < " + STREAM_A_MIN_VOL + " (" + b.volume.toLocaleString("en-IN") + ")");
      rejVol++;
      continue;
    }

    if (Math.abs(b.change_pct) > STREAM_A_MAX_CHANGE) {
      survive.drop(sym, "S3_STREAM_A_FAST", "Change " + b.change_pct.toFixed(1) + "% exceeds ±" + STREAM_A_MAX_CHANGE + "% — circuit/anomaly");
      rejChange++;
      continue;
    }

    passed.push(sym);
  }

  audit.log("S3_STREAM_A_FAST", "", "DONE",
    "Passed: " + passed.length + " | Rej price: " + rejPrice
    + " | Rej vol: " + rejVol + " | Rej change: " + rejChange);

  return passed;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 4 — OHLCV FETCH + COMPUTE
//
// For each symbol that passed Stream A Fast:
//   1. Fetch Kite historical data (PIPE_OHLCV_RANGE days, daily candles)
//   2. Compute: EMA(20), EMA(50), EMA(200), RSI(14), ADX(14),
//               ATR(14), Supertrend(10,2), Volume SMA(20), last close,
//               52-week high proximity, percent above EMA20
//   3. Store computed result per symbol to KV (TTL: 24h)
//
// Batched PIPE_BATCH_SIZE=5 with PIPE_BATCH_DELAY=300ms between batches.
// Failed symbols → dropped with reason.
// ═══════════════════════════════════════════════════════════════════════════════

// Math helpers (self-contained — no dependency on index.html functions)
function pipeEma(values, period) {
  if (!values || values.length < period) return null;
  const k = 2 / (period + 1);
  let ema = values.slice(0, period).reduce(function(a, b) { return a + b; }, 0) / period;
  for (let i = period; i < values.length; i++) {
    ema = values[i] * k + ema * (1 - k);
  }
  return ema;
}

function pipeSma(values, period) {
  if (!values || values.length < period) return null;
  const slice = values.slice(-period);
  return slice.reduce(function(a, b) { return a + b; }, 0) / slice.length;
}

function pipeRsi(closes, period) {
  period = period || 14;
  if (!closes || closes.length < period + 1) return null;
  let gains = 0, losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff > 0) gains  += diff;
    else          losses -= diff;
  }
  let avgGain = gains  / period;
  let avgLoss = losses / period;
  for (let i = period + 1; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    avgGain = (avgGain * (period - 1) + (diff > 0 ? diff : 0)) / period;
    avgLoss = (avgLoss * (period - 1) + (diff < 0 ? -diff : 0)) / period;
  }
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - (100 / (1 + rs));
}

function pipeAtr(highs, lows, closes, period) {
  period = period || 14;
  if (!highs || highs.length < period + 1) return null;
  const trs = [];
  for (let i = 1; i < highs.length; i++) {
    const tr = Math.max(
      highs[i]  - lows[i],
      Math.abs(highs[i]  - closes[i - 1]),
      Math.abs(lows[i]   - closes[i - 1])
    );
    trs.push(tr);
  }
  if (trs.length < period) return null;
  // Wilder smoothing
  let atr = trs.slice(0, period).reduce(function(a, b) { return a + b; }, 0) / period;
  for (let i = period; i < trs.length; i++) {
    atr = (atr * (period - 1) + trs[i]) / period;
  }
  return atr;
}

function pipeAdx(highs, lows, closes, period) {
  period = period || 14;
  if (!highs || highs.length < period * 2) return null;
  const len = highs.length;
  const dmPlus  = [];
  const dmMinus = [];
  const trs     = [];

  for (let i = 1; i < len; i++) {
    const upMove   = highs[i]  - highs[i - 1];
    const downMove = lows[i - 1] - lows[i];
    dmPlus.push((upMove > downMove && upMove > 0)   ? upMove   : 0);
    dmMinus.push((downMove > upMove && downMove > 0) ? downMove : 0);
    trs.push(Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i]  - closes[i - 1]),
      Math.abs(lows[i]   - closes[i - 1])
    ));
  }

  // Wilder smooth
  function wilderSmooth(arr, p) {
    let s = arr.slice(0, p).reduce(function(a, b) { return a + b; }, 0);
    const out = [s];
    for (let i = p; i < arr.length; i++) {
      s = s - (s / p) + arr[i];
      out.push(s);
    }
    return out;
  }

  const smTr     = wilderSmooth(trs,     period);
  const smDmPlus = wilderSmooth(dmPlus,  period);
  const smDmMinus= wilderSmooth(dmMinus, period);

  const dx = [];
  for (let i = 0; i < smTr.length; i++) {
    if (smTr[i] === 0) { dx.push(0); continue; }
    const diPlus  = (smDmPlus[i]  / smTr[i]) * 100;
    const diMinus = (smDmMinus[i] / smTr[i]) * 100;
    const sum = diPlus + diMinus;
    dx.push(sum === 0 ? 0 : Math.abs(diPlus - diMinus) / sum * 100);
  }

  if (dx.length < period) return null;
  let adx = dx.slice(0, period).reduce(function(a, b) { return a + b; }, 0) / period;
  for (let i = period; i < dx.length; i++) {
    adx = (adx * (period - 1) + dx[i]) / period;
  }
  return adx;
}

function pipeSupertrend(highs, lows, closes, period, multiplier) {
  period     = period     || 10;
  multiplier = multiplier || 2;
  if (!highs || highs.length < period + 1) return null;

  const len = highs.length;
  const atrArr = [];

  // Compute per-bar ATR first
  for (let i = 1; i < len; i++) {
    const tr = Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i] - closes[i - 1]),
      Math.abs(lows[i]  - closes[i - 1])
    );
    atrArr.push(tr);
  }

  // Wilder ATR
  let atr = atrArr.slice(0, period).reduce(function(a, b) { return a + b; }, 0) / period;
  const atrSmooth = [atr];
  for (let i = period; i < atrArr.length; i++) {
    atr = (atr * (period - 1) + atrArr[i]) / period;
    atrSmooth.push(atr);
  }

  // Supertrend — start from index period (first valid ATR)
  let stDir   = 1; // 1 = up (bullish), -1 = down (bearish)
  let stFinal = (highs[period] + lows[period]) / 2 + multiplier * atrSmooth[0];

  for (let i = period + 1; i < len; i++) {
    const ai   = i - period; // atrSmooth index
    const hl2  = (highs[i] + lows[i]) / 2;
    const up   = hl2 - multiplier * atrSmooth[ai];
    const dn   = hl2 + multiplier * atrSmooth[ai];

    if (stDir === 1) {
      // Bullish: use lower band
      stFinal = Math.max(up, stFinal);
      if (closes[i] < stFinal) { stDir = -1; stFinal = dn; }
    } else {
      // Bearish: use upper band
      stFinal = Math.min(dn, stFinal);
      if (closes[i] > stFinal) { stDir = 1; stFinal = up; }
    }
  }

  return { direction: stDir, value: stFinal }; // 1=bullish, -1=bearish
}

// ─── Fetch Nifty 50 closes for RS calculation ─────────────────────────────────
async function pipeLoadNiftyCloses(env, token, audit) {
  // Check cache first
  try {
    const tsRaw = await env.KITE_STORE.get("qe_pipe_nifty_ts");
    if (tsRaw && Date.now() - parseInt(tsRaw) < PIPE_NIFTY_TTL_MS) {
      const cached = await env.KITE_STORE.get("qe_pipe_nifty_closes");
      if (cached) {
        const closes = JSON.parse(cached);
        audit.log("S4_NIFTY", "NIFTY", "CACHED", closes.length + " bars from KV cache");
        return closes;
      }
    }
  } catch (_) {}

  // Fetch Nifty via Yahoo Finance (^NSEI) — no Kite instrument token needed
  try {
    const now      = new Date();
    const fromDate = new Date(now.getTime() - (PIPE_OHLCV_RANGE + 30) * 86400000);
    const from     = Math.floor(fromDate.getTime() / 1000);
    const to       = Math.floor(now.getTime() / 1000);

    const headers  = { "User-Agent": "Mozilla/5.0", "Accept": "application/json" };
    const yfUrl    = `https://query1.finance.yahoo.com/v8/finance/chart/%5ENSEI?interval=1d&period1=${from}&period2=${to}`;

    const ctrl  = new AbortController();
    const timer = setTimeout(function() { ctrl.abort(); }, 15000);
    const res   = await fetch(yfUrl, { signal: ctrl.signal, headers });
    clearTimeout(timer);

    if (!res.ok) throw new Error("Nifty YF HTTP " + res.status);

    const data = await res.json();
    const result = data && data.chart && data.chart.result && data.chart.result[0];
    if (!result) throw new Error("No Nifty chart result");

    const closes = (result.indicators && result.indicators.quote &&
                    result.indicators.quote[0] && result.indicators.quote[0].close) || [];
    const validCloses = closes.filter(function(c) { return c !== null && !isNaN(c); });

    if (validCloses.length < 50) throw new Error("Too few Nifty bars: " + validCloses.length);

    // Cache
    await env.KITE_STORE.put("qe_pipe_nifty_closes", JSON.stringify(validCloses));
    await env.KITE_STORE.put("qe_pipe_nifty_ts",     String(Date.now()));

    audit.log("S4_NIFTY", "NIFTY", "FETCHED", validCloses.length + " bars from Yahoo Finance");
    return validCloses;
  } catch (e) {
    audit.log("S4_NIFTY", "NIFTY", "ERROR", "Nifty fetch failed: " + e.message);
    return null;
  }
}

// ─── Fetch + compute OHLCV for a single symbol ───────────────────────────────
async function pipeFetchOhlcvSymbol(env, token, symbol) {
  const now      = new Date();
  const fromDate = new Date(now.getTime() - (PIPE_OHLCV_RANGE + 10) * 86400000);
  const fromStr  = fromDate.toISOString().slice(0, 10);
  const toStr    = now.toISOString().slice(0, 10);

  // Step 1: Get instrument token via quote — with timeout guard
  const quoteCtrl  = new AbortController();
  const quoteTimer = setTimeout(function() { quoteCtrl.abort(); }, PIPE_SYMBOL_TIMEOUT_MS);
  let quoteRes;
  try {
    quoteRes = await fetch(
      `${KITE_API_BASE}/quote?i=NSE:${encodeURIComponent(symbol)}`,
      { headers: { "X-Kite-Version": "3", "Authorization": kiteAuthHeader(token) },
        signal: quoteCtrl.signal }
    );
  } finally {
    clearTimeout(quoteTimer);
  }
  if (!quoteRes.ok) throw new Error("Quote HTTP " + quoteRes.status);
  const quoteData = await quoteRes.json();
  const q = quoteData && quoteData.data && quoteData.data["NSE:" + symbol];
  if (!q) throw new Error("No quote data for " + symbol);
  const instrToken = q.instrument_token;
  if (!instrToken) throw new Error("No instrument token for " + symbol);

  // Step 2: Fetch historical daily candles — with timeout guard
  const histCtrl  = new AbortController();
  const histTimer = setTimeout(function() { histCtrl.abort(); }, PIPE_SYMBOL_TIMEOUT_MS);
  let histRes;
  try {
    histRes = await fetch(
      `${KITE_API_BASE}/instruments/historical/${instrToken}/day?from=${fromStr}&to=${toStr}`,
      { headers: { "X-Kite-Version": "3", "Authorization": kiteAuthHeader(token) },
        signal: histCtrl.signal }
    );
  } finally {
    clearTimeout(histTimer);
  }
  if (!histRes.ok) throw new Error("Historical HTTP " + histRes.status);
  const histData = await histRes.json();
  const candles  = (histData && histData.data && histData.data.candles) || [];

  if (candles.length < PIPE_MIN_CANDLES) {
    throw new Error("Insufficient bars: " + candles.length + " < " + PIPE_MIN_CANDLES);
  }

  const opens   = candles.map(function(c) { return c[1]; });
  const highs   = candles.map(function(c) { return c[2]; });
  const lows    = candles.map(function(c) { return c[3]; });
  const closes  = candles.map(function(c) { return c[4]; });
  const volumes = candles.map(function(c) { return c[5]; });

  const lastClose = closes[closes.length - 1];
  const lastVol   = volumes[volumes.length - 1];
  const lastHigh  = highs[highs.length - 1];

  // Compute indicators
  const ema20  = pipeEma(closes, 20);
  const ema50  = pipeEma(closes, 50);
  const ema200 = pipeEma(closes, 200);
  const rsi14  = pipeRsi(closes, 14);
  const atr14  = pipeAtr(highs, lows, closes, 14);
  const adx14  = pipeAdx(highs, lows, closes, 14);
  const st     = pipeSupertrend(highs, lows, closes, 10, 2);
  const volSma = pipeSma(volumes, 20);

  // Derived metrics
  const atrPct      = (atr14 !== null && lastClose > 0) ? (atr14 / lastClose) * 100 : null;
  const pctAboveE20 = (ema20 !== null && lastClose > 0) ? ((lastClose - ema20) / ema20) * 100 : null;
  const hi52w       = Math.max.apply(null, highs.slice(-252));
  const prox52w     = hi52w > 0 ? ((lastClose / hi52w) * 100) : null; // % of 52w high
  const volRatio    = (volSma && volSma > 0) ? lastVol / volSma : null;

  // EMA stack (bullish = 20 > 50 > 200 AND price > 20)
  const emaStackBull = (ema20 !== null && ema50 !== null && ema200 !== null)
    ? (ema20 > ema50 && ema50 > ema200 && lastClose > ema20)
    : false;

  // Supertrend bullish
  const stBull = st ? st.direction === 1 : false;

  return {
    symbol:       symbol,
    lastClose:    lastClose,
    lastVol:      lastVol,
    ema20:        ema20,
    ema50:        ema50,
    ema200:       ema200,
    rsi14:        rsi14,
    atr14:        atr14,
    atrPct:       atrPct,
    adx14:        adx14,
    stBull:       stBull,
    stValue:      st ? st.value : null,
    volSma20:     volSma,
    volRatio:     volRatio,
    pctAboveE20:  pctAboveE20,
    hi52w:        hi52w,
    prox52w:      prox52w,
    emaStackBull: emaStackBull,
    closes:       closes.slice(-60),  // last 60 bars for RS calc — trimmed to save KV space
    candleCount:  candles.length,
  };
}

// ─── OHLCV batch runner ───────────────────────────────────────────────────────
async function pipeFetchOhlcvBatch(env, token, symbols, audit, survive) {
  audit.log("S4_OHLCV", "", "START",
    "Fetching OHLCV for " + symbols.length + " Stream A candidates");

  const results = {};

  for (let i = 0; i < symbols.length; i += PIPE_BATCH_SIZE) {
    const batch = symbols.slice(i, i + PIPE_BATCH_SIZE);

    const batchResults = await Promise.all(
      batch.map(async function(sym) {
        try {
          const ohlcv = await pipeFetchOhlcvSymbol(env, token, sym);
          // Cache per-symbol to KV (TTL 24h = 86400s)
          try {
            await env.KITE_STORE.put(
              "qe_pipe_ohlcv_" + sym,
              JSON.stringify(ohlcv),
              { expirationTtl: 86400 }
            );
          } catch (_) {}
          return { sym: sym, ok: true, data: ohlcv };
        } catch (e) {
          return { sym: sym, ok: false, error: e.message };
        }
      })
    );

    for (let bi = 0; bi < batchResults.length; bi++) {
      const br = batchResults[bi];
      if (br.ok) {
        results[br.sym] = br.data;
        audit.log("S4_OHLCV", br.sym, "OK",
          "RSI:" + (br.data.rsi14 !== null ? br.data.rsi14.toFixed(1) : "n/a")
          + " ADX:" + (br.data.adx14 !== null ? br.data.adx14.toFixed(1) : "n/a")
          + " ATR%:" + (br.data.atrPct !== null ? br.data.atrPct.toFixed(1) : "n/a"));
      } else {
        survive.drop(br.sym, "S4_OHLCV", "Fetch failed: " + br.error);
        audit.log("S4_OHLCV", br.sym, "FAIL", br.error);
      }
    }

    if (i + PIPE_BATCH_SIZE < symbols.length) {
      await new Promise(function(r) { setTimeout(r, PIPE_BATCH_DELAY); });
    }
  }

  const fetched = Object.keys(results).length;
  audit.log("S4_OHLCV", "", "DONE",
    "Fetched: " + fetched + "/" + symbols.length);

  return results;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 5 — STREAM A TECHNICAL FILTERS (post-OHLCV)
//
// Applied after OHLCV is computed. These are the core trend-confirmation filters.
// ALL must pass for a symbol to reach RS Engine.
//
// Criteria:
//   B1. EMA stack bullish: EMA20 > EMA50 > EMA200 AND close > EMA20
//   B2. RSI 14 in range [45, 75]           (momentum confirmation, not overbought)
//   B3. ADX 14 >= 18                        (trend present)
//   B4. Supertrend bullish (direction = 1)  (trend direction confirmation)
//   B5. ATR% in range [0.5, 8.0]            (not too compressed, not erratic)
//   B6. Volume ratio >= 0.8                 (at least 80% of 20-day avg volume)
// ═══════════════════════════════════════════════════════════════════════════════
const SA_RSI_MIN   = 45;
const SA_RSI_MAX   = 75;
const SA_ADX_MIN   = 18;
const SA_ATR_MIN   = 0.5;
const SA_ATR_MAX   = 8.0;
const SA_VOL_RATIO = 0.8;

function pipeStreamATech(ohlcvMap, audit, survive) {
  const symbols = Object.keys(ohlcvMap);
  audit.log("S5_STREAM_A_TECH", "", "START",
    "Technical filters on " + symbols.length + " symbols");

  const passed   = [];
  const rejected = { ema: 0, rsi: 0, adx: 0, st: 0, atr: 0, vol: 0 };

  for (let i = 0; i < symbols.length; i++) {
    const sym  = symbols[i];
    const ohlcv = ohlcvMap[sym];
    let reason = null;

    if (!ohlcv.emaStackBull) {
      reason = "EMA stack not bullish (20>50>200>price failed)";
      rejected.ema++;
    } else if (ohlcv.rsi14 === null || ohlcv.rsi14 < SA_RSI_MIN || ohlcv.rsi14 > SA_RSI_MAX) {
      reason = "RSI " + (ohlcv.rsi14 !== null ? ohlcv.rsi14.toFixed(1) : "null")
             + " outside [" + SA_RSI_MIN + "–" + SA_RSI_MAX + "]";
      rejected.rsi++;
    } else if (ohlcv.adx14 === null || ohlcv.adx14 < SA_ADX_MIN) {
      reason = "ADX " + (ohlcv.adx14 !== null ? ohlcv.adx14.toFixed(1) : "null")
             + " < " + SA_ADX_MIN;
      rejected.adx++;
    } else if (!ohlcv.stBull) {
      reason = "Supertrend bearish";
      rejected.st++;
    } else if (ohlcv.atrPct === null || ohlcv.atrPct < SA_ATR_MIN || ohlcv.atrPct > SA_ATR_MAX) {
      reason = "ATR% " + (ohlcv.atrPct !== null ? ohlcv.atrPct.toFixed(1) : "null")
             + "% outside [" + SA_ATR_MIN + "–" + SA_ATR_MAX + "%]";
      rejected.atr++;
    } else if (ohlcv.volRatio !== null && ohlcv.volRatio < SA_VOL_RATIO) {
      reason = "Volume ratio " + ohlcv.volRatio.toFixed(2) + " < " + SA_VOL_RATIO;
      rejected.vol++;
    }

    if (reason) {
      survive.drop(sym, "S5_STREAM_A_TECH", reason);
      audit.log("S5_STREAM_A_TECH", sym, "REJECT", reason);
    } else {
      passed.push(sym);
      audit.log("S5_STREAM_A_TECH", sym, "PASS",
        "RSI:" + ohlcv.rsi14.toFixed(1)
        + " ADX:" + ohlcv.adx14.toFixed(1)
        + " ATR%:" + ohlcv.atrPct.toFixed(1));
    }
  }

  audit.log("S5_STREAM_A_TECH", "", "DONE",
    "Passed: " + passed.length
    + " | Rej EMA:" + rejected.ema + " RSI:" + rejected.rsi
    + " ADX:" + rejected.adx + " ST:" + rejected.st
    + " ATR:" + rejected.atr + " Vol:" + rejected.vol);

  return passed;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 6 — RS ENGINE (server-side)
//
// Relative Strength = 3-period weighted return vs Nifty 50.
// Weights: 1m=40%, 3m=35%, 6m=25% (recency-biased).
// RS Score = percentile rank of weighted return vs all stocks in this pipeline run.
// Cutoff: PIPE_RS_THRESHOLD (default 55th percentile).
//
// If Nifty closes are unavailable (fetch failed), fall back to absolute
// 1-month return rank only (RS_FALLBACK mode).
// ═══════════════════════════════════════════════════════════════════════════════

function pipeCalcRS(stockCloses, niftyCloses) {
  if (!stockCloses || stockCloses.length < 20) return 0;

  function periodReturn(closes, bars) {
    if (!closes || closes.length < bars + 1) return null;
    const end   = closes[closes.length - 1];
    const start = closes[closes.length - 1 - bars];
    if (!start || start === 0) return null;
    return (end - start) / start;
  }

  const s1m = periodReturn(stockCloses, 21);
  const s3m = periodReturn(stockCloses, 63);
  const s6m = periodReturn(stockCloses, 126);

  if (s1m === null) return 0;

  if (!niftyCloses || niftyCloses.length < 22) {
    // Fallback: absolute return only
    return s1m !== null ? Math.max(0, Math.min(100, (s1m + 0.10) * 500)) : 0;
  }

  const n1m = periodReturn(niftyCloses, 21);
  const n3m = periodReturn(niftyCloses, 63);
  const n6m = periodReturn(niftyCloses, 126);

  // Relative returns
  const r1m = s1m !== null && n1m !== null ? s1m - n1m : 0;
  const r3m = s3m !== null && n3m !== null ? s3m - n3m : 0;
  const r6m = s6m !== null && n6m !== null ? s6m - n6m : 0;

  // Weighted composite relative return
  const composite = (r1m * 0.40) + (r3m * 0.35) + (r6m * 0.25);
  return composite; // raw score — percentile rank computed after all stocks computed
}

function pipeRankRS(streamAPassed, ohlcvMap, niftyCloses, audit, survive) {
  audit.log("S6_RS", "", "START", "RS ranking for " + streamAPassed.length + " symbols");

  // Compute raw RS composite for each symbol
  const rsRaw = [];
  for (let i = 0; i < streamAPassed.length; i++) {
    const sym   = streamAPassed[i];
    const ohlcv = ohlcvMap[sym];
    const raw   = pipeCalcRS(ohlcv.closes, niftyCloses);
    rsRaw.push({ sym: sym, raw: raw });
  }

  // Sort ascending to get ranks
  const sorted = rsRaw.slice().sort(function(a, b) { return a.raw - b.raw; });

  // Assign percentile rank
  const ranked = rsRaw.map(function(item) {
    const rank = sorted.findIndex(function(s) { return s.sym === item.sym; });
    const pct  = rsRaw.length > 1 ? (rank / (rsRaw.length - 1)) * 100 : 50;
    return { sym: item.sym, rsScore: Math.round(pct), rawRS: item.raw };
  });

  // Apply RS threshold cutoff
  const passed   = [];
  const rejected = [];

  for (let j = 0; j < ranked.length; j++) {
    const r = ranked[j];
    if (r.rsScore >= PIPE_RS_THRESHOLD) {
      passed.push(r);
      audit.log("S6_RS", r.sym, "PASS", "RS: " + r.rsScore + " (raw " + r.rawRS.toFixed(4) + ")");
    } else {
      survive.drop(r.sym, "S6_RS",
        "RS score " + r.rsScore + " < threshold " + PIPE_RS_THRESHOLD);
      audit.log("S6_RS", r.sym, "REJECT",
        "RS: " + r.rsScore + " < " + PIPE_RS_THRESHOLD);
      rejected.push(r.sym);
    }
  }

  audit.log("S6_RS", "", "DONE",
    "Passed: " + passed.length + " | Rejected: " + rejected.length);

  // Sort by rsScore descending
  passed.sort(function(a, b) { return b.rsScore - a.rsScore; });
  return passed;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 7 — SECTOR ENGINE
//
// Prevents sector concentration in final candidates.
// Max PIPE_MAX_SECTOR_N candidates per sector.
// Within each sector, keep highest RS score.
// ═══════════════════════════════════════════════════════════════════════════════
function pipeSectorFilter(rsRanked, ohlcvMap, audit, survive) {
  audit.log("S7_SECTOR", "", "START",
    "Sector concentration filter on " + rsRanked.length + " symbols");

  const sectorCount = {};
  const passed      = [];

  for (let i = 0; i < rsRanked.length; i++) {
    const r      = rsRanked[i];
    const sector = SECTOR_MAP[r.sym] || "OTHER";

    if (!sectorCount[sector]) sectorCount[sector] = 0;

    if (sectorCount[sector] < PIPE_MAX_SECTOR_N) {
      sectorCount[sector]++;
      passed.push(Object.assign({}, r, { sector: sector }));
      audit.log("S7_SECTOR", r.sym, "PASS",
        "Sector: " + sector + " (" + sectorCount[sector] + "/" + PIPE_MAX_SECTOR_N + ")");
    } else {
      survive.drop(r.sym, "S7_SECTOR",
        "Sector cap: " + sector + " already has " + PIPE_MAX_SECTOR_N + " candidates");
      audit.log("S7_SECTOR", r.sym, "REJECT",
        "Sector cap: " + sector + " (" + sectorCount[sector] + "/" + PIPE_MAX_SECTOR_N + ")");
    }
  }

  audit.log("S7_SECTOR", "", "DONE",
    "Passed: " + passed.length + " across "
    + Object.keys(sectorCount).filter(function(k) { return sectorCount[k] > 0; }).length
    + " sectors");

  return passed;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 8 — MERGE ENGINE + DISCOVERY SCORE
//
// Builds final candidate list for browser deep analysis.
// Discovery Score (server-side, mirrors browser calcDiscoveryScore logic):
//   RS Percentile   : 25 pts  (rsScore / 100 * 25)
//   Volume Surge    : 20 pts  (volRatio; 1.5x = full score)
//   Coil/Compression: 15 pts  (pctAboveE20 <= 5% and ATR% < 2.5%)
//   52-week Proximity: 15 pts (prox52w >= 80%)
//   Trend Quality   : 15 pts  (EMA stack + ADX + Supertrend)
//   Liquidity       : 10 pts  (daily volume >= 500K = full score)
//
// Sorts by discoveryScore descending, caps at PIPE_TOP_N.
// ═══════════════════════════════════════════════════════════════════════════════
function pipeCalcDiscoveryScore(ohlcv, rsScore) {
  let total = 0;
  const breakdown = {};

  // Factor 1: RS Percentile (25 pts)
  const rsPts = Math.round((rsScore / 100) * 25);
  breakdown.rs = rsPts;
  total += rsPts;

  // Factor 2: Volume Surge (20 pts) — volRatio 1.5x = full 20 pts
  let volPts = 0;
  if (ohlcv.volRatio !== null) {
    volPts = Math.min(20, Math.round((ohlcv.volRatio / 1.5) * 20));
  }
  breakdown.vol = volPts;
  total += volPts;

  // Factor 3: Coil / Compression (15 pts)
  // pctAboveE20 <= 5% AND atrPct < 2.5% = max score
  let coilPts = 0;
  if (ohlcv.pctAboveE20 !== null && ohlcv.atrPct !== null) {
    const coilScore = (ohlcv.pctAboveE20 <= 2 && ohlcv.atrPct < 1.5) ? 15
                    : (ohlcv.pctAboveE20 <= 5 && ohlcv.atrPct < 2.5) ? 10
                    : (ohlcv.pctAboveE20 <= 8)                        ? 5
                    : 0;
    coilPts = coilScore;
  }
  breakdown.coil = coilPts;
  total += coilPts;

  // Factor 4: 52-week Proximity (15 pts) — prox52w >= 95% = full; >= 80% = partial
  let proxPts = 0;
  if (ohlcv.prox52w !== null) {
    proxPts = ohlcv.prox52w >= 95 ? 15
            : ohlcv.prox52w >= 85 ? 10
            : ohlcv.prox52w >= 75 ? 5
            : 0;
  }
  breakdown.prox52w = proxPts;
  total += proxPts;

  // Factor 5: Trend Quality (15 pts)
  let trendPts = 0;
  if (ohlcv.emaStackBull) trendPts += 5;
  if (ohlcv.adx14 !== null && ohlcv.adx14 >= 25) trendPts += 5;
  else if (ohlcv.adx14 !== null && ohlcv.adx14 >= 18) trendPts += 3;
  if (ohlcv.stBull) trendPts += 5;
  breakdown.trend = trendPts;
  total += trendPts;

  // Factor 6: Liquidity (10 pts) — 500K daily vol = full score
  let liqPts = 0;
  if (ohlcv.lastVol >= 500000)      liqPts = 10;
  else if (ohlcv.lastVol >= 200000) liqPts = 6;
  else if (ohlcv.lastVol >= 100000) liqPts = 3;
  breakdown.liquidity = liqPts;
  total += liqPts;

  return { total: Math.min(100, total), breakdown: breakdown };
}

function pipeMerge(sectorFiltered, ohlcvMap, bhav, audit) {
  audit.log("S8_MERGE", "", "START", "Building " + sectorFiltered.length + " candidates");

  const candidates = [];

  for (let i = 0; i < sectorFiltered.length; i++) {
    const r     = sectorFiltered[i];
    const ohlcv = ohlcvMap[r.sym];
    if (!ohlcv) continue;

    const ds    = pipeCalcDiscoveryScore(ohlcv, r.rsScore);

    // Compute trade levels from ATR
    const atr   = ohlcv.atr14 || 0;
    const entry = ohlcv.lastClose;
    const sl    = atr > 0 ? parseFloat((entry - 1.5 * atr).toFixed(2)) : null;
    const t1    = atr > 0 ? parseFloat((entry + 2.0 * atr).toFixed(2)) : null;
    const t2    = atr > 0 ? parseFloat((entry + 3.5 * atr).toFixed(2)) : null;

    candidates.push({
      symbol:         r.sym,
      sector:         r.sector,
      rsScore:        r.rsScore,
      discoveryScore: ds.total,
      dsBreakdown:    ds.breakdown,
      // Price data for browser
      lastClose:      ohlcv.lastClose,
      entry:          parseFloat(entry.toFixed(2)),
      sl:             sl,
      t1:             t1,
      t2:             t2,
      atr:            atr !== null ? parseFloat(atr.toFixed(2)) : null,
      atrPct:         ohlcv.atrPct !== null ? parseFloat(ohlcv.atrPct.toFixed(2)) : null,
      // Indicators for browser card display
      rsi14:          ohlcv.rsi14 !== null ? parseFloat(ohlcv.rsi14.toFixed(1)) : null,
      adx14:          ohlcv.adx14 !== null ? parseFloat(ohlcv.adx14.toFixed(1)) : null,
      ema20:          ohlcv.ema20 !== null ? parseFloat(ohlcv.ema20.toFixed(2)) : null,
      ema50:          ohlcv.ema50 !== null ? parseFloat(ohlcv.ema50.toFixed(2)) : null,
      ema200:         ohlcv.ema200 !== null ? parseFloat(ohlcv.ema200.toFixed(2)) : null,
      stBull:         ohlcv.stBull,
      volRatio:       ohlcv.volRatio !== null ? parseFloat(ohlcv.volRatio.toFixed(2)) : null,
      pctAboveE20:    ohlcv.pctAboveE20 !== null ? parseFloat(ohlcv.pctAboveE20.toFixed(2)) : null,
      prox52w:        ohlcv.prox52w !== null ? parseFloat(ohlcv.prox52w.toFixed(1)) : null,
      // Pipeline metadata
      builtAt:        new Date().toISOString(),
      // closes omitted from candidates — too large; kept in per-symbol KV key
    });

    audit.log("S8_MERGE", r.sym, "CANDIDATE",
      "DS:" + ds.total + " RS:" + r.rsScore + " Sector:" + r.sector);
  }

  // Sort by discoveryScore descending
  candidates.sort(function(a, b) { return b.discoveryScore - a.discoveryScore; });

  // Cap at PIPE_TOP_N
  const final = candidates.slice(0, PIPE_TOP_N);
  audit.log("S8_MERGE", "", "DONE",
    "Final candidates: " + final.length + " (top " + PIPE_TOP_N + " by DS)");

  return final;
}

// ═══════════════════════════════════════════════════════════════════════════════
// STAGE 9 — TELEGRAM SIGNAL DISPATCH
//
// Sends top PIPE_SIGNAL_TOP candidates as Telegram signals.
// FIX 2: Server-side signal integrity gate applied before dispatch.
// A candidate must pass all three structural checks to be dispatched as
// a WATCH-eligible signal. Candidates that fail are dispatched as
// WATCH_ONLY with a clear label — preventing a TG signal that the browser
// will later contradict with IGNORE.
//
// Gate criteria (mirrors the structural subset of browser finalDecision()):
//   G1. discoveryScore >= 60    (minimum score threshold)
//   G2. stBull === true         (Supertrend must be bullish — trend direction)
//   G3. adx14 >= 18             (trend must be present — not ranging/dead)
//
// Candidates below gate are still dispatched as WATCH_ONLY (informational)
// so the pipeline is transparent, but BUY button is suppressed at Telegram level.
// ═══════════════════════════════════════════════════════════════════════════════
async function pipeDispatchTelegram(env, candidates, audit) {
  if (!candidates || candidates.length === 0) {
    audit.log("S9_TELEGRAM", "", "SKIP", "No candidates to dispatch");
    return 0;
  }

  // ── Deduplication: load today's already-alerted symbols from KV ──────────────
  // KV key: qe_pipe_alerted_today  →  { date: "YYYY-MM-DD", symbols: ["SYM1", ...] }
  // Resets automatically each new trading day (date mismatch → fresh set).
  const todayIST = new Date().toLocaleDateString("en-CA",
    { timeZone: "Asia/Kolkata" }); // "YYYY-MM-DD" in IST
  let alertedSymbols = [];
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_alerted_today");
    if (raw) {
      const parsed = JSON.parse(raw);
      if (parsed.date === todayIST) {
        alertedSymbols = parsed.symbols || [];
      }
      // If date differs — new trading day — alertedSymbols stays empty (fresh start)
    }
  } catch (_) {}
  const alertedSet = new Set(alertedSymbols);

  audit.log("S9_TELEGRAM", "", "DEDUP_LOAD",
    "Today: " + todayIST + " | Already alerted: [" + alertedSymbols.join(", ") + "]");
  // ── End dedup load ────────────────────────────────────────────────────────────

  const top    = candidates.slice(0, PIPE_SIGNAL_TOP);
  const expiry = Date.now() + SIGNAL_TTL_MS;
  let   sent   = 0;

  for (let i = 0; i < top.length; i++) {
    const c        = top[i];

    // ── Deduplication check — skip if already alerted today ───────────────────
    if (alertedSet.has(c.symbol)) {
      audit.log("S9_TELEGRAM", c.symbol, "DEDUP_SKIP",
        "Already alerted today (" + todayIST + ") — suppressing repeat");
      continue;
    }
    // ── End dedup check ───────────────────────────────────────────────────────

    // ── Fix 2: Signal integrity gate ──────────────────────────────────────────
    const gateDS   = (c.discoveryScore || 0) >= 60;
    const gateST   = c.stBull === true;
    const gateADX  = (c.adx14 || 0) >= 18;
    const gatePass = gateDS && gateST && gateADX;
    const watchOnly = !gatePass;

    if (watchOnly) {
      const failReason = !gateDS ? "DS " + c.discoveryScore + " < 60"
                       : !gateST ? "Supertrend bearish"
                       : "ADX " + (c.adx14 || 0).toFixed(1) + " < 18";
      audit.log("S9_TELEGRAM", c.symbol, "GATE_FAIL",
        "Signal integrity gate failed: " + failReason + " — dispatching WATCH_ONLY");
    } else {
      audit.log("S9_TELEGRAM", c.symbol, "GATE_PASS",
        "DS:" + c.discoveryScore + " ST:bullish ADX:" + (c.adx14 || 0).toFixed(1));
    }
    // ── End Fix 2 gate ────────────────────────────────────────────────────────

    const signalId = genRunId();
    const hmac     = await signPayload(env, signalId, c.symbol, c.entry, expiry);

    // Store signal in KV for callback verification — include watchOnly flag
    try {
      await env.KITE_STORE.put(
        "qe_signal_" + signalId,
        JSON.stringify({
          signalId:  signalId,
          symbol:    c.symbol,
          entry:     c.entry,
          sl:        c.sl,
          t1:        c.t1,
          t2:        c.t2,
          qty:       1,    // browser fills actual qty after deep analysis
          cmp:       c.lastClose,
          expiry:    expiry,
          hmac:      hmac,
          source:    "pipeline_v4",
          watchOnly: watchOnly,
        }),
        { expirationTtl: 1800 }
      );
    } catch (_) {}

    const expiryStr = new Date(expiry).toLocaleTimeString("en-IN",
      { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata" });

    // Fix 2: Message clearly labels WATCH_ONLY signals — no ambiguity
    const signalTypeLabel = watchOnly
      ? "⚠️ <b>WATCH ONLY</b> — pending deep analysis"
      : "🔭 <b>Pipeline Signal</b> — gate passed";

    const gateStatusLine = watchOnly
      ? "⚠️ Gate: WATCH_ONLY (deep analysis required before BUY)\n"
      : "✅ Gate: DS✓ Supertrend✓ ADX✓\n";

    const msg = signalTypeLabel + ` #${i + 1}\n\n`
      + `<b>${c.symbol}</b>  [${c.sector}]\n\n`
      + `📊 Discovery Score: <b>${c.discoveryScore}/100</b>\n`
      + `📈 RS Rank: <b>${c.rsScore}/100</b>\n`
      + `💹 RSI: ${c.rsi14 !== null ? c.rsi14 : "—"}  ADX: ${c.adx14 !== null ? c.adx14 : "—"}  ST: ${c.stBull ? "🟢 Bull" : "🔴 Bear"}\n`
      + gateStatusLine
      + `\n💰 CMP: ₹${c.lastClose}\n`
      + `🎯 Entry: ₹${c.entry}  SL: ₹${c.sl !== null ? c.sl : "—"}\n`
      + `✅ T1: ₹${c.t1 !== null ? c.t1 : "—"}  T2: ₹${c.t2 !== null ? c.t2 : "—"}\n\n`
      + `⏱ Open QuantEdge for deep analysis. Signal expires ${expiryStr} IST.\n`
      + `<i>Source: Server Pipeline v4.1</i>`;

    // Inline keyboard with BUY / WATCH / REJECT
    const cbData = {
      action:   "WATCH", // default action from Telegram — browser does BUY after deep analysis
      signalId: signalId,
      symbol:   c.symbol,
      entry:    c.entry,
      sl:       c.sl,
      t1:       c.t1,
      t2:       c.t2,
      qty:      1,
      cmp:      c.lastClose,
      expiry:   expiry,
      hmac:     hmac,
    };

    const keyboard = {
      inline_keyboard: [[
        { text: "👀 Watch",  callback_data: JSON.stringify(Object.assign({}, cbData, { action: "WATCH"  })) },
        { text: "❌ Skip",   callback_data: JSON.stringify(Object.assign({}, cbData, { action: "REJECT" })) },
      ]]
    };

    const ok = await sendTelegram(env, msg, keyboard);
    if (ok) {
      sent++;
      audit.log("S9_TELEGRAM", c.symbol, "SENT",
        "Signal #" + (i + 1) + " DS:" + c.discoveryScore);

      // ── Dedup persist: mark this symbol as alerted today ──────────────────────
      alertedSet.add(c.symbol);
      try {
        await env.KITE_STORE.put(
          "qe_pipe_alerted_today",
          JSON.stringify({ date: todayIST, symbols: Array.from(alertedSet) }),
          { expirationTtl: 86400 } // auto-expires after 24h — belt-and-suspenders
        );
      } catch (_) {}
      // ── End dedup persist ─────────────────────────────────────────────────────
    } else {
      audit.log("S9_TELEGRAM", c.symbol, "FAIL", "Telegram send failed");
    }

    // Small delay between messages to avoid Telegram rate limits
    if (i < top.length - 1) {
      await new Promise(function(r) { setTimeout(r, 500); });
    }
  }

  audit.log("S9_TELEGRAM", "", "DONE", "Sent: " + sent + "/" + top.length);
  return sent;
}

// ═══════════════════════════════════════════════════════════════════════════════
// FIX 3: PIPELINE REGIME SNAPSHOT
// Derives a structural market regime from Nifty 50 daily closes.
// Mirrors the browser's fetchRegime() scoring logic (bull/sideways/bear).
// This snapshot is embedded in every KV signal so the browser uses the
// pipeline-time regime for deep analysis — not the browser's current regime.
//
// Returns: { regime: 'bull'|'sideways'|'bear', bullScore: N, ts: ISO }
// Falls back to 'sideways' if niftyCloses is unavailable.
// ═══════════════════════════════════════════════════════════════════════════════
function computePipelineRegime(niftyCloses, audit) {
  const fallback = { regime: "sideways", bullScore: 0, ts: new Date().toISOString(), source: "fallback" };

  if (!niftyCloses || niftyCloses.length < 50) {
    audit.log("REGIME_SNAP", "", "FALLBACK", "Insufficient Nifty bars — using sideways");
    return fallback;
  }

  try {
    const c   = niftyCloses;
    const n   = c.length;
    const cmp = c[n - 1];

    // EMA helpers (Wilder/standard)
    function emaLast(arr, period) {
      if (arr.length < period) return null;
      const k = 2 / (period + 1);
      let e = arr.slice(0, period).reduce(function(a, b) { return a + b; }, 0) / period;
      for (let i = period; i < arr.length; i++) { e = arr[i] * k + e * (1 - k); }
      return e;
    }
    function meanArr(arr) {
      return arr.reduce(function(a, b) { return a + b; }, 0) / arr.length;
    }

    const e50  = emaLast(c, 50);
    const e200 = emaLast(c, Math.min(200, c.length));

    // Use available data for trend metrics
    const recent20 = c.slice(-20);
    const prior40  = c.slice(-60, -20);
    const recentMean = meanArr(recent20);
    const priorMean  = prior40.length > 0 ? meanArr(prior40) : recentMean;
    const trend      = recentMean - priorMean;
    const mom5       = c[n - 1] - c[Math.max(0, n - 6)];

    let upDays = 0;
    for (let i = Math.max(1, n - 20); i < n; i++) { if (c[i] > c[i - 1]) upDays++; }
    const breadth = upDays / 20;

    // Bull score (mirrors browser logic)
    let bull = 0;
    if (e50 !== null   && cmp > e50)      bull += 2;
    if (e50 !== null   && e200 !== null && e50 > e200) bull += 2;
    if (trend > 0)     bull += 1;
    if (mom5 > 0)      bull += 1;
    if (breadth > 0.55) bull += 1;
    if (e200 !== null  && cmp > e200)     bull += 1;

    const regime = bull >= 5 ? "bull" : bull <= 2 ? "bear" : "sideways";

    return {
      regime:    regime,
      bullScore: bull,
      ts:        new Date().toISOString(),
      source:    "nifty_computed",
      niftyBars: n,
      cmp:       parseFloat(cmp.toFixed(2)),
      e50:       e50 !== null ? parseFloat(e50.toFixed(2)) : null,
      e200:      e200 !== null ? parseFloat(e200.toFixed(2)) : null,
    };
  } catch (e) {
    audit.log("REGIME_SNAP", "", "ERROR", "Regime computation failed: " + e.message);
    return fallback;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// FULL PIPELINE ORCHESTRATOR
// Runs all stages in sequence, writes results to KV at each checkpoint.
// ═══════════════════════════════════════════════════════════════════════════════
async function runFullPipeline(env) {
  const runId    = genRunId();
  const startedAt = new Date().toISOString();
  const audit    = makePipeAudit();
  const survive  = makeSurvivorshipLog();

  audit.log("PIPELINE", "", "START", "Run ID: " + runId + " at " + startedAt);

  // Write initial status
  await writePipeStatus(env, "STARTING", 2, {
    runId:     runId,
    startedAt: startedAt,
  });
  await env.KITE_STORE.put("qe_pipe_run_id", runId);

  // ── Get Kite token ───────────────────────────────────────────────────────────
  let token;
  try {
    token = await getToken(env);
  } catch (e) {
    audit.log("PIPELINE", "", "ERROR", "No Kite token: " + e.message);
    await writePipeStatus(env, "FAILED", 0, {
      runId: runId, error: "No Kite token — login required",
    });
    await env.KITE_STORE.put("qe_pipe_audit", JSON.stringify(audit.getAll().slice(0, 500)));
    throw e;
  }

  // ── Stage 1: Universe ────────────────────────────────────────────────────────
  await writePipeStatus(env, "S1_UNIVERSE", 5, { runId: runId });
  const universe = await pipeLoadUniverse(env, audit);
  if (!universe) {
    await writePipeStatus(env, "FAILED", 0, { runId: runId, error: "Universe not found in KV" });
    await env.KITE_STORE.put("qe_pipe_audit", JSON.stringify(audit.getAll().slice(0, 500)));
    await sendTelegram(env,
      `⚠️ <b>Pipeline Failed — Stage 1</b>\nUniverse not in KV. Run /universe/refresh first.`);
    return { ok: false, error: "Universe not found" };
  }

  // ── Stage 2: Bhav Copy ───────────────────────────────────────────────────────
  await writePipeStatus(env, "S2_BHAV", 10, {
    runId: runId, universeCount: universe.length,
  });
  const bhav = await pipeBhavCopy(env, token, universe, audit, survive);

  // ── Stage 3: Stream A Fast (bhav-only) ──────────────────────────────────────
  await writePipeStatus(env, "S3_STREAM_A_FAST", 18, { runId: runId });
  const bhavSymbols    = universe.filter(function(s) { return !!bhav[s]; });
  const streamAFast    = pipeStreamAFast(bhav, bhavSymbols, audit, survive);

  audit.log("PIPELINE", "", "CHECKPOINT",
    "Post-S3: " + streamAFast.length + " symbols entering OHLCV fetch");

  if (streamAFast.length === 0) {
    await writePipeStatus(env, "FAILED", 0, { runId: runId, error: "No symbols passed Stream A Fast" });
    await env.KITE_STORE.put("qe_pipe_audit",        JSON.stringify(audit.getAll().slice(0, 500)));
    await env.KITE_STORE.put("qe_pipe_survivorship", JSON.stringify(survive.getAll().slice(0, 1000)));
    await sendTelegram(env, `⚠️ <b>Pipeline: No candidates after bhav filters</b>. Market may be broadly weak.`);
    return { ok: false, error: "No symbols passed Stream A Fast" };
  }

  // ── Critical Fix 2: Sort by volume descending, cap at PIPE_MAX_OHLCV_CAP ───
  // Highest-liquidity stocks processed first — cap protects Worker CPU budget.
  // Symbols beyond cap are survivorship-logged so the audit trail is complete.
  const streamAFastSorted = streamAFast.slice().sort(function(a, b) {
    const va = (bhav[a] && bhav[a].volume) || 0;
    const vb = (bhav[b] && bhav[b].volume) || 0;
    return vb - va; // descending — highest volume first
  });

  const ohlcvQueue   = streamAFastSorted.slice(0, PIPE_MAX_OHLCV_CAP);
  const ohlcvDropped = streamAFastSorted.slice(PIPE_MAX_OHLCV_CAP);

  for (let di = 0; di < ohlcvDropped.length; di++) {
    survive.drop(ohlcvDropped[di], "S4_OHLCV_CAP",
      "OHLCV cap exceeded (" + PIPE_MAX_OHLCV_CAP + ") — lower liquidity than cutoff");
  }

  if (ohlcvDropped.length > 0) {
    audit.log("PIPELINE", "", "OHLCV_CAP",
      "Capped at " + PIPE_MAX_OHLCV_CAP + " symbols. Dropped " + ohlcvDropped.length
      + " lower-volume symbols. Processing top " + ohlcvQueue.length + " by volume.");
  }

  // ── Stage 4: OHLCV fetch ─────────────────────────────────────────────────────
  await writePipeStatus(env, "S4_OHLCV", 25, {
    runId: runId, streamAFastCount: streamAFast.length, ohlcvQueueCount: ohlcvQueue.length,
  });

  // Also fetch Nifty closes (parallel with OHLCV batch)
  const niftyClosesPromise = pipeLoadNiftyCloses(env, token, audit);
  const ohlcvMap = await pipeFetchOhlcvBatch(env, token, ohlcvQueue, audit, survive);
  const niftyCloses = await niftyClosesPromise;

  // ── Fix 3: Compute and snapshot macro regime from Nifty data ────────────────
  // This regime snapshot travels with every pipeline signal so the browser
  // can use the pipeline-time regime for deterministic deep analysis scoring,
  // regardless of what time of day the user opens the Discovery panel.
  const pipelineRegime = computePipelineRegime(niftyCloses, audit);
  audit.log("PIPELINE", "", "MACRO_SNAP",
    "Pipeline regime: " + pipelineRegime.regime + " (bull:" + pipelineRegime.bullScore
    + " niftyBars:" + (niftyCloses ? niftyCloses.length : 0) + ")");
  // ── End Fix 3a ────────────────────────────────────────────────────────────

  if (Object.keys(ohlcvMap).length === 0) {
    await writePipeStatus(env, "FAILED", 0, { runId: runId, error: "All OHLCV fetches failed" });
    await env.KITE_STORE.put("qe_pipe_audit",        JSON.stringify(audit.getAll().slice(0, 500)));
    await env.KITE_STORE.put("qe_pipe_survivorship", JSON.stringify(survive.getAll().slice(0, 1000)));
    await sendTelegram(env, `⚠️ <b>Pipeline: OHLCV fetch failed for all symbols</b>. Kite API may be throttling.`);
    return { ok: false, error: "All OHLCV fetches failed" };
  }

  // ── Stage 5: Stream A Technical ─────────────────────────────────────────────
  await writePipeStatus(env, "S5_STREAM_A_TECH", 55, {
    runId: runId, ohlcvCount: Object.keys(ohlcvMap).length,
  });
  const streamATech = pipeStreamATech(ohlcvMap, audit, survive);

  // Write Stream A result to KV
  try {
    await env.KITE_STORE.put("qe_pipe_stream_a", JSON.stringify(streamATech));
  } catch (_) {}

  audit.log("PIPELINE", "", "CHECKPOINT",
    "Post-S5: " + streamATech.length + " symbols entering RS Engine");

  if (streamATech.length === 0) {
    await writePipeStatus(env, "COMPLETED_EMPTY", 100, {
      runId: runId, reason: "No symbols passed technical filters",
    });
    await env.KITE_STORE.put("qe_pipe_audit",        JSON.stringify(audit.getAll().slice(0, 500)));
    await env.KITE_STORE.put("qe_pipe_survivorship", JSON.stringify(survive.getAll().slice(0, 1000)));
    await env.KITE_STORE.put("qe_pipe_signals",      JSON.stringify([]));
    await env.KITE_STORE.put("qe_pipe_candidates",   JSON.stringify([]));
    await sendTelegram(env, `📊 <b>Pipeline Complete</b>\nNo stocks passed technical filters today. Market may be consolidating.`);
    return { ok: true, candidateCount: 0, signalCount: 0 };
  }

  // ── Stage 6: RS Engine ───────────────────────────────────────────────────────
  await writePipeStatus(env, "S6_RS", 62, {
    runId: runId, streamATechCount: streamATech.length,
  });
  const rsRanked = pipeRankRS(streamATech, ohlcvMap, niftyCloses, audit, survive);

  // Write RS result to KV
  try {
    await env.KITE_STORE.put("qe_pipe_rs_ranked",
      JSON.stringify(rsRanked.map(function(r) {
        return { sym: r.sym, rsScore: r.rsScore };
      }))
    );
  } catch (_) {}

  audit.log("PIPELINE", "", "CHECKPOINT",
    "Post-S6: " + rsRanked.length + " symbols entering Sector Engine");

  // ── Stage 7: Sector Engine ───────────────────────────────────────────────────
  await writePipeStatus(env, "S7_SECTOR", 70, {
    runId: runId, rsCount: rsRanked.length,
  });
  const sectorFiltered = pipeSectorFilter(rsRanked, ohlcvMap, audit, survive);

  // Write sector map to KV
  const sectorMapOut = {};
  for (let si = 0; si < sectorFiltered.length; si++) {
    sectorMapOut[sectorFiltered[si].sym] = sectorFiltered[si].sector;
  }
  try {
    await env.KITE_STORE.put("qe_pipe_sector_map", JSON.stringify(sectorMapOut));
  } catch (_) {}

  // ── Stage 8: Merge + Discovery Score ────────────────────────────────────────
  await writePipeStatus(env, "S8_MERGE", 78, {
    runId: runId, sectorCount: sectorFiltered.length,
  });
  const candidates = pipeMerge(sectorFiltered, ohlcvMap, bhav, audit);

  // Write candidates to KV — this is what the browser reads in Part 3
  try {
    await env.KITE_STORE.put("qe_pipe_candidates", JSON.stringify(candidates));
  } catch (_) {}

  // ── Stage 9: Write signals to KV (browser-readable) ─────────────────────────
  // Signals are a trimmed version of candidates with HMAC-ready fields
  const signalsForKv = candidates.map(function(c) {
    return {
      symbol:         c.symbol,
      sector:         c.sector,
      discoveryScore: c.discoveryScore,
      rsScore:        c.rsScore,
      dsBreakdown:    c.dsBreakdown,
      lastClose:      c.lastClose,
      entry:          c.entry,
      sl:             c.sl,
      t1:             c.t1,
      t2:             c.t2,
      atr:            c.atr,
      atrPct:         c.atrPct,
      rsi14:          c.rsi14,
      adx14:          c.adx14,
      stBull:         c.stBull,
      volRatio:       c.volRatio,
      pctAboveE20:    c.pctAboveE20,
      prox52w:        c.prox52w,
      builtAt:        c.builtAt,
      source:         "pipeline_v4",
      // Fix 3: Pipeline-time regime snapshot — browser uses this for deterministic scoring
      pipelineRegime: pipelineRegime,
      // deepResult filled in by browser via POST /pipe/deep-result
      deepResult:     null,
    };
  });

  try {
    await env.KITE_STORE.put("qe_pipe_signals", JSON.stringify(signalsForKv));
  } catch (_) {}

  // ── Stage 10: Survivorship write ────────────────────────────────────────────
  await writePipeStatus(env, "S10_SURVIVORSHIP", 88, { runId: runId });
  try {
    await env.KITE_STORE.put("qe_pipe_survivorship",
      JSON.stringify(survive.getAll().slice(0, 1000)));
  } catch (_) {}

  // ── Stage 11: Telegram Dispatch ─────────────────────────────────────────────
  await writePipeStatus(env, "S11_TELEGRAM", 93, { runId: runId });
  const signalCount = await pipeDispatchTelegram(env, candidates, audit);

  // ── Final: Write audit + run summary ────────────────────────────────────────
  const completedAt = new Date().toISOString();
  const lastRun = {
    runId:          runId,
    runDate:        completedAt.slice(0, 10),
    startedAt:      startedAt,
    completedAt:    completedAt,
    universeCount:  universe.length,
    bhavCount:      Object.keys(bhav).length,
    streamAFast:    streamAFast.length,
    ohlcvQueue:     ohlcvQueue.length,
    ohlcvCapped:    ohlcvDropped.length,
    ohlcvFetched:   Object.keys(ohlcvMap).length,
    streamATech:    streamATech.length,
    rsPassCount:    rsRanked.length,
    streamACount:   sectorFiltered.length,
    candidateCount: candidates.length,
    signalCount:    signalCount,
    survivorCount:  survive.getAll().length,
    niftyAvailable: niftyCloses !== null,
  };

  try {
    await env.KITE_STORE.put("qe_pipe_last_run", JSON.stringify(lastRun));
    await env.KITE_STORE.put("qe_pipe_audit",
      JSON.stringify(audit.getAll().slice(0, 500)));
  } catch (_) {}

  await writePipeStatus(env, "COMPLETED", 100, {
    runId:          runId,
    completedAt:    completedAt,
    candidateCount: candidates.length,
    signalCount:    signalCount,
  });

  audit.log("PIPELINE", "", "COMPLETE",
    "Run " + runId + " done. Candidates: " + candidates.length
    + " Signals: " + signalCount
    + " Survivors dropped: " + survive.getAll().length);

  return {
    ok:             true,
    runId:          runId,
    candidateCount: candidates.length,
    signalCount:    signalCount,
    stats:          lastRun,
  };
}

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE HTTP ROUTE HANDLERS
// ═══════════════════════════════════════════════════════════════════════════════

// GET /pipe/trigger — manually trigger full pipeline run
async function handlePipeTrigger(env) {
  // Check token first
  let token;
  try { token = await getToken(env); } catch (e) {
    return corsErr("Kite token missing — login at /login first: " + e.message, 401);
  }

  // Check if pipeline is already running
  try {
    const statusRaw = await env.KITE_STORE.get("qe_pipe_status");
    if (statusRaw) {
      const status = JSON.parse(statusRaw);
      if (status.phase && !["COMPLETED", "FAILED", "COMPLETED_EMPTY", "STARTING"].includes(status.phase)) {
        const ageMs = Date.now() - new Date(status.updatedAt).getTime();
        // Running for < 35 minutes = still in progress (Worker CPU budget ~30s but cron extends)
        if (ageMs < 35 * 60 * 1000) {
          return cors({
            status:  "already_running",
            message: "Pipeline is already running: " + status.phase + " (" + Math.round(ageMs / 1000) + "s ago)",
            phase:   status.phase,
            pct:     status.pct,
          });
        }
      }
    }
  } catch (_) {}

  // Run pipeline — note: this is synchronous in the HTTP handler
  // Cloudflare Workers paid plan gives 30s CPU per request
  // For large universes, the pipeline may hit the limit on OHLCV fetch.
  // In production, use the cron trigger which runs in background via ctx.waitUntil.
  // Manual trigger via HTTP is for testing + small universe runs.
  try {
    const result = await runFullPipeline(env);
    return cors({
      status:         "success",
      run_id:         result.runId,
      candidate_count: result.candidateCount,
      signal_count:   result.signalCount,
      stats:          result.stats,
      message:        `Pipeline complete. ${result.candidateCount} candidates, ${result.signalCount} signals sent.`,
    });
  } catch (e) {
    return corsErr("Pipeline error: " + e.message, 500);
  }
}

// GET /pipe/status — current pipeline run status
async function handlePipeStatus(env) {
  try {
    const statusRaw  = await env.KITE_STORE.get("qe_pipe_status");
    const lastRunRaw = await env.KITE_STORE.get("qe_pipe_last_run");
    const runId      = await env.KITE_STORE.get("qe_pipe_run_id");

    const status  = statusRaw  ? JSON.parse(statusRaw)  : null;
    const lastRun = lastRunRaw ? JSON.parse(lastRunRaw) : null;

    return cors({
      status:   "success",
      current:  status,
      last_run: lastRun,
      run_id:   runId,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// GET /pipe/signals — read completed signals from KV (browser polls this)
async function handlePipeSignals(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_signals");
    const signals = raw ? JSON.parse(raw) : [];
    const lastRunRaw = await env.KITE_STORE.get("qe_pipe_last_run");
    const lastRun = lastRunRaw ? JSON.parse(lastRunRaw) : null;
    return cors({
      status:   "success",
      count:    signals.length,
      signals:  signals,
      last_run: lastRun ? { runId: lastRun.runId, runDate: lastRun.runDate, completedAt: lastRun.completedAt } : null,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// GET /pipe/candidates — read pre-deep-analysis candidates
async function handlePipeCandidates(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_candidates");
    const candidates = raw ? JSON.parse(raw) : [];
    return cors({
      status:     "success",
      count:      candidates.length,
      candidates: candidates,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// GET /pipe/audit — pipeline audit log for last run
async function handlePipeAudit(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_audit");
    const entries = raw ? JSON.parse(raw) : [];
    return cors({
      status:  "success",
      count:   entries.length,
      entries: entries,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// GET /pipe/survivorship — eliminated stocks with reasons
async function handlePipeSurvivorship(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_survivorship");
    const entries = raw ? JSON.parse(raw) : [];

    // Group by stage for summary
    const byStage = {};
    for (let i = 0; i < entries.length; i++) {
      const e = entries[i];
      if (!byStage[e.stage]) byStage[e.stage] = 0;
      byStage[e.stage]++;
    }

    return cors({
      status:    "success",
      total:     entries.length,
      by_stage:  byStage,
      entries:   entries,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// POST /pipe/deep-result — browser posts completed deep analysis result per symbol
// Browser runs analyseWithRetry(symbol) and posts the score/verdict/bt/mc result here.
// Worker merges it into qe_pipe_signals so the final signal has both DS and QS.
async function handlePipeDeepResult(request, env) {
  let body;
  try { body = await request.json(); } catch (_) {
    return corsErr("Invalid JSON");
  }

  const { symbol, quantScore, verdict, bt, mc, entry, sl, t1, t2, targetDays } = body;
  if (!symbol) return corsErr("Required: symbol");

  try {
    // Read current signals
    const raw     = await env.KITE_STORE.get("qe_pipe_signals");
    const signals = raw ? JSON.parse(raw) : [];

    // Find this symbol in signals
    let found = false;
    for (let i = 0; i < signals.length; i++) {
      if (signals[i].symbol === symbol.toUpperCase()) {
        signals[i].deepResult = {
          quantScore:  quantScore  || null,
          verdict:     verdict     || null,
          bt:          bt          || null,
          mc:          mc          || null,
          entry:       entry       || signals[i].entry,
          sl:          sl          || signals[i].sl,
          t1:          t1          || signals[i].t1,
          t2:          t2          || signals[i].t2,
          targetDays:  targetDays  || null,
          analysedAt:  new Date().toISOString(),
        };
        found = true;
        break;
      }
    }

    if (!found) {
      return corsErr("Symbol not found in current pipeline signals: " + symbol, 404);
    }

    await env.KITE_STORE.put("qe_pipe_signals", JSON.stringify(signals));

    return cors({
      status:  "success",
      symbol:  symbol.toUpperCase(),
      message: "Deep result merged into pipeline signals",
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// SCREENER.IN FUNDAMENTAL PARSER (server-side, Worker)
// Parses PE, ROE, Revenue Growth, Profit Growth, D/E, Market Cap from HTML.
// Cloudflare Workers have no DOM — uses regex on raw HTML instead of DOMParser.
// ═══════════════════════════════════════════════════════════════════════════════
function parseScreenerFundamentals(html, symbol) {
  try {
    var pe     = null;
    var roe    = null;
    var de     = null;
    var revGr  = null;
    var profGr = null;
    var mcap   = null;

    // ── Key ratios: scan #top-ratios li blocks ────────────────────────────────
    // Pattern: <li ...><span class="name">Label</span>...<span class="number">Value</span>
    var ratioPattern = /<li[^>]*>[\s\S]*?<span[^>]*class="[^"]*name[^"]*"[^>]*>([\s\S]*?)<\/span>[\s\S]*?<span[^>]*class="[^"]*number[^"]*"[^>]*>([\s\S]*?)<\/span>/gi;
    var match;
    while ((match = ratioPattern.exec(html)) !== null) {
      var lbl = match[1].replace(/<[^>]+>/g, '').trim().toLowerCase();
      var raw = match[2].replace(/<[^>]+>/g, '').replace(/,/g, '').trim();
      var val = parseFloat(raw);
      if (isNaN(val)) continue;

      if (/^stock p\/e$|^p\/e$/i.test(lbl))         pe  = val;
      if (/return on equity/i.test(lbl))             roe = val;
      if (/debt\s*\/\s*equity|debt to equity/i.test(lbl)) de = val;
      if (/market cap/i.test(lbl))                   mcap = val * 1e7; // Screener shows Cr
    }

    // ── Revenue & Profit Growth: scan P&L table rows ──────────────────────────
    // Find Sales/Revenue row and Net Profit row — get last 2 non-zero numeric values
    function extractGrowth(rowPattern) {
      var rowMatch = rowPattern.exec(html);
      if (!rowMatch) return null;
      var rowHtml = rowMatch[0];
      var cellPattern = /<td[^>]*>([\s\S]*?)<\/td>/gi;
      var cells = [];
      var cellMatch;
      while ((cellMatch = cellPattern.exec(rowHtml)) !== null) {
        var txt = cellMatch[1].replace(/<[^>]+>/g, '').replace(/,/g, '').trim();
        var num = parseFloat(txt);
        if (!isNaN(num) && num !== 0) cells.push(num);
      }
      if (cells.length < 2) return null;
      var curr = cells[cells.length - 1];
      var prev = cells[cells.length - 2];
      if (prev === 0) return null;
      return parseFloat(((curr - prev) / Math.abs(prev) * 100).toFixed(1));
    }

    // Sales/Revenue row
    var salesPattern = /<tr[^>]*>[\s\S]*?<td[^>]*>\s*(?:Sales|Revenue)\s*<\/td>([\s\S]*?)<\/tr>/i;
    revGr = extractGrowth(salesPattern);

    // Net Profit row
    var profitPattern = /<tr[^>]*>[\s\S]*?<td[^>]*>\s*(?:Net Profit|PAT)\s*<\/td>([\s\S]*?)<\/tr>/i;
    profGr = extractGrowth(profitPattern);

    // Return null if nothing was found
    if (pe === null && roe === null && revGr === null && profGr === null && de === null) {
      return null;
    }

    return { pe: pe, roe: roe, revGr: revGr, profGr: profGr, de: de, mcap: mcap,
             _source: "screener_worker" };
  } catch (_) {
    return null;
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN FETCH HANDLER — all v3.1 routes preserved exactly
// ═══════════════════════════════════════════════════════════════════════════════
export default {

  // ── Scheduled cron handler ──────────────────────────────────────────────────
  async scheduled(event, env, ctx) {
    const cron = event.cron;

    // 03:15 UTC Mon–Fri = 08:45 IST — Auth reminder
    if (cron === "15 3 * * 2-6") {
      ctx.waitUntil(sendAuthReminder(env));
    }

    // 03:45 UTC Mon–Fri = 09:15 IST — Legacy discovery trigger
    // In v4.0 this now fires the server-side pipeline
    if (cron === "45 3 * * 2-6") {
      ctx.waitUntil(triggerDiscoveryScan(env));
    }

    // 04:00 UTC Mon–Fri = 09:30 IST — Pipeline run 1: market open
    if (cron === "0 4 * * 2-6") {
      ctx.waitUntil(runFullPipeline(env));
    }

    // 06:00 UTC Mon–Fri = 11:30 IST — Pipeline run 2: mid-morning re-scan
    if (cron === "0 6 * * 2-6") {
      ctx.waitUntil(runFullPipeline(env));
    }

    // 08:00 UTC Mon–Fri = 13:30 IST — Pipeline run 3: post-lunch re-scan
    if (cron === "0 8 * * 2-6") {
      ctx.waitUntil(runFullPipeline(env));
    }

    // 09:00 UTC Mon–Fri = 14:30 IST — Pipeline run 4: pre-close re-scan
    if (cron === "0 9 * * 2-6") {
      ctx.waitUntil(runFullPipeline(env));
    }

    // Every 30 min 04:00–10:00 UTC Mon–Fri = 09:30–15:30 IST — Position monitor
    if (cron === "*/30 4-10 * * 2-6") {
      ctx.waitUntil(monitorPositions(env));
    }

    // 10:30 UTC Mon–Fri = 16:00 IST — Daily summary
    if (cron === "30 10 * * 2-6") {
      ctx.waitUntil(sendDailySummary(env));
    }

    // 03:00 UTC Sunday = 08:30 IST Sunday — Weekly universe rebuild
    if (cron === "0 3 * * 0") {
      ctx.waitUntil(buildUniverse(env));
    }
  },

  // ── HTTP fetch handler ──────────────────────────────────────────────────────
  async fetch(request, env) {
    const url    = new URL(request.url);
    const path   = url.pathname;
    const method = request.method;

    if (method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS });
    }

    // ══ NEW ROUTES (v3.0) ════════════════════════════════════════════════════

    // POST /telegram/callback — Telegram inline button handler (Priority 1)
    if (path === "/telegram/callback" && method === "POST") {
      return handleTelegramCallback(request, env);
    }

    // GET /kv/get — safe KV read for frontend (extended in v4.0)
    if (path === "/kv/get" && method === "GET") {
      return handleKvGet(url, env);
    }

    // POST /tg/register — store Telegram credentials from UI
    if (path === "/tg/register" && method === "POST") {
      return handleTgRegister(request, env);
    }

    // POST /signal/store — store signal for callback verification
    if (path === "/signal/store" && method === "POST") {
      return handleSignalStore(request, env);
    }

    // GET /tg/status — check if Telegram is configured
    if (path === "/tg/status" && method === "GET") {
      const { ok } = await getTgCreds(env);
      return cors({ status: "success", telegram_configured: ok });
    }

    // ══ UNIVERSE MANAGER ROUTES (v3.1) ═══════════════════════════════════════

    if (path === "/universe/refresh" && method === "GET") {
      return handleUniverseRefresh(env);
    }

    if (path === "/universe/status" && method === "GET") {
      return handleUniverseStatus(env);
    }

    // ══ PIPELINE ROUTES (v4.0) ════════════════════════════════════════════════

    // GET /pipe/trigger — manually run the full server-side discovery pipeline
    if (path === "/pipe/trigger" && method === "GET") {
      return handlePipeTrigger(env);
    }

    // GET /pipe/status — pipeline progress / last run summary
    if (path === "/pipe/status" && method === "GET") {
      return handlePipeStatus(env);
    }

    // GET /pipe/signals — completed signals for browser to render
    if (path === "/pipe/signals" && method === "GET") {
      return handlePipeSignals(env);
    }

    // GET /pipe/candidates — pre-deep-analysis candidates list
    if (path === "/pipe/candidates" && method === "GET") {
      return handlePipeCandidates(env);
    }

    // GET /pipe/audit — full pipeline audit log
    if (path === "/pipe/audit" && method === "GET") {
      return handlePipeAudit(env);
    }

    // GET /pipe/survivorship — eliminated stocks with rejection reasons
    if (path === "/pipe/survivorship" && method === "GET") {
      return handlePipeSurvivorship(env);
    }

    // POST /pipe/deep-result — browser posts deep analysis result per symbol
    if (path === "/pipe/deep-result" && method === "POST") {
      return handlePipeDeepResult(request, env);
    }

    // ══ ALL V2.5 ROUTES BELOW — PRESERVED EXACTLY ════════════════════════════

    // GET / — Root: status + OHLCV + fundamentals
    if ((path === "/" || path === "") && method === "GET") {
      const symbol   = url.searchParams.get("symbol");
      const interval = url.searchParams.get("interval");
      const range    = url.searchParams.get("range");
      const type     = url.searchParams.get("type");

      if (!symbol) {
        const token = await env.KITE_STORE.get(KV_TOKEN_KEY);
        return cors({ kite: token ? "connected" : "disconnected",
                      status: "success", version: "4.4" });
      }

      if (type === "fundamentals") {
        // v4.4 FIX: Yahoo Finance v10/quoteSummary requires crumb auth since late 2024.
        // Both browser and Worker calls to v10 fail (crumb required / IP blocked).
        // Solution: Worker fetches Screener.in server-side — no CORS restriction from Worker.
        // Parses PE, ROE, Revenue Growth, Profit Growth, D/E from Screener HTML.
        // Returns structured JSON that browser renders directly — no browser parsing needed.
        try {
          const cleanSym = symbol.replace(/\.NS$|\.BO$/i, "").toUpperCase().trim();
          const screenerUrls = [
            "https://www.screener.in/company/" + cleanSym + "/consolidated/",
            "https://www.screener.in/company/" + cleanSym + "/",
          ];
          const headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
          };

          let fundData = null;

          for (let si = 0; si < screenerUrls.length; si++) {
            try {
              const ctrl  = new AbortController();
              const timer = setTimeout(function() { ctrl.abort(); }, 12000);
              let res;
              try {
                res = await fetch(screenerUrls[si], { signal: ctrl.signal, headers });
              } finally {
                clearTimeout(timer);
              }
              if (!res.ok) continue;
              const html = await res.text();
              if (!html || html.includes("Page not found") || html.includes("404")) continue;

              // Parse key ratios from Screener HTML server-side
              const parsed = parseScreenerFundamentals(html, cleanSym);
              if (parsed) { fundData = parsed; break; }
            } catch (_) { continue; }
          }

          if (!fundData) {
            return cors({ status: "error", source: "screener",
                          message: "Screener.in data unavailable for " + cleanSym,
                          fundamentals: null }, 200);
          }

          return cors({
            status: "success",
            source: "screener",
            symbol: cleanSym,
            fundamentals: fundData,
          });
        } catch (e) {
          return cors({ status: "error", source: "screener",
                        message: e.message, fundamentals: null }, 200);
        }
      }

      const decodedSym  = (function() { try { return decodeURIComponent(symbol); } catch(_) { return symbol; } })();
      const cleanSym    = decodedSym.replace(/\.NS$|\.BO$/, "").toUpperCase();
      const isGlobalSym = decodedSym.startsWith("^") || decodedSym.includes("=F") ||
                          decodedSym.includes("=X") || decodedSym.startsWith("%5E") ||
                          symbol.startsWith("%25") || symbol.includes("%3D");

      if (isGlobalSym) return await proxyYahooFinance(decodedSym, interval, range);

      try {
        const token    = await getToken(env);
        const now      = new Date();
        const msDay    = 86400000;
        const rangeMap = { "5d":5, "1mo":30, "3mo":90, "6mo":180, "1y":365, "2y":730, "5y":1825 };
        const days     = rangeMap[range] || 365;
        const fromStr  = new Date(now - days * msDay).toISOString().slice(0, 10);
        const toStr    = now.toISOString().slice(0, 10);
        const intervalMap = { "1d":"day","1wk":"week","1mo":"month",
                              "5m":"5minute","15m":"15minute","60m":"60minute" };
        const kiteInterval = intervalMap[interval] || "day";

        const quoteRes = await kiteRequest("GET", `/quote?i=NSE:${encodeURIComponent(cleanSym)}`, null, token);
        if (!quoteRes.ok) throw new Error("Quote failed: " + (quoteRes.data.message || quoteRes.status));
        const instrToken = quoteRes.data.data["NSE:" + cleanSym].instrument_token;
        if (!instrToken) throw new Error("No instrument token for " + cleanSym);

        const histRes = await kiteRequest(
          "GET",
          `/instruments/historical/${instrToken}/${kiteInterval}?from=${fromStr}&to=${toStr}`,
          null, token
        );
        if (!histRes.ok) throw new Error("Historical fetch failed: " + (histRes.data.message || histRes.status));

        const candles = (histRes.data.data && histRes.data.data.candles) || [];
        if (!candles.length) throw new Error("No candles returned from Kite");

        return cors({
          status: "success", source: "kite",
          chart: { result: [{
            meta: { symbol: cleanSym, currency: "INR", exchangeName: "NSE", dataSource: "kite" },
            timestamp: candles.map(function(c) { return Math.floor(new Date(c[0]).getTime() / 1000); }),
            indicators: { quote: [{
              open:   candles.map(function(c) { return c[1]; }),
              high:   candles.map(function(c) { return c[2]; }),
              low:    candles.map(function(c) { return c[3]; }),
              close:  candles.map(function(c) { return c[4]; }),
              volume: candles.map(function(c) { return c[5]; })
            }] }
          }], error: null }
        });
      } catch (e) {
        return await proxyYahooFinance(decodedSym, interval, range);
      }
    }

    // GET /login
    if (path === "/login" && method === "GET") {
      const loginUrl = `https://kite.zerodha.com/connect/login?api_key=${API_KEY}&v=3`;
      return Response.redirect(loginUrl, 302);
    }

    // GET /callback + /auth
    if ((path === "/callback" || path === "/auth") && method === "GET") {
      const requestToken = url.searchParams.get("request_token");
      if (!requestToken) return corsErr("Missing request_token");
      const apiSecret = await env.KITE_STORE.get("api_secret");
      if (!apiSecret) return corsErr("API secret not configured in KV");
      const raw = `${API_KEY}${requestToken}${apiSecret}`;
      const hashBuffer = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(raw));
      const checksum = Array.from(new Uint8Array(hashBuffer))
        .map(function(b) { return b.toString(16).padStart(2, "0"); }).join("");
      const resp = await fetch(`${KITE_API_BASE}/session/token`, {
        method: "POST",
        headers: { "X-Kite-Version": "3", "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ api_key: API_KEY, request_token: requestToken, checksum: checksum }).toString(),
      });
      const data = await resp.json();
      if (!resp.ok) return corsErr(data.message || "Session generation failed", 401);
      const accessToken = data.data.access_token;
      await env.KITE_STORE.put(KV_TOKEN_KEY, accessToken);
      await env.KITE_STORE.put("kite_token_timestamp", String(Date.now()));
      return new Response(
        `<html><body style="font-family:monospace;padding:2rem">
          <h2>✅ Kite Login Successful</h2>
          <p>Access token stored. QuantEdge KITE badge will show ✓</p>
          <p><a href="${QE_URL}">→ Open QuantEdge</a></p>
        </body></html>`,
        { status: 200, headers: { "Content-Type": "text/html" } }
      );
    }

    // GET /token
    if (path === "/token" && method === "GET") {
      const token = await env.KITE_STORE.get(KV_TOKEN_KEY);
      return cors({ status: "success", has_token: !!token });
    }

    // GET /quote
    if (path === "/quote" && method === "GET") {
      const symbol = url.searchParams.get("symbol");
      if (!symbol) return corsErr("Missing symbol parameter");
      try {
        const token = await getToken(env);
        const { ok, data } = await kiteRequest("GET", `/quote?i=NSE:${encodeURIComponent(symbol)}`, null, token);
        if (!ok) return corsErr(data.message || "Quote fetch failed", 502);
        return cors({ status: "success", data: data.data });
      } catch (e) { return corsErr(e.message, 401); }
    }

    // GET /instruments/NSE
    if (path === "/instruments/NSE" && method === "GET") {
      try {
        const token = await getToken(env);
        const resp  = await fetch(`${KITE_API_BASE}/instruments/NSE`, {
          headers: { "X-Kite-Version": "3", Authorization: kiteAuthHeader(token) },
        });
        if (!resp.ok) return corsErr("Instruments fetch failed", 502);
        const csv = await resp.text();
        return new Response(csv, { headers: { "Content-Type": "text/csv", ...CORS } });
      } catch (e) { return corsErr(e.message, 401); }
    }

    // POST /gtt/create
    if (path === "/gtt/create" && method === "POST") {
      let body;
      try { body = await request.json(); } catch (_) { return corsErr("Invalid JSON body"); }
      const { symbol, cmp, entry, quantity } = body;
      if (!symbol || !cmp || !entry || !quantity) return corsErr("Required: symbol, cmp, entry, quantity");
      if (quantity <= 0)           return corsErr("Quantity must be > 0");
      if (entry <= 0 || cmp <= 0)  return corsErr("Price values must be > 0");
      const triggerPrice = parseFloat(entry).toFixed(2);
      const limitPrice   = parseFloat(entry).toFixed(2);
      const lastPrice    = parseFloat(cmp).toFixed(2);
      const condition = JSON.stringify({ exchange: "NSE", tradingsymbol: symbol.toUpperCase(),
                                         trigger_values: [parseFloat(triggerPrice)],
                                         last_price: parseFloat(lastPrice) });
      const orders = JSON.stringify([{ exchange: "NSE", tradingsymbol: symbol.toUpperCase(),
                                       transaction_type: "BUY", quantity: parseInt(quantity, 10),
                                       order_type: "LIMIT", product: "CNC",
                                       price: parseFloat(limitPrice) }]);
      try {
        const token = await getToken(env);
        const { ok, data } = await kiteRequest("POST", "/gtt/triggers",
                                               { type: "single", condition, orders }, token);
        if (!ok) return corsErr(data.message || "GTT creation failed at Kite API",
                                data.status || 502);
        const triggerId = data.data.trigger_id;
        await appendGttLog(env, { timestamp: new Date().toISOString(),
                                   symbol: symbol.toUpperCase(), entry: parseFloat(entry),
                                   sl: body.sl || null, t1: body.t1 || null,
                                   quantity: parseInt(quantity, 10), trigger_id: triggerId,
                                   source: "ui" });
        return cors({ status: "success", trigger_id: triggerId,
                      message: `GTT created for ${symbol.toUpperCase()} @ ₹${triggerPrice} | Qty: ${quantity}`,
                      kite_url: "https://kite.zerodha.com/gtt" });
      } catch (e) { return corsErr(e.message, e.message.includes("token") ? 401 : 502); }
    }

    // GET /gtt/list
    if (path === "/gtt/list" && method === "GET") {
      try {
        const token = await getToken(env);
        const { ok, data } = await kiteRequest("GET", "/gtt/triggers", null, token);
        if (!ok) return corsErr(data.message || "GTT list fetch failed", 502);
        const gtts = (data.data || []).filter(function(g) { return g.status === "active"; })
          .map(function(g) {
            return { trigger_id: g.id, symbol: g.condition && g.condition.tradingsymbol,
                     trigger_price: g.condition && g.condition.trigger_values && g.condition.trigger_values[0],
                     order_price: g.orders && g.orders[0] && g.orders[0].price,
                     quantity: g.orders && g.orders[0] && g.orders[0].quantity,
                     product: g.orders && g.orders[0] && g.orders[0].product,
                     type: g.orders && g.orders[0] && g.orders[0].transaction_type,
                     created_at: g.created_at, status: g.status };
          });
        return cors({ status: "success", count: gtts.length, gtts: gtts });
      } catch (e) { return corsErr(e.message, 401); }
    }

    // DELETE /gtt/delete/:id
    if (path.startsWith("/gtt/delete/") && method === "DELETE") {
      const triggerId = path.split("/gtt/delete/")[1];
      if (!triggerId || isNaN(triggerId)) return corsErr("Invalid trigger_id");
      try {
        const token = await getToken(env);
        const resp  = await fetch(`${KITE_API_BASE}/gtt/triggers/${triggerId}`, {
          method: "DELETE",
          headers: { "X-Kite-Version": "3", Authorization: kiteAuthHeader(token) },
        });
        const data = await resp.json();
        if (!resp.ok) return corsErr(data.message || "GTT delete failed", 502);
        return cors({ status: "success", message: `GTT #${triggerId} cancelled successfully` });
      } catch (e) { return corsErr(e.message, 401); }
    }

    return corsErr(`Unknown route: ${method} ${path}`, 404);
  },
};
