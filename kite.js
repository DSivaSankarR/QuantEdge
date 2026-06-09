/**
 * QuantEdge Cloudflare Worker — kite.js v4.4
 *
 * Changelog v4.4 (05-Jun-2026) — Fundamentals: Screener.in server-side:
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
      .map(b => ("00" + b.toString(16)).slice(-2)).join("").slice(0, 32);
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
      .map(b => ("00" + b.toString(16)).slice(-2)).join("").slice(0, 32);
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
  } catch (e) { console.warn("[appendGttLog] non-fatal:", e && e.message); }
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
    } catch (e) { console.warn("[handleTelegramCallback] non-fatal:", e && e.message); }

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
    } catch (e) { console.warn("[handleTelegramCallback] non-fatal:", e && e.message); }

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

  const allKiteGTTs = data.data || [];
  // R2 fix: build a map of trigger_id → actual Kite status. The previous logic
  // inferred "triggered" from ABSENCE in the active list — but a GTT also
  // disappears from active when it is cancelled, deleted, rejected, or expired,
  // producing a FALSE "GTT Triggered" alert. We now read the real status field
  // and only fire the trigger alert when status === "triggered".
  const statusById = {};
  for (let k = 0; k < allKiteGTTs.length; k++) {
    statusById[String(allKiteGTTs[k].id)] = allKiteGTTs[k].status;
  }

  const raw    = await env.KITE_STORE.get("qe_gtt_log");
  const ourLog = raw ? JSON.parse(raw) : [];

  const alerts  = [];

  for (let i = 0; i < ourLog.length; i++) {
    const logged = ourLog[i];
    if (!logged.trigger_id) continue;
    const kiteStatus = statusById[String(logged.trigger_id)];
    // Genuine fill only — explicit "triggered" status. Absence (undefined),
    // "cancelled", "deleted", "rejected", "expired" are NOT trade fills.
    const triggered = kiteStatus === "triggered";
    if (triggered && !logged.alerted) {
      alerts.push(logged);
      logged.alerted   = true;
      logged.alertedAt = new Date().toISOString();
    }
    const age = (Date.now() - new Date(logged.timestamp).getTime()) / (1000 * 60 * 60 * 24);
    // Stale check only applies to GTTs that are STILL active (not filled/gone).
    if (kiteStatus === "active" && age > 25 && !logged.staleAlerted) {
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
  } catch (e) { console.warn("[sendDailySummary] non-fatal:", e && e.message); }

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
  const colInstrToken     = headers.indexOf("instrument_token"); // Commit 1: capture token

  if (colTradingsymbol < 0 || colInstrumentType < 0) {
    return { ok: false, error: "CSV missing required columns. Got: " + headers.join(","), count: 0 };
  }

  const symbols  = [];
  const tokenMap = {}; // Commit 1: symbol → instrument_token, eliminates per-symbol /quote in S4
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
    // Commit 1: capture instrument token for this symbol (used by S4 history fetch)
    if (colInstrToken >= 0) {
      const tok = parseInt((cols[colInstrToken] || "").trim().replace(/"/g, ""), 10);
      if (tok > 0) tokenMap[symbol] = tok;
    }
  }

  if (symbols.length < 50) {
    return { ok: false, error: "Too few symbols after filter: " + symbols.length, count: 0 };
  }

  const ts = Date.now();
  try {
    await env.KITE_STORE.put("qe_db_universe",       JSON.stringify(symbols));
    await env.KITE_STORE.put("qe_db_universe_ts",    String(ts));
    await env.KITE_STORE.put("qe_db_universe_count", String(symbols.length));
    // Commit 1: persist symbol→token map (TTL 8 days; rebuilt weekly with universe)
    await env.KITE_STORE.put("qe_db_token_map", JSON.stringify(tokenMap),
      { expirationTtl: 8 * 24 * 60 * 60 });
  } catch (e) {
    return { ok: false, error: "KV write failed: " + e.message, count: symbols.length };
  }

  return {
    ok:         true,
    count:      symbols.length,
    tokenCount: Object.keys(tokenMap).length,
    builtAt:    new Date(ts).toISOString(),
    sample:     symbols.slice(0, 10),
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
//   Stage 1  — Universe load          (KV read)
//   Stage 2  — NSE Bhav Copy ingest   (Kite /quote bulk — last_price, volume, change)
//   Stage 3  — Stream A Fast filters  (bhav-only: price, volume, circuit — no history)
//   Stage 4  — OHLCV fetch + compute  (Kite historical per symbol, batched)
//   Stage 5  — Stream A Technical      (EMA stack, RSI, ADX, Supertrend, ATR, volume)
//   Stage 5B — Stream B Discovery      (hidden-gem branch on same ohlcvMap)
//   Stage 6  — RS Engine              (percentile rank vs Nifty, 3-period weighted)
//   Stage 7  — Sector Engine          (sector concentration limit)
//   Stage 8  — Merge Engine           (Discovery Score, rank, top N)
//   Stage 9  — KV signal store         (candidates + signals JSON for browser)
//   Stage 10 — Survivorship write     (all eliminated symbols logged with reason)
//   Stage 11 — Telegram dispatch       (top signals with WATCH/SKIP buttons)
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
const PIPE_MIN_CANDLES   = 220;   // min daily bars (EMA200 needs 200; +20 margin for reliable seed)
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
  } catch (e) { console.warn("[writePipeStatus] non-fatal:", e && e.message); }
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
const BHAV_BATCH_SIZE = 250;   // Kite full /quote hard cap = 250 instruments/call (was 400 → HTTP 403)

// Parse one /quote response into the bhav map. Returns count written.
function pipeWriteQuotesToBhav(quotes, batch, bhav) {
  let written = 0;
  for (let bi = 0; bi < batch.length; bi++) {
    const sym = batch[bi];
    const q   = quotes["NSE:" + sym];
    if (!q) continue;
    const last   = q.last_price || 0;
    const ohlc   = q.ohlc || {};
    const prev   = ohlc.close || last;
    const vol    = q.volume || 0;
    const chgPct = prev > 0 ? ((last - prev) / prev) * 100 : 0;
    bhav[sym] = {
      last_price: last, prev_close: prev, volume: vol, change_pct: chgPct,
      day_open: ohlc.open || 0, day_high: ohlc.high || 0, day_low: ohlc.low || 0,
    };
    written++;
  }
  return written;
}

// Fetch one batch of symbols via /quote. Evidence (audit 2026-06-09 run
// 811de142): a 250-sym batch returned HTTP 403, but the SAME symbols succeeded
// on smaller/slower calls with ZERO poison symbols found — i.e. the 403 is a
// transient Kite rate-limit, not a bad ticker. So we do ONE retry after a short
// delay rather than a recursive split (the split fan-out consumed ~10 extra
// subrequests and tripped the 50-cap, failing 6 OHLCV fetches + the completion
// message). One retry = at most 1 extra subrequest. Cost-safe.
async function pipeFetchQuoteBatch(env, token, syms, bhav, audit) {
  if (syms.length === 0) return;
  const istr = syms.map(function(s) { return "i=NSE:" + encodeURIComponent(s); }).join("&");

  for (let attempt = 1; attempt <= 2; attempt++) {
    let resp;
    try {
      resp = await fetch(`${KITE_API_BASE}/quote?${istr}`,
        { headers: { "X-Kite-Version": "3", "Authorization": kiteAuthHeader(token) } });
    } catch (e) {
      audit.log("S2_BHAV", "", "WARN", "Quote batch fetch error: " + e.message);
      return;
    }
    if (resp.ok) {
      const data = await resp.json();
      pipeWriteQuotesToBhav((data && data.data) ? data.data : {}, syms, bhav);
      return;
    }
    // Transient rate-limit (403/429/5xx) → wait and retry ONCE.
    const retryable = resp.status === 403 || resp.status === 429 || resp.status >= 500;
    if (attempt === 1 && retryable) {
      audit.log("S2_BHAV", "", "RETRY",
        "Batch HTTP " + resp.status + " — waiting 800ms then retrying once (rate-limit)");
      await new Promise(function(r) { setTimeout(r, 800); });
      continue;
    }
    audit.log("S2_BHAV", "", "WARN",
      "Batch failed after " + attempt + " attempt(s): HTTP " + resp.status
      + " — " + syms.length + " symbols not fetched this run");
    return;
  }
}

async function pipeBhavCopy(env, token, symbols, audit, survive) {
  audit.log("S2_BHAV", "", "START", "Fetching bhav copy for " + symbols.length + " symbols");

  const bhav = {}; // symbol → { last_price, prev_close, volume, change_pct }

  for (let i = 0; i < symbols.length; i += BHAV_BATCH_SIZE) {
    const batch = symbols.slice(i, i + BHAV_BATCH_SIZE);

    // Healthy batch = 1 /quote call. On transient 403/429/5xx, one spaced
    // retry recovers it (rate-limit, not poison symbol — proven by audit).
    await pipeFetchQuoteBatch(env, token, batch, bhav, audit);

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
  } catch (e) { console.warn("[pipeBhavCopy] non-fatal:", e && e.message); }

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
  } catch (e) { console.warn("[pipeLoadNiftyCloses] non-fatal:", e && e.message); }

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
// Commit 1: instrToken now passed in from the cached token-map (qe_db_token_map),
// eliminating the per-symbol /quote call. This halves S4 subrequest cost from
// 2/symbol (quote+historical) to 1/symbol (historical only).
async function pipeFetchOhlcvSymbol(env, token, symbol, instrToken) {
  const now      = new Date();
  const fromDate = new Date(now.getTime() - (PIPE_OHLCV_RANGE + 10) * 86400000);
  const fromStr  = fromDate.toISOString().slice(0, 10);
  const toStr    = now.toISOString().slice(0, 10);

  // Step 1: instrument token comes from the cached map — NO /quote fetch.
  if (!instrToken || instrToken <= 0) {
    throw new Error("No cached token for " + symbol);
  }

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
  // R3 fix: 365 calendar days ≈ 248 trading days fetched. Use the actual
  // available window (min of 248 and what we have) rather than 252, which
  // silently fell short and made "52w high" really a ~49-week high.
  const win52w      = Math.min(closes.length, 248);
  const hi52w       = Math.max.apply(null, highs.slice(-win52w));
  const prox52w     = hi52w > 0 ? ((lastClose / hi52w) * 100) : null; // % of 52w high
  const volRatio    = (volSma && volSma > 0) ? lastVol / volSma : null;

  // EMA stack (bullish = 20 > 50 > 200 AND price > 20)
  const emaStackBull = (ema20 !== null && ema50 !== null && ema200 !== null)
    ? (ema20 > ema50 && ema50 > ema200 && lastClose > ema20)
    : false;

  // Supertrend bullish
  const stBull = st ? st.direction === 1 : false;

  // ── Stream B field completion (additive — no existing field touched) ──────────
  // All derived from arrays/values already in scope. No new fetches.
  const sbVolSma3   = pipeSma(volumes.slice(-3), 3);
  const sbVolSma10  = pipeSma(volumes.slice(-10), 10);
  const sbVolAccel  = (sbVolSma3 && sbVolSma10) ? (sbVolSma3 > sbVolSma10) : false;
  const sbMtv       = (volSma && lastClose) ? (lastClose * volSma) / 100000 : 0;        // ₹ Lakh/day
  const sbPctBelow52w = (hi52w > 0) ? ((hi52w - lastClose) / hi52w) * 100 : 99;          // % BELOW high
  const sbH20       = Math.max.apply(null, highs.slice(-20));
  const sbL20       = Math.min.apply(null, lows.slice(-20));
  const sbRange20pct = (sbL20 > 0 && isFinite(sbL20)) ? ((sbH20 - sbL20) / sbL20) * 100 : 99;
  const sbRs1m      = (closes.length >= 22)
    ? ((lastClose - closes[closes.length - 22]) / closes[closes.length - 22])
    : 0;

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
    mtv:          sbMtv,
    volAccel:     sbVolAccel,
    pctBelow52w:  sbPctBelow52w,
    range20pct:   sbRange20pct,
    rs1m:         sbRs1m,
  };
}

// ─── OHLCV batch runner ───────────────────────────────────────────────────────
async function pipeFetchOhlcvBatch(env, token, symbols, audit, survive) {
  audit.log("S4_OHLCV", "", "START",
    "Fetching OHLCV for " + symbols.length + " Stream A candidates");

  // Commit 1: load symbol→token map ONCE (KV read, not a subrequest).
  let tokenMap = {};
  try {
    const raw = await env.KITE_STORE.get("qe_db_token_map");
    if (raw) tokenMap = JSON.parse(raw);
  } catch (e) { console.warn("[pipeFetchOhlcvBatch] token-map read failed:", e && e.message); }
  audit.log("S4_OHLCV", "", "TOKENMAP",
    "Loaded " + Object.keys(tokenMap).length + " cached tokens");

  const results = {};

  for (let i = 0; i < symbols.length; i += PIPE_BATCH_SIZE) {
    const batch = symbols.slice(i, i + PIPE_BATCH_SIZE);

    const batchResults = await Promise.all(
      batch.map(async function(sym) {
        try {
          const instrToken = tokenMap[sym];
          const ohlcv = await pipeFetchOhlcvSymbol(env, token, sym, instrToken);
          // Cache per-symbol to KV (TTL 24h = 86400s)
          try {
            await env.KITE_STORE.put(
              "qe_pipe_ohlcv_" + sym,
              JSON.stringify(ohlcv),
              { expirationTtl: 86400 }
            );
          } catch (e) { console.warn("[pipeFetchOhlcvBatch] non-fatal:", e && e.message); }
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
// STREAM B — Hidden-Gem Discovery (B1–B5 + HGS)  [Commit 3 port, Commit 4 wiring]
// Ported VERBATIM from streamb-harness-v2.2.html (applyStreamBFilters + calcHGS),
// differential-tested green against streamb_golden_fixture_v1.json (5/5).
//
// SCOPE NOTE (MVP): Stream B runs on the SAME ohlcvMap as Stream A, i.e. only on
// symbols that survived the bhav pre-pass (pipeStreamAFast) and the OHLCV cap.
// Symbols dropped before Stage 4 are not seen by Stream B. Accepted for MVP.
//
// B2_MIN_AGE = 90 (production decision, matches fixture cfg_snapshot).
// ═══════════════════════════════════════════════════════════════════════════════
function streamBCfg() {
  return {
    SA_MIN_PRICE: 100, SA_MIN_VOL: 200000, SA_ADX_MIN: 18,
    SA_RSI_MIN: 45, SA_RSI_MAX: 75, SA_EMA_STACK: true,
    B2_PRICE_MIN: 20, B2_PRICE_MAX: 800, B2_MIN_AGE: 90,
    B3_VOL_RATIO: 1.5, B3_VOL_ACCEL: true, B3_MIN_MTV_LAKH: 5,
    B4_PROX_52W: 0.15, B4_RANGE_PCT: 0.12, B4_RSI_MIN: 40, B4_RSI_MAX: 68,
    B5_ADX_MIN: 10, B5_ADX_MAX: 28, B5_EMA20_GT_50: true, B5_PRICE_GT_E20: true,
  };
}

// Pure: production ohlcvMap entry -> harness-shaped object. No mutation of input.
function sbMapToHarnessShape(p) {
  return {
    symbol: p.symbol, last: p.lastClose, lastVol: p.lastVol,
    e20: p.ema20, e50: p.ema50, rsi: p.rsi14, adx: p.adx14,
    volRatio: p.volRatio, volAccel: p.volAccel, mtv: p.mtv,
    high52w: p.hi52w, pctBelow52w: p.pctBelow52w, range20pct: p.range20pct,
    emaStackBull: p.emaStackBull, rs1m: p.rs1m, candles: p.candleCount,
  };
}

function sbCalcHGS(data) {
  var score = 0;
  var vs = data.volRatio || 0;
  score += Math.min(20, Math.max(0, (vs - 1.0) * 13.3));
  var tightness = data.range20pct || 20;
  score += Math.min(20, Math.max(0, (15 - tightness) * 1.6));
  var rs = data.rs1m || 0;
  score += Math.min(20, Math.max(0, (rs + 0.05) * 200));
  var rsi = data.rsi || 50;
  var rsiScore = rsi >= 50 && rsi <= 62 ? 20 :
                 rsi >= 45 && rsi < 50  ? 10 :
                 rsi > 62 && rsi <= 68  ? 10 : 0;
  score += rsiScore;
  var prox = data.pctBelow52w || 30;
  score += prox >= 3 && prox <= 15 ? 20 :
           prox > 15 && prox <= 25 ? 8  : 0;
  return Math.round(Math.min(100, Math.max(0, score)));
}

function sbApplyFilters(ohlcv, cfg) {
  var result = {
    symbol: ohlcv.symbol,
    b1: false, b2: false, b3: false, b4: false, b5: false,
    b1reason:'', b2reason:'', b3reason:'', b4reason:'', b5reason:'',
    passAll: false,
  };
  var wouldPassSA = ohlcv.last >= cfg.SA_MIN_PRICE &&
                    ohlcv.lastVol >= cfg.SA_MIN_VOL &&
                    ohlcv.adx >= cfg.SA_ADX_MIN &&
                    ohlcv.rsi >= cfg.SA_RSI_MIN &&
                    ohlcv.rsi <= cfg.SA_RSI_MAX &&
                    ohlcv.emaStackBull;
  if (wouldPassSA) { result.b1 = false; result.b1reason = 'Passes Stream A (not a hidden gem)'; return result; }
  result.b1 = true;

  var price = ohlcv.last;
  var estAgeDays = Math.round(ohlcv.candles * 1.4);
  if (price < cfg.B2_PRICE_MIN) { result.b2 = false; result.b2reason = 'Price below min'; return result; }
  if (price > cfg.B2_PRICE_MAX) { result.b2 = false; result.b2reason = 'Price above max'; return result; }
  if (estAgeDays < cfg.B2_MIN_AGE) { result.b2 = false; result.b2reason = 'Listing age ' + estAgeDays + 'd < ' + cfg.B2_MIN_AGE; return result; }
  result.b2 = true;

  if (!ohlcv.volRatio || ohlcv.volRatio < cfg.B3_VOL_RATIO) { result.b3 = false; result.b3reason = 'Vol ratio below ' + cfg.B3_VOL_RATIO; return result; }
  if (cfg.B3_VOL_ACCEL && !ohlcv.volAccel) { result.b3 = false; result.b3reason = 'No volume acceleration'; return result; }
  if (ohlcv.mtv < cfg.B3_MIN_MTV_LAKH) { result.b3 = false; result.b3reason = 'MTV below ' + cfg.B3_MIN_MTV_LAKH + 'L/d'; return result; }
  result.b3 = true;

  if (ohlcv.pctBelow52w > cfg.B4_PROX_52W * 100) { result.b4 = false; result.b4reason = ohlcv.pctBelow52w.toFixed(1) + '% below 52w high'; return result; }
  if (ohlcv.range20pct > cfg.B4_RANGE_PCT * 100) { result.b4 = false; result.b4reason = '20d range ' + ohlcv.range20pct.toFixed(1) + '% too wide'; return result; }
  if (ohlcv.rsi < cfg.B4_RSI_MIN || ohlcv.rsi > cfg.B4_RSI_MAX) { result.b4 = false; result.b4reason = 'RSI outside band'; return result; }
  result.b4 = true;

  if (!ohlcv.adx || ohlcv.adx < cfg.B5_ADX_MIN) { result.b5 = false; result.b5reason = 'ADX below ' + cfg.B5_ADX_MIN; return result; }
  if (ohlcv.adx > cfg.B5_ADX_MAX) { result.b5 = false; result.b5reason = 'ADX above ' + cfg.B5_ADX_MAX; return result; }
  if (cfg.B5_EMA20_GT_50 && ohlcv.e20 && ohlcv.e50 && ohlcv.e20 <= ohlcv.e50) { result.b5 = false; result.b5reason = 'EMA20 not > EMA50'; return result; }
  if (cfg.B5_PRICE_GT_E20 && ohlcv.e20 && ohlcv.last <= ohlcv.e20) { result.b5 = false; result.b5reason = 'Price not > EMA20'; return result; }
  result.b5 = true;
  result.passAll = true;
  return result;
}

// Orchestrator: run Stream B over the production ohlcvMap. Returns candidate array.
// Pure read of ohlcvMap; logs to audit; drops to survive. Does NOT touch Stream A.
function pipeStreamBTech(ohlcvMap, audit, survive) {
  const cfg = streamBCfg();
  const symbols = Object.keys(ohlcvMap);
  audit.log("S5B_STREAM_B", "", "START", "Stream B over " + symbols.length + " symbols");
  const candidates = [];
  for (let i = 0; i < symbols.length; i++) {
    const sym = symbols[i];
    try {
      const shaped = sbMapToHarnessShape(ohlcvMap[sym]);
      const filt = sbApplyFilters(shaped, cfg);
      if (!filt.passAll) {
        const firstFail = !filt.b1 ? "B1" : !filt.b2 ? "B2" : !filt.b3 ? "B3" : !filt.b4 ? "B4" : "B5";
        const reason = filt.b1reason || filt.b2reason || filt.b3reason || filt.b4reason || filt.b5reason;
        survive.drop(sym, "S5B_STREAM_B", firstFail + ": " + reason);
        continue;
      }
      const hgs = sbCalcHGS({
        volRatio: shaped.volRatio, range20pct: shaped.range20pct,
        rs1m: shaped.rs1m, rsi: shaped.rsi, pctBelow52w: shaped.pctBelow52w
      });
      candidates.push({
        symbol: sym, hgs: hgs,
        last: shaped.last, pctBelow52w: shaped.pctBelow52w,
        volRatio: shaped.volRatio, mtv: shaped.mtv,
        rsi: shaped.rsi, adx: shaped.adx, range20pct: shaped.range20pct,
      });
      audit.log("S5B_STREAM_B", sym, "PASS", "HGS " + hgs);
    } catch (e) {
      // No silent failure — log and drop.
      audit.log("S5B_STREAM_B", sym, "ERROR", e.name + ": " + e.message);
      survive.drop(sym, "S5B_STREAM_B", "Exception: " + e.message);
    }
  }
  candidates.sort(function(a, b) { return b.hgs - a.hgs; });
  audit.log("S5B_STREAM_B", "", "DONE", candidates.length + " Stream B candidates");
  return candidates;
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

  // R5 fix: build a symbol→rank map once (O(n)) instead of findIndex per
  // element (O(n²)). Ties: equal raw values share the rank of the earliest
  // index holding that raw value — deterministic and stable.
  const rankBySym = {};
  let tieRank = 0;
  for (let k = 0; k < sorted.length; k++) {
    if (k > 0 && sorted[k].raw !== sorted[k - 1].raw) tieRank = k;
    rankBySym[sorted[k].sym] = tieRank;
  }

  // Assign percentile rank
  const ranked = rsRaw.map(function(item) {
    const rank = rankBySym[item.sym];
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

function pipeMerge(sectorFiltered, ohlcvMap, audit) {
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

  const top    = candidates.slice(0, PIPE_SIGNAL_TOP);
  const expiry = Date.now() + SIGNAL_TTL_MS;

  // Commit 3: build ONE consolidated message + ONE inline keyboard.
  // KV signal storage per candidate is retained (KV puts are NOT subrequests),
  // so callback verification still works for each signal individually. Only the
  // outbound Telegram fetch collapses from N → 1.
  const expiryStr = new Date(expiry).toLocaleTimeString("en-IN",
    { hour: "2-digit", minute: "2-digit", timeZone: "Asia/Kolkata" });

  const sections    = [];
  const keyboardRows = [];
  let   eligible    = 0;

  for (let i = 0; i < top.length; i++) {
    const c = top[i];

    // ── Signal integrity gate (unchanged logic) ───────────────────────────────
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
      eligible++;
      audit.log("S9_TELEGRAM", c.symbol, "GATE_PASS",
        "DS:" + c.discoveryScore + " ST:bullish ADX:" + (c.adx14 || 0).toFixed(1));
    }

    const signalId = genRunId();
    const hmac     = await signPayload(env, signalId, c.symbol, c.entry, expiry);

    // Store signal in KV for callback verification (per-signal, unchanged)
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
          qty:       1,
          cmp:       c.lastClose,
          expiry:    expiry,
          hmac:      hmac,
          source:    "pipeline_v4",
          watchOnly: watchOnly,
        }),
        { expirationTtl: 1800 }
      );
    } catch (e) { console.warn("[pipeDispatchTelegram] non-fatal:", e && e.message); }

    // ── Per-signal section (same content/formatting as before) ────────────────
    const gateStatusLine = watchOnly
      ? "⚠️ Gate: WATCH_ONLY (deep analysis required before BUY)\n"
      : "✅ Gate: DS✓ Supertrend✓ ADX✓\n";

    sections.push(
      `${watchOnly ? "⚠️" : "🔭"} <b>#${i + 1} ${c.symbol}</b>  [${c.sector}]\n`
      + `📊 Discovery Score: <b>${c.discoveryScore}/100</b>   📈 RS: <b>${c.rsScore}/100</b>\n`
      + `💹 RSI: ${c.rsi14 !== null ? c.rsi14 : "—"}  ADX: ${c.adx14 !== null ? c.adx14 : "—"}  ST: ${c.stBull ? "🟢 Bull" : "🔴 Bear"}\n`
      + gateStatusLine
      + `💰 CMP: ₹${c.lastClose}   🎯 Entry: ₹${c.entry}  SL: ₹${c.sl !== null ? c.sl : "—"}\n`
      + `✅ T1: ₹${c.t1 !== null ? c.t1 : "—"}  T2: ₹${c.t2 !== null ? c.t2 : "—"}`
    );

    // Per-signal callback buttons (Watch / Skip), labelled with the symbol
    const cbData = {
      action: "WATCH", signalId: signalId, symbol: c.symbol, entry: c.entry,
      sl: c.sl, t1: c.t1, t2: c.t2, qty: 1, cmp: c.lastClose, expiry: expiry, hmac: hmac,
    };
    keyboardRows.push([
      { text: "👀 " + c.symbol, callback_data: JSON.stringify(Object.assign({}, cbData, { action: "WATCH"  })) },
      { text: "❌ Skip",        callback_data: JSON.stringify(Object.assign({}, cbData, { action: "REJECT" })) },
    ]);
  }

  // ── Single consolidated message ─────────────────────────────────────────────
  const header = `🔭 <b>QuantEdge Signals</b> — ${eligible}/${top.length} gate-passed\n`
    + `⏱ Expires ${expiryStr} IST · open QuantEdge for deep analysis\n`
    + `━━━━━━━━━━━━━━━━━━━━\n`;
  const body   = sections.join("\n\n━━━━━━━━━━━━━━━━━━━━\n");
  const footer = `\n\n<i>Source: Server Pipeline v4.1</i>`;
  const msg    = header + body + footer;

  const keyboard = { inline_keyboard: keyboardRows };

  const ok = await sendTelegram(env, msg, keyboard);
  const sent = ok ? top.length : 0;
  audit.log("S9_TELEGRAM", "", "DONE",
    "Consolidated send " + (ok ? "OK" : "FAIL") + " — " + top.length + " signals in 1 message");
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
// PIPELINE WRAPPER — intraday re-scan crons (0 6, 0 9)
// Runs the full pipeline then sends a structured Telegram summary with
// bottleneck detection. The 09:30 cron (0 4) calls runFullPipeline directly
// and already has per-stage Telegram alerts; these re-scan runs need their
// own post-completion summary because they fire without a human watching.
// ═══════════════════════════════════════════════════════════════════════════════
async function runPipelineWithSummary(env, label) {
  // ── DIAGNOSTIC HEARTBEAT (unconditional) ───────────────────────────────────
  // Proves the wrapper was entered at all. If this arrives but the summary does
  // not, the fault is between here and sendPipelineSummary. If this does NOT
  // arrive, the cron is not invoking the wrapper. Either way the next run tells
  // us the truth instead of failing silently.
  const hbOk = await sendTelegram(env, `🫀 <b>Re-scan started — ${label}</b>`);

  let result;
  try {
    // Manual triggers skip dedup so an after-hours re-run shows the SAME true
    // top candidates every time (data is frozen post-close). Crons keep dedup
    // to spread coverage across the day. Evidence: audit 811de142 showed manual
    // re-runs returning different candidates due to the analysed-today exclusion.
    const isManual = !!(label && label.indexOf("MANUAL") !== -1);
    result = await runFullPipeline(env, { skipDedup: isManual });
  } catch (e) {
    // Report the ACTUAL error — do not assume it is a missing token.
    await sendTelegram(env,
      `🔴 <b>Pipeline Threw — ${label}</b>\n\n`
      + `<b>${e.name || "Error"}:</b> ${e.message}\n`
      + `<code>${((e.stack || "").split("\n")[1] || "").trim().slice(0, 120)}</code>`
    );
    return { ok: false, error: e.message }; // return so HTTP callers can report it
  }

  // Wrap the summary so a failure INSIDE it is reported instead of swallowed.
  try {
    await sendPipelineSummary(env, result, label);
  } catch (e2) {
    await sendTelegram(env,
      `🟠 <b>Summary Failed — ${label}</b>\n\n`
      + `Pipeline ran OK (candidates: ${result && result.candidateCount}), `
      + `but summary build/send threw:\n`
      + `<b>${e2.name || "Error"}:</b> ${e2.message}\n`
      + `Heartbeat sent: ${hbOk}`
    );
  }

  // Manual-trigger only: a separate completion message listing selected
  // candidates by name. Gated to the MANUAL label so the in-market crons
  // (where the 50-subrequest budget is tightest) do NOT incur this extra send.
  if (label && label.indexOf("MANUAL") !== -1) {
    try {
      const cands = (result && result.candidates) || [];
      let msg;
      if (cands.length === 0) {
        msg = `✅ <b>Pipeline Complete — ${label}</b>\n`
            + `${result && result.candidateCount ? result.candidateCount : 0} candidates. `
            + `No candidate names available this run (empty or early-exit path).`;
      } else {
        const lines = cands.map(function(c, i) {
          return `${i + 1}. <b>${c.symbol}</b> — DS ${c.discoveryScore} [${c.sector}]`;
        }).join("\n");
        msg = `✅ <b>Pipeline Complete — ${label}</b>\n`
            + `${cands.length} candidate${cands.length === 1 ? "" : "s"} selected:\n\n${lines}`;
      }
      const okSend = await sendTelegram(env, msg);
      // If the completion send fails (e.g. subrequest budget exhausted), surface
      // it explicitly rather than swallowing — past silent failures hid the cause.
      if (!okSend) {
        await sendTelegram(env,
          `⚠️ <b>Completion message failed to send</b> (${label}). `
          + `Likely subrequest budget. Candidates: ${result && result.candidateCount}.`);
      }
    } catch (e3) {
      await sendTelegram(env,
        `⚠️ <b>Completion message threw</b> (${label}): ${e3 && e3.message}`);
    }
  }

  return result; // additive: cron path ignores this; HTTP trigger uses it
}

// ─── sendPipelineSummary ──────────────────────────────────────────────────────
// Sends a structured post-run Telegram summary.
// Bottleneck detection: compares funnel ratios at each stage.
// A bottleneck is flagged when a stage drops > 70% of symbols entering it.
// ─────────────────────────────────────────────────────────────────────────────
async function sendPipelineSummary(env, result, label) {
  // result shape from runFullPipeline():
  // { ok, runId, candidateCount, signalCount, stats }
  // stats: { universeCount, bhavCount, streamAFast, ohlcvQueue, ohlcvCapped,
  //          ohlcvFetched, streamATech, rsPassCount, streamACount,
  //          candidateCount, signalCount, survivorCount, niftyAvailable }

  const now = new Date();
  const ist = new Date(now.getTime() + 5.5 * 60 * 60 * 1000);
  const timeStr = ist.toISOString().slice(11, 16) + " IST";

  // ── Bug A fix: pipeline already messaged these paths — stay silent ─────────
  // runFullPipeline() sends its OWN sendTelegram on every ok:false early exit
  // (no bhav, no Stream A pass, all OHLCV failed) AND on the COMPLETED_EMPTY
  // path (no symbols passed technical filters). Sending again here would
  // produce a duplicate Telegram message on those days — violating the
  // no-duplicate-alerts requirement. This wrapper therefore owns messaging
  // ONLY for the full-success path (result.stats present).
  if (!result || !result.ok) {
    // Pipeline already sent a stage-specific failure/abort message. Do not re-send.
    return;
  }

  // ── Bug B fix: COMPLETED_EMPTY returns { ok:true, candidateCount:0,
  //    signalCount:0 } with NO stats and NO runId. runFullPipeline already
  //    sent "No stocks passed technical filters today" on this path.
  //    Re-sending a thin, blank-runId summary here is a duplicate + degraded
  //    message. Stay silent — pipeline owns this path's messaging.
  const s = result.stats;
  if (!s) {
    return;
  }

  // ── Funnel table ───────────────────────────────────────────────────────────
  const funnel = [
    { name: "Universe",       count: s.universeCount  || 0 },
    { name: "Bhav passed",    count: s.bhavCount      || 0 },
    { name: "Stream A fast",  count: s.streamAFast    || 0 },
    { name: "OHLCV queue",    count: s.ohlcvQueue     || 0 },
    { name: "OHLCV fetched",  count: s.ohlcvFetched   || 0 },
    { name: "Tech filters",   count: s.streamATech    || 0 },
    { name: "RS passed",      count: s.rsPassCount    || 0 },
    { name: "Sector filtered",count: s.streamACount   || 0 },
    { name: "Candidates",     count: s.candidateCount || 0 },
    { name: "Signals sent",   count: s.signalCount    || 0 },
  ];

  // ── Bottleneck detection ───────────────────────────────────────────────────
  // Flag any stage that drops > 70% of the symbols it received.
  // Skip stages where the prior count is 0 (avoid div-by-zero).
  const BOTTLENECK_THRESHOLD = 0.70; // 70% drop = bottleneck
  const bottlenecks = [];

  for (let i = 1; i < funnel.length; i++) {
    const prev = funnel[i - 1].count;
    const curr = funnel[i].count;
    if (prev > 0) {
      const dropRate = (prev - curr) / prev;
      if (dropRate >= BOTTLENECK_THRESHOLD && prev >= 5) {
        // Only flag if the input was meaningful (≥5 symbols) to avoid noise
        // on the final funnel stages where small numbers are expected.
        bottlenecks.push({
          stage: funnel[i].name,
          from:  prev,
          to:    curr,
          pct:   Math.round(dropRate * 100),
        });
      }
    }
  }

  // ── Nifty regime ──────────────────────────────────────────────────────────
  let regimeStr = "—";
  try {
    const sigRaw = await env.KITE_STORE.get("qe_pipe_signals");
    if (sigRaw) {
      const sigs = JSON.parse(sigRaw);
      if (sigs.length > 0 && sigs[0].pipelineRegime) {
        const r = sigs[0].pipelineRegime;
        const icon = r.regime === "bull" ? "🟢" : r.regime === "bear" ? "🔴" : "🟡";
        regimeStr = icon + " " + r.regime.toUpperCase()
          + (r.cmp ? " (Nifty ₹" + r.cmp.toLocaleString("en-IN") + ")" : "");
      }
    }
  } catch (e) { console.warn("[sendPipelineSummary] non-fatal:", e && e.message); }

  // ── Build message ──────────────────────────────────────────────────────────
  const funnelLines = funnel
    .map(function(f) { return `  ${f.name.padEnd(16)}: ${f.count}`; })
    .join("\n");

  let bottleneckBlock = "";
  if (bottlenecks.length > 0) {
    bottleneckBlock = "\n\n⚠️ <b>Bottleneck Detected</b>\n"
      + bottlenecks.map(function(b) {
          return `  🔻 <b>${b.stage}</b>: ${b.from} → ${b.to} (${b.pct}% drop)`;
        }).join("\n");
  }

  const ohlcvCapLine = (s.ohlcvCapped && s.ohlcvCapped > 0)
    ? `\n⚡ OHLCV cap: ${s.ohlcvCapped} symbols dropped (CPU budget)`
    : "";

  const niftyLine = s.niftyAvailable === false
    ? "\n⚠️ Nifty data unavailable — RS ranking may be imprecise"
    : "";

  const statusIcon = s.candidateCount > 0 ? "✅" : "📊";

  const msg = `${statusIcon} <b>Pipeline Complete — ${label}</b>\n`
    + `⏰ ${timeStr}\n`
    + `🔑 Run: <code>${(result.runId || "").slice(-8)}</code>\n`
    + `📶 Regime: ${regimeStr}\n\n`
    + `<b>Funnel</b>\n<code>\n${funnelLines}\n</code>`
    + ohlcvCapLine
    + niftyLine
    + bottleneckBlock
    + (s.candidateCount === 0
        ? "\n\n📭 No candidates today — market filters too tight or broad weakness."
        : "");

  await sendTelegram(env, msg);
}

// ═══════════════════════════════════════════════════════════════════════════════
// COMMIT 2 — SMART MOMENTUM PRE-FILTER (candidate selection only)
//
// PURPOSE: prioritize which Stream-A-Fast survivors enter the expensive S4
// history fetch, under the free-tier subrequest budget. This layer ONLY decides
// selection/ordering. It does NOT touch QuantEdge score, BUY/WAIT/IGNORE,
// position sizing, entry/SL/T1/T2, or Telegram output.
//
// Frozen formula (approved): all inputs from bhav (full /quote) — available
// BEFORE S4. No RS/Sector (those don't exist pre-S4). No volume in the score
// (volume stays a hard FILTER, not a ranker).
// ═══════════════════════════════════════════════════════════════════════════════

const MOMENTUM_W = { m1: 0.25, m2: 0.25, m3: 0.25, m4: 0.10, m5: 0.15 };
const FINALRANK_MOMENTUM_W  = 0.85;
const FINALRANK_FRESHNESS_W = 0.15;
const PIPE_HISTORY_BUDGET   = 36;  // max symbols into S4 per run (free-tier safe; run-1 cold-nifty worst case)

function clamp01(x) { return Math.max(0, Math.min(1, x)); }

// MomentumScore — returns 0..100. Inputs from bhav[sym].
// b = { last_price, prev_close, day_open, day_high, day_low }
function pipeMomentumScore(b) {
  if (!b) return 0;
  const ltp = b.last_price, o = b.day_open, h = b.day_high, l = b.day_low, pc = b.prev_close;
  // Guard: need valid intraday range. If OHLC missing/degenerate, score 0.
  if (!(h > 0) || !(o > 0) || !(pc > 0) || h === l) return 0;

  const m1 = clamp01(ltp / h);                              // proximity to day-high
  const m2 = clamp01(((ltp - o) / o + 0.05) / 0.10);        // intraday momentum, ±5% window
  const m3 = clamp01((ltp - l) / (h - l));                  // position in day-range
  const m4 = clamp01(((h - l) / o) / 0.05);                 // range expansion, cap 5%
  const m5 = clamp01(((ltp - pc) / pc + 0.02) / 0.07);      // day change, -2%..+5% window

  const score = MOMENTUM_W.m1 * m1 + MOMENTUM_W.m2 * m2 + MOMENTUM_W.m3 * m3
              + MOMENTUM_W.m4 * m4 + MOMENTUM_W.m5 * m5;     // 0..1
  return Math.round(score * 1000) / 10;                      // 0..100, 1 decimal
}

// ── Daily dedup + freshness store ───────────────────────────────────────────
// KV key qe_analysed_<YYYYMMDD> holds { sym: priorMomentumScore }.
// TTL 26h → auto-resets each trading day (yesterday's key expires, today's is
// absent → every symbol eligible again). Cannot grow indefinitely: keyed per
// day, expires daily; size bounded by symbols analysed that day (≤ ~110).
function pipeTodayKey() {
  const ist = new Date(Date.now() + 5.5 * 60 * 60 * 1000);
  return "qe_analysed_" + ist.toISOString().slice(0, 10).replace(/-/g, "");
}

async function pipeLoadAnalysedToday(env) {
  try {
    const raw = await env.KITE_STORE.get(pipeTodayKey());
    return raw ? JSON.parse(raw) : {};
  } catch (_) { return {}; }
}

async function pipeSaveAnalysedToday(env, analysedMap) {
  try {
    await env.KITE_STORE.put(pipeTodayKey(), JSON.stringify(analysedMap),
      { expirationTtl: 26 * 60 * 60 }); // 26h → resets next trading day
  } catch (e) { console.warn("[pipeSaveAnalysedToday] non-fatal:", e && e.message); }
}

// FreshnessScore (0..100): rewards symbols NOT analysed yet today, and symbols
// whose momentum is RISING vs their prior-run score.
//   - never analysed today  → freshness 100 (max diversity reward)
//   - analysed, score rising → partial reward by delta
//   - analysed, score flat/falling → low freshness (already had its look)
// Bounded 0..100; cannot grow without limit (delta clamped, base capped).
function pipeFreshnessScore(sym, currentMomentum, analysedMap) {
  if (!(sym in analysedMap)) return 100;          // not yet analysed today
  const prior = analysedMap[sym];
  const delta = currentMomentum - prior;          // points (0..100 scale)
  // Rising fast → up to ~60; flat/falling → ~10 floor. Clamped.
  return clamp01((delta + 5) / 30) * 60 + 10;     // range ~10..70, never ≥ a fresh 100
}

// SINGLE SOURCE OF TRUTH for candidate ranking. Both the production selection
// path (runFullPipeline) and the /pipe/momentum/debug observer call THIS. There
// is no second copy of the scoring/assembly logic — debug cannot drift from
// production because they execute the identical function.
// Returns array sorted by finalRank desc, each: { sym, momentum, freshness, finalRank }.
function pipeRankCandidates(streamAFast, bhav, analysedMap) {
  return streamAFast.map(function(sym) {
    const b         = bhav[sym];
    const mom       = pipeMomentumScore(b);
    const fresh     = pipeFreshnessScore(sym, mom, analysedMap);
    const finalRank = FINALRANK_MOMENTUM_W * mom + FINALRANK_FRESHNESS_W * fresh;
    return { sym: sym, momentum: mom, freshness: fresh, finalRank: finalRank };
  }).sort(function(a, b) { return b.finalRank - a.finalRank; });
}

// ═══════════════════════════════════════════════════════════════════════════════
// FULL PIPELINE ORCHESTRATOR
// Runs all stages in sequence, writes results to KV at each checkpoint.
// ═══════════════════════════════════════════════════════════════════════════════
async function runFullPipeline(env, opts) {
  const skipDedup = !!(opts && opts.skipDedup);
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

  // ── Commit 2: Smart Momentum pre-filter (replaces raw volume-cap) ──────────
  // Volume FILTER already applied in Stream A Fast (≥200k). Here we RANK the
  // survivors by MomentumScore + freshness, exclude symbols already analysed
  // earlier today, and take the top PIPE_HISTORY_BUDGET into S4. This decides
  // SELECTION ONLY — it never touches QuantEdge score or trade logic.
  const analysedToday = await pipeLoadAnalysedToday(env);

  // Score + rank all Stream-A-Fast survivors (shared ranker — see pipeRankCandidates)
  const ranked = pipeRankCandidates(streamAFast, bhav, analysedToday);

  // Exclude symbols already analysed today (dedup) — they keep their prior result.
  // skipDedup (manual trigger): bypass exclusion so an after-hours re-run shows
  // the same true top candidates every time, not a shrinking leftover pool.
  const notYetAnalysed = skipDedup
    ? ranked
    : ranked.filter(function(r) { return !(r.sym in analysedToday); });

  // Per-run history budget. Manual runs use a smaller budget so the extra
  // completion Telegram message fits under the 50-subrequest cap (proven: with
  // 36 history + 13 bhav + 4 telegram = 53 > 50, the last send was dropped).
  // 30 history → 13 bhav + 30 history + 4 telegram = 47/50. Crons keep 36.
  const historyBudget = skipDedup ? 30 : PIPE_HISTORY_BUDGET;

  // Select top N for this run's S4 history fetch
  const selected     = notYetAnalysed.slice(0, historyBudget);
  const ohlcvQueue    = selected.map(function(r) { return r.sym; });
  const ohlcvDropped  = notYetAnalysed.slice(historyBudget).map(function(r) { return r.sym; });

  // Survivorship-log the not-selected (budget-capped) symbols with their score
  for (let di = 0; di < ohlcvDropped.length; di++) {
    const r = notYetAnalysed[historyBudget + di];
    survive.drop(ohlcvDropped[di], "S4_HISTORY_BUDGET",
      "Below history budget cutoff (rank " + (historyBudget + di + 1)
      + ", momentum " + (r ? r.momentum : "n/a") + ")");
  }

  // Commit 2 audit: momentum/rank metrics for every selected symbol
  for (let si = 0; si < selected.length; si++) {
    const r = selected[si];
    audit.log("S3B_MOMENTUM", r.sym, "SELECTED",
      "rank:" + (si + 1) + " momentum:" + r.momentum
      + " freshness:" + r.freshness.toFixed(1) + " final:" + r.finalRank.toFixed(1));
  }
  audit.log("PIPELINE", "", "MOMENTUM_RANK",
    "Ranked " + ranked.length + " | already-analysed-today " + Object.keys(analysedToday).length
    + " | eligible " + notYetAnalysed.length + " | selected " + ohlcvQueue.length
    + " | budget-dropped " + ohlcvDropped.length);

  if (ohlcvQueue.length === 0) {
    // All survivors already analysed today, or none scored — nothing new to fetch.
    await writePipeStatus(env, "COMPLETED_EMPTY", 100, {
      runId: runId, reason: "No new symbols to analyse (all done today or empty)",
    });
    await env.KITE_STORE.put("qe_pipe_audit",        JSON.stringify(audit.getAll().slice(0, 500)));
    await env.KITE_STORE.put("qe_pipe_survivorship", JSON.stringify(survive.getAll().slice(0, 1000)));
    await sendTelegram(env, `📊 <b>Pipeline Complete</b>\nNo new candidates this run — all qualifying symbols already analysed today.`);
    return { ok: true, candidateCount: 0, signalCount: 0 };
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
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

  // ── Stage 5B: Stream B Discovery (independent branch on same ohlcvMap) ───────
  // Runs in parallel intent to Stream A — does NOT consume or alter the Stream A
  // flow (RS/Sector/Merge below). Writes its own KV key for the Discovery panel.
  try {
    const streamBCandidates = pipeStreamBTech(ohlcvMap, audit, survive);
    const sbEvaluated = Object.keys(ohlcvMap).length;
    await env.KITE_STORE.put("qe_pipe_stream_b", JSON.stringify({
      generated_utc: new Date().toISOString(),
      run_id: runId,
      bhav_count: Object.keys(bhav).length,
      ohlcv_map_count: sbEvaluated,
      evaluated_count: sbEvaluated,
      count: streamBCandidates.length,
      candidates: streamBCandidates.slice(0, 50),
    }));
    audit.log("PIPELINE", "", "STREAM_B_KV",
      "Stream B wrote " + streamBCandidates.length + " candidates to qe_pipe_stream_b");
  } catch (e) {
    // Stream B failure must NOT break the Stream A pipeline.
    audit.log("PIPELINE", "", "STREAM_B_ERROR", e.name + ": " + e.message);
    try { await env.KITE_STORE.put("qe_pipe_stream_b", JSON.stringify({
      generated_utc: new Date().toISOString(), run_id: runId, count: 0,
      candidates: [], error: e.message,
    })); } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }
  }

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
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

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
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

  // ── Stage 8: Merge + Discovery Score ────────────────────────────────────────
  await writePipeStatus(env, "S8_MERGE", 78, {
    runId: runId, sectorCount: sectorFiltered.length,
  });
  const candidates = pipeMerge(sectorFiltered, ohlcvMap, audit);

  // Write candidates to KV — this is what the browser reads in Part 3
  try {
    await env.KITE_STORE.put("qe_pipe_candidates", JSON.stringify(candidates));
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

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
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

  // ── Stage 10: Survivorship write ────────────────────────────────────────────
  await writePipeStatus(env, "S10_SURVIVORSHIP", 88, { runId: runId });
  try {
    await env.KITE_STORE.put("qe_pipe_survivorship",
      JSON.stringify(survive.getAll().slice(0, 1000)));
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

  // ── Commit 2: persist dedup set + per-symbol metrics for recalibration ──────
  // Mark every symbol analysed THIS run into today's dedup map, storing its
  // momentum score (used as freshness baseline for later runs today).
  // candidates carry discoveryScore; selected[] carries momentum + rank.
  try {
    const dsBySym = {};
    for (let ci = 0; ci < candidates.length; ci++) {
      dsBySym[candidates[ci].symbol] = candidates[ci];
    }
    // Update dedup map: every selected symbol is now "analysed today".
    // skipDedup (manual): do NOT write — a manual re-run must not consume the
    // crons' daily coverage pool or affect their dedup state.
    if (!skipDedup) {
      for (let si = 0; si < selected.length; si++) {
        analysedToday[selected[si].sym] = selected[si].momentum;
      }
      await pipeSaveAnalysedToday(env, analysedToday);
    }

    // Append per-symbol metrics to a daily metrics log for 2–4 week evaluation.
    // Records: ts, symbol, momentumScore, momentumRank, quantEdgeScore, decision.
    const metricsKey = "qe_metrics_" + (new Date(Date.now() + 5.5*60*60*1000)).toISOString().slice(0,10).replace(/-/g,"");
    let metricsLog = [];
    try {
      const raw = await env.KITE_STORE.get(metricsKey);
      if (raw) metricsLog = JSON.parse(raw);
    } catch (_) {}
    const nowIso = new Date().toISOString();
    for (let si = 0; si < selected.length; si++) {
      const r = selected[si];
      const cand = dsBySym[r.sym];
      let decision = "IGNORE"; // not a candidate after full analysis
      if (cand) {
        const gatePass = (cand.discoveryScore || 0) >= 60 && cand.stBull === true && (cand.adx14 || 0) >= 18;
        decision = gatePass ? "BUY" : "WAIT";
      }
      metricsLog.push({
        ts:        nowIso,
        runId:     runId,
        symbol:    r.sym,
        momentum:  r.momentum,
        momRank:   si + 1,
        qeScore:   cand ? cand.discoveryScore : null,
        decision:  decision,
      });
    }
    await env.KITE_STORE.put(metricsKey, JSON.stringify(metricsLog.slice(-500)),
      { expirationTtl: 35 * 24 * 60 * 60 }); // keep ~5 weeks for recalibration
  } catch (e) { console.warn("[runFullPipeline] metrics persist non-fatal:", e && e.message); }

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
  } catch (e) { console.warn("[runFullPipeline] non-fatal:", e && e.message); }

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
    // For the manual-trigger completion message (names + scores + decision).
    candidates:     candidates.map(function(c) {
      return { symbol: c.symbol, discoveryScore: c.discoveryScore, sector: c.sector };
    }),
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
  } catch (e) { console.warn("[handlePipeTrigger] non-fatal:", e && e.message); }

  // Run the EXACT cron path: runPipelineWithSummary → runFullPipeline →
  // momentum rank → S4 → StreamA → RS → Sector → Merge → Telegram → metrics.
  // This is the identical function the 09:30/12:00/14:30 crons call. No
  // duplicate logic, no alternate path. Synchronous here (manual trigger);
  // crons use ctx.waitUntil for the same function.
  try {
    const result = await runPipelineWithSummary(env, "MANUAL UI trigger");
    if (!result || result.ok === false) {
      return corsErr("Pipeline error: " + ((result && result.error) || "unknown"), 500);
    }
    return cors({
      status:          "success",
      run_id:          result.runId,
      candidate_count: result.candidateCount,
      signal_count:    result.signalCount,
      stats:           result.stats,
      message:         `Pipeline complete. ${result.candidateCount} candidates, ${result.signalCount} signals sent.`,
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

// GET /pipe/streamb/debug — read-only Stream B execution proof (Commit 4.5)
// Pure read of qe_pipe_stream_b. No writes. Does not touch Stream A or Stream B logic.
async function handlePipeStreamBDebug(env) {
  try {
    const raw = await env.KITE_STORE.get("qe_pipe_stream_b");
    if (!raw) {
      return cors({
        status: "success",
        qe_pipe_stream_b_exists: false,
        message: "qe_pipe_stream_b not found — pipeline has not run since Stream B was deployed.",
      });
    }
    const sb = JSON.parse(raw);
    const cands = Array.isArray(sb.candidates) ? sb.candidates : [];
    return cors({
      status: "success",
      qe_pipe_stream_b_exists: true,
      pipeline_run_timestamp: sb.generated_utc || null,
      run_id: sb.run_id || null,
      bhav_universe_count: (sb.bhav_count !== undefined) ? sb.bhav_count : null,
      ohlcv_map_count: (sb.ohlcv_map_count !== undefined) ? sb.ohlcv_map_count : null,
      stream_b_evaluated_count: (sb.evaluated_count !== undefined) ? sb.evaluated_count : null,
      stream_b_candidate_count: (sb.count !== undefined) ? sb.count : cands.length,
      hgs_populated: cands.length > 0 ? cands.every(function(c){ return typeof c.hgs === "number"; }) : null,
      top_10_candidates: cands.slice(0, 10).map(function(c){
        return { symbol: c.symbol, hgs: c.hgs, last: c.last,
                 pctBelow52w: c.pctBelow52w, volRatio: c.volRatio, mtv: c.mtv,
                 rsi: c.rsi, adx: c.adx };
      }),
      error_in_run: sb.error || null,
    });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// GET /pipe/momentum/debug — TEMPORARY (Commit 2 validation)
// Scores ALL Stream-A-Fast survivors with the SAME production MomentumScore +
// freshness logic, returns full distribution + ranked table. Read-only: no
// history fetch, no signal write, no Telegram. Cost = bhav quotes only.
async function handleMomentumDebug(env) {
  try {
    let token;
    try { token = await getToken(env); }
    catch (e) { return corsErr("Kite token missing — login first: " + e.message, 401); }

    const audit   = makePipeAudit();
    const survive = makeSurvivorshipLog();

    const universe = await pipeLoadUniverse(env, audit);
    if (!universe) return corsErr("Universe not in KV — run /universe/refresh", 500);

    const bhav        = await pipeBhavCopy(env, token, universe, audit, survive);
    const bhavSymbols = universe.filter(function(s) { return !!bhav[s]; });
    const streamAFast = pipeStreamAFast(bhav, bhavSymbols, audit, survive);

    const analysedToday = await pipeLoadAnalysedToday(env);

    // SAME ranker as production — single source of truth, cannot drift.
    const rankedRaw = pipeRankCandidates(streamAFast, bhav, analysedToday);

    // Display-only: round for readability + attach rank index. Does NOT alter
    // scoring — operates on the output of the shared production ranker.
    const ranked = rankedRaw.map(function(r, i) {
      return { sym: r.sym, momentum: r.momentum,
               freshness: Math.round(r.freshness * 10) / 10,
               finalRank: Math.round(r.finalRank * 10) / 10,
               rank: i + 1 };
    });

    // Distribution statistics on raw MomentumScore
    const scores = ranked.map(function(r) { return r.momentum; }).slice().sort(function(a, b) { return a - b; });
    const n = scores.length;
    const pct = function(p) {
      if (n === 0) return null;
      const idx = Math.min(n - 1, Math.floor((p / 100) * n));
      return scores[idx];
    };
    const sum  = scores.reduce(function(a, b) { return a + b; }, 0);
    const mean = n ? sum / n : 0;
    const variance = n ? scores.reduce(function(a, b) { return a + (b - mean) * (b - mean); }, 0) / n : 0;
    const median = n ? (n % 2 ? scores[(n - 1) / 2] : (scores[n / 2 - 1] + scores[n / 2]) / 2) : 0;

    // Cutoff analysis at PIPE_HISTORY_BUDGET (raw-only, ignoring dedup, for distribution view)
    const r36 = ranked[PIPE_HISTORY_BUDGET - 1] || null;
    const r37 = ranked[PIPE_HISTORY_BUDGET] || null;
    const cutoffScore = r36 ? r36.finalRank : null;
    const within2 = (cutoffScore !== null)
      ? ranked.filter(function(r) { return Math.abs(r.finalRank - cutoffScore) <= 2; }).length : 0;

    // Raw-momentum-only top N vs finalRank top N (freshness impact)
    const byRaw = ranked.slice().sort(function(a, b) { return b.momentum - a.momentum; });
    const rawTopSet   = new Set(byRaw.slice(0, PIPE_HISTORY_BUDGET).map(function(r) { return r.sym; }));
    const finalTopSet = new Set(ranked.slice(0, PIPE_HISTORY_BUDGET).map(function(r) { return r.sym; }));
    const addedByFresh   = ranked.slice(0, PIPE_HISTORY_BUDGET).filter(function(r) { return !rawTopSet.has(r.sym); }).map(function(r){return r.sym;});
    const removedByFresh = byRaw.slice(0, PIPE_HISTORY_BUDGET).filter(function(r) { return !finalTopSet.has(r.sym); }).map(function(r){return r.sym;});
    const overlap = PIPE_HISTORY_BUDGET - addedByFresh.length;

    const payload = {
      status: "success",
      generated_utc: new Date().toISOString(),
      eligible_count: ranked.length,
      already_analysed_today: Object.keys(analysedToday).length,
      history_budget: PIPE_HISTORY_BUDGET,
      distribution: {
        min: scores[0] || 0, max: scores[n - 1] || 0,
        mean: Math.round(mean * 10) / 10, median: median,
        stddev: Math.round(Math.sqrt(variance) * 10) / 10,
        p50: pct(50), p75: pct(75), p90: pct(90), p95: pct(95), p99: pct(99),
      },
      cutoff: {
        rank36_finalRank: r36 ? r36.finalRank : null,
        rank36_momentum:  r36 ? r36.momentum : null,
        rank37_finalRank: r37 ? r37.finalRank : null,
        rank37_momentum:  r37 ? r37.momentum : null,
        spread_r1_r36:    (ranked[0] && r36) ? Math.round((ranked[0].finalRank - r36.finalRank) * 10) / 10 : null,
        spread_r10_r36:   (ranked[9] && r36) ? Math.round((ranked[9].finalRank - r36.finalRank) * 10) / 10 : null,
        spread_r36_r50:   (r36 && ranked[49]) ? Math.round((r36.finalRank - ranked[49].finalRank) * 10) / 10 : null,
        within_2pts_of_cutoff: within2,
      },
      freshness_impact: {
        overlap_raw_vs_final: overlap,
        overlap_pct: Math.round((overlap / PIPE_HISTORY_BUDGET) * 100),
        added_by_freshness: addedByFresh,
        removed_by_freshness: removedByFresh,
      },
      top50: ranked.slice(0, 50).map(function(r) {
        return { rank: r.rank, symbol: r.sym, momentum: r.momentum,
                 freshness: r.freshness, finalRank: r.finalRank };
      }),
    };

    // Persist for later inspection (TTL 7 days)
    try {
      await env.KITE_STORE.put("qe_momentum_debug", JSON.stringify(payload),
        { expirationTtl: 7 * 24 * 60 * 60 });
    } catch (_) {}

    return cors(payload);
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

    // ── Commit 3: THREE scheduled production runs — all call the SAME
    //    pipeline path (runPipelineWithSummary → runFullPipeline). No duplicate
    //    code paths, no separate ranking. Each run pulls fresh market data and
    //    skips symbols already analysed earlier today (KV dedup).
    // 04:00 UTC Mon–Fri = 09:30 IST — Run 1 (market open)
    if (cron === "0 4 * * 2-6") {
      ctx.waitUntil(runPipelineWithSummary(env, "09:30 IST open scan"));
    }

    // 06:30 UTC Mon–Fri = 12:00 IST — Run 2 (midday)
    if (cron === "30 6 * * 2-6") {
      ctx.waitUntil(runPipelineWithSummary(env, "12:00 IST midday scan"));
    }

    // 09:00 UTC Mon–Fri = 14:30 IST — Run 3 (pre-close)
    if (cron === "0 9 * * 2-6") {
      ctx.waitUntil(runPipelineWithSummary(env, "14:30 IST pre-close scan"));
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

    // GET /pipe/trigger-summary — DIAGNOSTIC: runs the same wrapper the
    // 0 6 / 0 9 crons use, so the heartbeat + summary path can be tested
    // on demand from the phone instead of waiting for the cron. Remove once
    // the re-scan summaries are confirmed working.
    if (path === "/pipe/trigger-summary" && method === "GET") {
      await runPipelineWithSummary(env, "MANUAL diagnostic test");
      return cors({ status: "done", note: "Heartbeat + summary should now be in Telegram." });
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

    // GET /pipe/streamb/debug — read-only Stream B execution proof (Commit 4.5)
    if (path === "/pipe/streamb/debug" && method === "GET") {
      return handlePipeStreamBDebug(env);
    }

    // GET /pipe/momentum/debug — TEMPORARY validation route (Commit 2).
    // Runs bhav → Stream A Fast → scores ALL survivors with MomentumScore +
    // freshness, returns the full distribution + ranked table. Read-only:
    // does NOT fetch history, does NOT write signals, does NOT touch production
    // selection or Telegram. Remove after validation review.
    if (path === "/pipe/momentum/debug" && method === "GET") {
      return handleMomentumDebug(env);
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
