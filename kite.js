/**
 * QuantEdge Cloudflare Worker — kite.js v4.13
 *
 * Changelog v4.13 (11-Jun-2026) — D1 backfill: diagnostics + self-chaining auto-run.
 *   1. DIAGNOSTICS on /d1/backfill: the catch block used to swallow error reasons
 *      (catch(e){fail++}), leaving "40/40 failed" unexplained. Now the response
 *      reports tokens_missing_in_slice, token_map_size, error_breakdown (reason→
 *      count), and sample_errors (first 5 "symbol: reason"). One run now shows WHY
 *      symbols fail instead of guessing.
 *   2. NEW /d1/autobackfill (POST, self-chaining): trigger ONCE; processes batches
 *      back-to-back within an ~18s wall-time budget, then schedules the next leg via
 *      ctx.waitUntil + self-fetch — automatically working through the whole universe
 *      with no manual clicking. Reports progress to Telegram each leg. Bounded:
 *      offset always advances; stops at universe end (no runaway). Idempotent writes
 *      mean a retried/overlapping leg can't corrupt data. Start: POST /d1/autobackfill
 *      (optionally ?offset=N&batch=30).
 *   3. fetch handler signature gains ctx (additive — async fetch(request, env, ctx))
 *      to enable background chaining. Existing routes unaffected.
 *   UNTOUCHED: indicator math, scoring, filters, entry placement, A1 monitor.
 *
 * Changelog v4.12 (11-Jun-2026) — D1 history cache (Option 2): break the Kite
 *   historical rate limit so the full liquid universe is analysable per run.
 *   PROBLEM: Kite throttles historical data ~3 req/sec/key, so one run fetched only
 *     ~60–90 symbols' 365-bar history regardless of budget (proven: queue 60 →
 *     fetched 43). Coverage capped far below the ~620 liquid candidates.
 *   FIX: store daily OHLCV bars in Cloudflare D1; replace the per-run historical
 *     fetch with a cheap daily bulk-quote (NOT rate-limited). Indicators compute
 *     from stored bars. The rate-limited call disappears from the hot path.
 *   ARCHITECTURE (parity-by-construction):
 *     - Indicator math EXTRACTED verbatim into pipeComputeIndicatorsFromCandles().
 *       Live path and D1 path feed identical candles into this ONE function.
 *     - d1ReadCandles windows D1 bars by the SAME calendar-date cutoff the live
 *       path uses (PIPE_OHLCV_RANGE days) — NOT a fixed bar count. CRITICAL: a
 *       fixed-count trim left EMA200 diverging ~0.16–0.44% (EMA is recursive;
 *       different bar counts = different seed chain), enough to flip emaStackBull.
 *       Calendar-cutoff windowing drives divergence to 0.0000000000 (verified).
 *   SAFETY (nothing breaks unflipped):
 *     - Gated behind KV flag USE_D1_CACHE (must be exactly 'true'); default OFF.
 *     - Requires D1 binding env.QE_DB; absent → everything falls back to live fetch.
 *     - d1ReadCandles returns null (→ live fallback) on: too few bars, stale data
 *       (>D1_FRESH_DAYS old), or ANY error. Live-fetch path fully preserved.
 *     - Idempotent writes (ON CONFLICT). Backfill is chunked + rate-limited.
 *   ROUTES (admin): POST /d1/init, /d1/backfill?offset&limit, /d1/update;
 *     GET /d1/status, /d1/verify?symbol=X (Phase-B parity go/no-go).
 *   CRON: daily bar appended on the 16:00 IST cron (independent of the flag, so the
 *     cache stays current while you verify before flipping it on).
 *   UNTOUCHED: scoring/ranking/RS/Stream B/filters/thresholds; entry placement;
 *     A1 stop-loss/monitor. Indicator MATH unchanged (extracted, not rewritten).
 *   GO-LIVE: bind QE_DB → /d1/init → /d1/backfill (repeat) → /d1/verify (must PASS)
 *     → set KV USE_D1_CACHE='true'. Revert instantly by setting it 'false'.
 *
 * Changelog v4.11 (11-Jun-2026) — Recall improvements (zero precision cost):
 *   Two asymmetric fixes — more winning stocks identified, no relaxation of any
 *   quality filter (a stock must still clear every S5/S6 gate to signal):
 *     1. Sector cap PIPE_MAX_SECTOR_N 3 → 5. Momentum winners cluster by sector
 *        (sector rotation), so a cap of 3 silently dropped already-qualified winners
 *        during strong sector moves — the exact regime this strategy trades. Raising
 *        to 5 surfaces MORE stocks that already passed every filter; admits zero
 *        lower-quality stock. Pure recall gain.
 *     2. Dedup on PASS, not on ANALYSIS. Previously every symbol that entered OHLCV
 *        was marked "analysed today" and excluded from later same-day runs — so a
 *        stock that FAILED the morning run but broke out cleanly by afternoon was
 *        never re-examined (the freshest early-confirmed-breakout archetype). Now
 *        only PASSING symbols (candidates) are deduped; a failed symbol stays
 *        eligible for later runs and must still clear every filter to signal.
 *        Verified: candidates[].symbol === selected[].sym (both = the string passed
 *        to pipeFetchOhlcvSymbol, L1952 symbol:symbol), so the gate is exact.
 *   EXPLICITLY UNCHANGED (per plan, until P6 outcome data): ADX threshold, RSI
 *     threshold, S5 filter logic, S6 filter logic, scoring methodology. These are
 *     precision dials and must be tuned with realized T1/SL data, not intuition.
 *   UNTOUCHED: entry placement byte-for-byte unchanged.
 *
 * Changelog v4.10 (11-Jun-2026) — P5: Scan breadth increase (paid plan):
 *   CONTEXT: the 34-symbol-per-run deep-analysis budget was sized purely to fit the
 *     FREE-tier 50-subrequest cap. At ~108 symbols/day it scanned only ~3.7% of the
 *     liquid universe daily — most early breakouts were never seen in their first
 *     1–3 days, defeating the "catch movers early" objective.
 *   CHANGE (paid plan = 1,000 subrequests/invocation, 30s CPU):
 *     • PIPE_HISTORY_BUDGET 34 → 150 (subrequests now ~166/1000, 83% headroom).
 *     • PIPE_MAX_OHLCV_CAP 80 → 150 (must match budget, else silent re-cap).
 *     • PIPE_BATCH_SIZE 5 → 10, PIPE_BATCH_DELAY 300 → 200ms (fit 150 under 30s CPU;
 *       worst-case ≈18s).
 *     • Manual-run budget 30 → 60.
 *   RESULT: daily coverage ~3.7% → ~30% of the liquid universe, momentum-ranked so
 *     it's the right 30%. NO change to scoring, ranking, selection, sizing, entry/
 *     SL/T1/T2, or Telegram output — only the COUNT of symbols flowing through.
 *   NEW BINDING LIMITS (were not binding before): 30s CPU wall-time and Kite's
 *     historical rate limit. Both have graceful handling (CPU margin + one-retry-on-
 *     429 + survivorship log). Watch pipeline timing post-deploy (caveat C-P5a);
 *     if runs near 30s or Kite 429s rise, tune BATCH_SIZE/DELAY or add runs/day.
 *   UNTOUCHED: entry placement byte-for-byte unchanged.
 *
 * Changelog v4.9 (11-Jun-2026) — A4a: API auth scaffolding (safe-by-default):
 *   CONTEXT (audit Phase 6): /signal/store, /tg/register, /pipe/deep-result accept
 *     unauthenticated POSTs; CORS is "*". Anyone with the Worker URL could inject a
 *     signal into the approval queue or overwrite Telegram routing. (HMAC on the
 *     callback is what still prevents a forged signal from placing a GTT.)
 *   A4a (this release) — code/scaffolding that needs NO deployment input:
 *     1. requireApiAuth(request, env): a gate the three open endpoints now call.
 *        SAFE-BY-DEFAULT — if KV key QE_API_SECRET is unset (today), it is a NO-OP
 *        that ALLOWS the request (behaviour identical to before; deploying this
 *        cannot break anything). When QE_API_SECRET is set (A4b) AND the browser
 *        sends "X-QE-Auth: <secret>", it enforces. Constant-time comparison.
 *     2. corsHeadersFor(request, env): origin-allowlist scaffolding. If KV key
 *        QE_ALLOWED_ORIGIN is set (A4b), locks CORS to that origin; else "*".
 *     3. All three endpoints gated with the no-op guard.
 *   A4b (DEFERRED — needs your confirmation, NOT in this release):
 *     • Set QE_API_SECRET in KV + have the browser send X-QE-Auth.
 *     • Set QE_ALLOWED_ORIGIN to the exact Pages origin.
 *     • Remove the hardcoded HMAC_SECRET fallback ("QE_DB_v2_SIGNAL_SECRET") —
 *       RETAINED for now because deleting it before confirming the KV key is set
 *       would break signal verification and halt Telegram BUYs (execution risk).
 *   UNTOUCHED: entry placement (placeGTT, /gtt/create) byte-for-byte unchanged;
 *     no scoring/ranking/RS/discovery changes.
 *
 * Changelog v4.8 (11-Jun-2026) — A1: Exchange-resting stop loss (auto-armed OCO):
 *   PROBLEM (audit CRITICAL-1): every BUY path placed an entry-only single-leg
 *     GTT. After the entry filled, the position sat with NO stop and NO target
 *     resting on the exchange. SL/T1/T2 existed only in qe_gtt_log + the browser
 *     UI — the log claimed protection that did not exist on Kite. One gap-down
 *     could produce a loss many multiples of the modelled 1R.
 *   ROOT CAUSE: placeGTT and the /gtt/create handler both hardcoded
 *     type:"single" with a lone BUY leg; no code ever issued a SELL exit. Kite's
 *     two-leg OCO brackets a POSITION, so the exit can only be placed AFTER the
 *     entry fills — which nothing did.
 *   FIX (broker-validated vs Kite Connect v3 GTT docs):
 *     1. armExitBracket(): on a newly-filled entry, place a two-leg OCO
 *        (stop SELL + target SELL, CNC, LIMIT — GTT legs are LIMIT-only) to
 *        bracket the position. trigger_values ascending [stop, target]. Falls
 *        back to a single stop SELL if the OCO is rejected.
 *     2. Stop leg LIMIT price buffered 0.3% BELOW the trigger (A1_STOP_LIMIT_BUFFER)
 *        to widen fill probability on fast moves; the stop TRIGGER stays at the
 *        user's computed sl, so risk math is unchanged.
 *     3. monitorPositions wired to arm on status=="triggered", idempotent via an
 *        exitArmed flag, with retry of a previously-failed arm (armPending).
 *     4. Loud Telegram alerts: "STOP ARMED" on success, "STOP NOT ARMED — place
 *        manually" on any failure (a fill that fails to arm is a naked position).
 *     5. _retainActive(): armed/pending positions are never dropped by the 200-cap.
 *     6. Monitor cron tightened (30-min -> 5-min cadence, paid plan) to cut the
 *        fill->arm window to ~5 min. NOTE: wrangler.toml [triggers] crons must match.
 *   UNTOUCHED: entry placement (placeGTT, /gtt/create) byte-for-byte unchanged;
 *     no scoring/ranking/RS/discovery changes; no index.html changes.
 *   NOT COMPLETE UNTIL: a test fill produces an OCO visible in Kite's GTT book.
 *
 * Changelog v4.7 (10-Jun-2026) — Signal TTL extension + chatId auth gate:
 *   PROBLEM: Signals sent at 09:30 expired after 15 minutes. GTTs placed via
 *     QuantEdge Telegram buttons in the evening showed "Signal expired" and
 *     were rejected — gtt_log never written — daily summary showed 0 GTTs.
 *   ROOT CAUSE: SIGNAL_TTL_MS = 15 min + no chatId authentication on the
 *     callback handler. The short TTL was compensating for the missing auth.
 *   FIX (Option 4 — chatId gate first, then safe TTL extension):
 *     1. chatId gate added to handleTelegramCallback: reads tg_chat_id from
 *        KV and rejects any callback not from your registered chat. Closes the
 *        real security gap that existed regardless of TTL length.
 *     2. SIGNAL_TTL_MS: 15 min → 8 hours. Signals sent at 09:30 remain
 *        actionable until 17:30. Safe because chatId gate now prevents replay.
 *     3. KV expirationTtl for qe_signal_*: 1800s → 32400s (9 hours, 1hr
 *        buffer beyond the 8-hour action window so key never expires early).
 *
 * Changelog v4.6 (10-Jun-2026) — Subrequest budget fix (signals Telegram):
 *   ROOT CAUSE (proven from audit log run 20666b0615acea9a):
 *     Cloudflare Workers free tier: 50 subrequests/invocation hard cap.
 *     Exact sequence: 1 heartbeat + 12–13 bhav batches (13 when a 403 retry
 *     fires) + 1 Nifty + 36 OHLCV = 50–51. Signals Telegram sendTelegram()
 *     was the 51st call — Cloudflare blocked it → resp never obtained →
 *     catch(e) → return false → signalCount:0 every cron run after 09:30.
 *     Evidence: JUBLFOOD "Too many subrequests" at position 50 in S4_OHLCV,
 *     followed by signalCount:0 in pipe_status. Pattern consistent across
 *     12:00 and 14:30 cron runs on 10-Jun-2026.
 *   FIX: PIPE_HISTORY_BUDGET 36 → 34.
 *     Budget worst case: 1+13+1+34+1 = 50 (exactly within cap).
 *     Budget normal case: 1+12+1+34+1 = 49 (one spare).
 *     Daily coverage: 102 unique symbols/day (was 108). Difference: 6/day.
 *
 * Changelog v4.5 (09-Jun-2026) — KV write reduction + Telegram diagnostics
 *                               + Screener industry extraction:
 *   KV write reduction (free tier: 1000 writes/day):
 *     Removed 10 mid-run writePipeStatus calls (S1–S8, S10, S11).
 *     Removed per-symbol qe_pipe_ohlcv_* cache writes (27/run, never read).
 *     Combined: 36 puts/run → 88 puts/day normal (vs 236 before).
 *   Telegram error logging:
 *     sendTelegram() previously swallowed all errors: catch(_){return false}.
 *     Now logs exact HTTP status + Telegram error body to Cloudflare console.
 *     Enabled diagnosis of the subrequest cap bug above.
 *   Observability — rank snapshot:
 *     Writes qe_pipe_rank_<date>_<runId> after budget cut (post-decision).
 *     Stores {s,r,m,sel,px} for every Stream-A survivor. Zero subrequest cost
 *     (KV put). Enables single-key lookup to trace any symbol's exit stage.
 *   Screener industry extraction:
 *     parseScreenerFundamentals() now extracts industry label from Screener
 *     HTML and returns it in the fundamentals object. Used by index.html v22
 *     Opportunity Radar to replace hardcoded sector map progressively.
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
const SIGNAL_TTL_MS  = 8 * 60 * 60 * 1000; // 8 hours — safe: chatId gate added to callback handler

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

// ═══════════════════════════════════════════════════════════════════════════════
// A4a — API AUTH SCAFFOLDING (safe-by-default; enforcement is opt-in via KV)
//
// Provides a single gate the unauthenticated write endpoints (/signal/store,
// /tg/register, /pipe/deep-result) can call. Design goals:
//   • ZERO deployment dependency to ship: if KV key QE_API_SECRET is NOT set,
//     this is a NO-OP that ALLOWS the request — behaviour is identical to today,
//     so deploying this change cannot break anything (no capital/execution risk).
//   • Enforcement turns on the moment you set QE_API_SECRET in KV (that's the
//     A4b deployment step) AND have the browser send the matching header. Until
//     BOTH are true, requests pass — no lockout risk.
//   • Constant-time comparison to avoid timing leaks.
//
// Header expected once enabled: "X-QE-Auth: <secret>".
// Returns null when allowed, or a Response (401) when blocked.
// ═══════════════════════════════════════════════════════════════════════════════
function _timingSafeEqual(a, b) {
  if (typeof a !== "string" || typeof b !== "string") return false;
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

async function requireApiAuth(request, env) {
  // Opt-in: no secret configured → allow (preserves current open behaviour).
  let secret;
  try { secret = await env.KITE_STORE.get("QE_API_SECRET"); } catch (_) { secret = null; }
  if (!secret) return null; // A4a no-op until A4b sets the secret

  const provided = request.headers.get("X-QE-Auth") || "";
  if (_timingSafeEqual(provided, secret)) return null; // authorized
  return cors({ status: "error", message: "Unauthorized" }, 401);
}

// A4a — CORS origin allowlist scaffolding (safe-by-default).
// If KV key QE_ALLOWED_ORIGIN is set (A4b), echo it back ONLY for matching
// requests; otherwise fall back to "*" (current behaviour). Lets you lock the
// frontend origin without a code change, and without risking a self-lockout
// before you've confirmed the exact Pages origin.
async function corsHeadersFor(request, env) {
  let allowed;
  try { allowed = await env.KITE_STORE.get("QE_ALLOWED_ORIGIN"); } catch (_) { allowed = null; }
  if (!allowed) return CORS; // no-op default "*"
  const origin = request.headers.get("Origin") || "";
  const list = allowed.split(",").map(s => s.trim()).filter(Boolean);
  if (list.includes(origin)) {
    return { ...CORS, "Access-Control-Allow-Origin": origin };
  }
  // Origin not in allowlist: still return a valid CORS object (locked to the
  // first configured origin) so the browser blocks cross-origin reads.
  return { ...CORS, "Access-Control-Allow-Origin": list[0] };
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
    if (!resp.ok) {
      // Log the exact Telegram error so silent failures become diagnosable.
      // Previously: catch (_) { return false; } — swallowed all rejection reasons.
      const errBody = await resp.text().catch(function() { return "(unreadable)"; });
      console.error("[sendTelegram] FAIL HTTP " + resp.status + ": " + errBody.slice(0, 200));
    }
    return resp.ok;
  } catch (e) {
    console.error("[sendTelegram] THROW: " + (e && e.message));
    return false;
  }
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
// A1 — EXIT BRACKET ARMING (auto-armed OCO after entry fill)
// Places a two-leg OCO (stop SELL + target SELL) to bracket a freshly-filled
// position, so the stop rests on the EXCHANGE instead of living only in a log.
// Falls back to a single stop SELL if the two-leg is rejected. Idempotency is
// enforced by the caller via the `exitArmed` flag on the qe_gtt_log record.
//
// Broker-validated against Kite Connect v3 GTT docs (kite.trade/docs/connect/v3/gtt):
//   - type "two-leg" = OCO; requires EXACTLY 2 ascending trigger_values [stop, target]
//   - GTT legs support order_type "LIMIT" ONLY (no SL / SL-M on a GTT) — so the stop
//     is a LIMIT triggered at the stop price. To widen fill probability on a fast
//     move, the stop leg's LIMIT price is buffered slightly BELOW the trigger, while
//     the trigger itself stays at the user's computed stop (risk math unchanged).
//   - all legs CNC, SELL.
// Returns { ok, exitTriggerId, mode } where mode is "oco" | "stop_only" | "fail".
// ═══════════════════════════════════════════════════════════════════════════════
const A1_STOP_LIMIT_BUFFER = 0.003; // 0.3% below trigger for the stop leg's LIMIT floor

async function armExitBracket(env, token, rec, lastPrice) {
  const symbol = String(rec.symbol).toUpperCase();
  const qty    = parseInt(rec.filledQty || rec.quantity, 10);
  const slTrig = rec.sl ? parseFloat(parseFloat(rec.sl).toFixed(2)) : null;   // stop TRIGGER (risk math)
  const t1F    = rec.t1 ? parseFloat(parseFloat(rec.t1).toFixed(2)) : null;   // target
  const ltpF   = parseFloat(parseFloat(lastPrice || rec.entry).toFixed(2));

  // A stop is mandatory. Without it, arm nothing — the caller will alert.
  if (!slTrig || slTrig <= 0 || !qty || qty <= 0) {
    return { ok: false, mode: "fail", reason: "missing stop or quantity" };
  }

  // Stop leg LIMIT floor: slightly below the trigger so a fast tick still crosses
  // it. Trigger fires at slTrig; the order is a LIMIT at slLimit. NSE tick = 0.05.
  const slLimit = parseFloat((Math.round((slTrig * (1 - A1_STOP_LIMIT_BUFFER)) / 0.05) * 0.05).toFixed(2));

  // ── Attempt 1: two-leg OCO (stop + target), trigger_values ASCENDING [stop, target]
  if (t1F && t1F > slTrig) {
    const condition = JSON.stringify({
      exchange: "NSE", tradingsymbol: symbol,
      trigger_values: [slTrig, t1F], last_price: ltpF,
    });
    const orders = JSON.stringify([
      { exchange:"NSE", tradingsymbol:symbol, transaction_type:"SELL",
        quantity:qty, order_type:"LIMIT", product:"CNC", price:slLimit },   // stop leg (index 0 ↔ trigger_values[0])
      { exchange:"NSE", tradingsymbol:symbol, transaction_type:"SELL",
        quantity:qty, order_type:"LIMIT", product:"CNC", price:t1F },        // target leg (index 1 ↔ trigger_values[1])
    ]);
    const { ok, data } = await kiteRequest(
      "POST", "/gtt/triggers", { type:"two-leg", condition, orders }, token
    );
    if (ok && data && data.data && data.data.trigger_id) {
      return { ok:true, exitTriggerId:data.data.trigger_id, mode:"oco" };
    }
    rec._ocoError = (data && data.message) || "two-leg rejected";
  }

  // ── Attempt 2 (fallback): single stop SELL — a resting stop alone still protects.
  const condition2 = JSON.stringify({
    exchange: "NSE", tradingsymbol: symbol,
    trigger_values: [slTrig], last_price: ltpF,
  });
  const orders2 = JSON.stringify([
    { exchange:"NSE", tradingsymbol:symbol, transaction_type:"SELL",
      quantity:qty, order_type:"LIMIT", product:"CNC", price:slLimit },
  ]);
  const r2 = await kiteRequest(
    "POST", "/gtt/triggers", { type:"single", condition:condition2, orders:orders2 }, token
  );
  if (r2.ok && r2.data && r2.data.data && r2.data.data.trigger_id) {
    return { ok:true, exitTriggerId:r2.data.data.trigger_id, mode:"stop_only" };
  }
  return { ok:false, mode:"fail",
           reason:(r2.data && r2.data.message) || rec._ocoError || "exit placement failed" };
}

// A1: keep the 200 most-recent records BUT never drop a position that filled and
// is still being protected (armed / arm-pending / exit not yet closed) — losing
// it would orphan a live stop from our tracking.
function _retainActive(log) {
  const recent = log.slice(0, 200);
  const seen   = new Set(recent.map(r => r.trigger_id));
  const active = log.filter(r =>
    (r.alerted && !r.exitArmed) || r.armPending || (r.exitArmed && !r.exitClosed)
  );
  for (const r of active) if (!seen.has(r.trigger_id)) recent.push(r);
  return recent;
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

  // ── Authorisation gate ────────────────────────────────────────────────────
  // Only the registered Telegram chat (your account) may trigger BUY/WATCH.
  // Without this check, anyone who sees the Telegram message buttons could
  // place a GTT on your Zerodha account. chatId is read from KV tg_chat_id
  // (same key used by getTgCreds) — set once at /login.
  const authorisedChat = await env.KITE_STORE.get("tg_chat_id");
  if (authorisedChat && String(chatId) !== String(authorisedChat)) {
    await answerCallback(env, callbackQueryId, "❌ Unauthorised.");
    return cors({ ok: true });
  }
  // ── End authorisation gate ────────────────────────────────────────────────

  let slim;
  try { slim = JSON.parse(cq.data); } catch (_) {
    await answerCallback(env, callbackQueryId, "Invalid signal data.");
    return cors({ ok: true });
  }

  // Slim button payload: { a: action, s: signalId }. Full trade details are
  // read back from KV (qe_signal_<signalId>) — the button can't carry them
  // because Telegram limits callback_data to 64 bytes.
  const action   = slim.a;
  const signalId = slim.s;
  if (!signalId || !action) {
    await answerCallback(env, callbackQueryId, "Invalid signal reference.");
    return cors({ ok: true });
  }

  let sig;
  try {
    const sigRaw = await env.KITE_STORE.get("qe_signal_" + signalId);
    sig = sigRaw ? JSON.parse(sigRaw) : null;
  } catch (_) { sig = null; }
  if (!sig) {
    await answerCallback(env, callbackQueryId, "⏱ Signal expired or not found. Run a new scan.");
    if (chatId && messageId) {
      await editTgMessage(env, chatId, messageId,
        "⏱ <b>Signal Expired</b>\nRun a fresh Discovery scan for new signals.");
    }
    return cors({ ok: true });
  }

  const { symbol, entry, sl, t1, t2, qty, cmp, expiry, hmac } = sig;

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
  // A1: best-effort reference LTP for exit-bracket last_price. armExitBracket
  // falls back to rec.entry if null. The stop TRIGGER comes from rec.sl, not
  // this — so a stale LTP never affects the protective price.
  statusById._ltp = (allKiteGTTs[0] && allKiteGTTs[0].condition && allKiteGTTs[0].condition.last_price) || null;

  const raw    = await env.KITE_STORE.get("qe_gtt_log");
  const ourLog = raw ? JSON.parse(raw) : [];

  const alerts  = [];
  const armExitAlerts = [];   // A1: exit-arming notifications (arm success / fail / retry)

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
    // ── A1: auto-arm exit bracket on a newly-filled entry ──────────────────────
    // Only our own BUY entries (SELL records are exit legs — never re-bracket),
    // only once (exitArmed flag), only when the entry actually triggered. This is
    // where the stop comes to rest on the exchange.
    if (triggered && !logged.exitArmed && logged.transaction !== "SELL") {
      logged.filledQty = logged.filledQty || logged.quantity;
      const armed = await armExitBracket(env, token, logged, statusById._ltp);
      if (armed.ok) {
        logged.exitArmed     = true;
        logged.exitTriggerId = armed.exitTriggerId;
        logged.exitMode      = armed.mode;
        logged.exitArmedAt   = new Date().toISOString();
        logged.armPending    = false;
        armExitAlerts.push({ rec: logged, mode: armed.mode });
      } else {
        logged.armPending    = true;   // retried on next monitor run
        armExitAlerts.push({ rec: logged, mode: "fail", reason: armed.reason });
      }
    }
    // ── A1: retry a previously-failed arm (entry filled earlier, arm had failed) ─
    else if (logged.armPending && !logged.exitArmed && logged.transaction !== "SELL") {
      logged.filledQty = logged.filledQty || logged.quantity;
      const armed = await armExitBracket(env, token, logged, statusById._ltp);
      if (armed.ok) {
        logged.exitArmed     = true;
        logged.exitTriggerId = armed.exitTriggerId;
        logged.exitMode      = armed.mode;
        logged.exitArmedAt   = new Date().toISOString();
        logged.armPending    = false;
        armExitAlerts.push({ rec: logged, mode: armed.mode, retried: true });
      }
    }
    const age = (Date.now() - new Date(logged.timestamp).getTime()) / (1000 * 60 * 60 * 24);
    // Stale check only applies to GTTs that are STILL active (not filled/gone).
    if (kiteStatus === "active" && age > 25 && !logged.staleAlerted) {
      alerts.push(Object.assign({}, logged, { stale: true }));
      logged.staleAlerted = true;
    }
  }

  // A1: persist if EITHER fill/stale alerts OR exit-arming changed the log.
  // Without the armExitAlerts condition, exitArmed/armPending flags would be lost
  // when there are no other alerts, causing the position to be re-armed next run.
  // _retainActive replaces the bare slice(0,200) so an armed-but-unclosed trade is
  // never dropped (which would orphan a live exchange stop from our tracking).
  if (alerts.length || armExitAlerts.length) {
    await env.KITE_STORE.put("qe_gtt_log", JSON.stringify(_retainActive(ourLog)));
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
    // ── A1: exit-arming notifications ────────────────────────────────────────
    // A detected fill that fails to arm = a NAKED position. It must scream.
    for (let m = 0; m < armExitAlerts.length; m++) {
      const e = armExitAlerts[m];
      if (e.mode === "fail") {
        await sendTelegram(env,
          `⚠️ <b>STOP NOT ARMED — ${e.rec.symbol}</b>\n\n`
          + `Entry filled but the exchange stop could NOT be placed.\n`
          + `Reason: ${e.reason || "unknown"}\n`
          + `Qty: ${e.rec.filledQty} | Intended SL: ₹${e.rec.sl}\n\n`
          + `❗ PLACE A STOP MANUALLY IN KITE NOW.\n`
          + `<a href="https://kite.zerodha.com/gtt">Open Kite GTT →</a>`
        );
      } else {
        await sendTelegram(env,
          `🛡️ <b>STOP ARMED — ${e.rec.symbol}</b>${e.retried ? " (retry)" : ""}\n\n`
          + `${e.mode === "oco" ? "OCO bracket" : "Stop"} now resting on the exchange.\n`
          + `SL: ₹${e.rec.sl}${e.mode === "oco" ? ` | T1: ₹${e.rec.t1}` : ""} | Qty: ${e.rec.filledQty}\n`
          + `Exit Trigger ID: <code>${e.rec.exitTriggerId}</code>\n`
          + `<a href="https://kite.zerodha.com/gtt">Verify in Kite →</a>`
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
  // Rank-list snapshots are dynamically named (qe_pipe_rank_<date>_<runId>),
  // so they can't be enumerated in the static allowlist — permit by prefix.
  // Read-only observability data; same exposure class as qe_pipe_survivorship.
  const isRankKey = key.indexOf("qe_pipe_rank_") === 0;
  if (!allowed.includes(key) && !isRankKey) return corsErr("Key not readable", 403);
  try {
    const value = await env.KITE_STORE.get(key);
    return cors({ key, value: value || null });
  } catch (e) {
    return corsErr(e.message, 500);
  }
}

// ─── /tg/register  (store TG credentials from QuantEdge UI) ──────────────────
async function handleTgRegister(request, env) {
  const authErr = await requireApiAuth(request, env); if (authErr) return authErr; // A4a
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
  const authErr = await requireApiAuth(request, env); if (authErr) return authErr; // A4a
  let body;
  try { body = await request.json(); } catch (_) { return corsErr("Invalid JSON"); }
  const { signalId, symbol, entry, sl, t1, t2, qty, cmp, expiry, hmac } = body;
  if (!signalId || !symbol || !expiry) return corsErr("Required: signalId, symbol, expiry");
  if (Date.now() > expiry) return corsErr("Signal already expired", 400);
  await env.KITE_STORE.put(
    `qe_signal_${signalId}`,
    JSON.stringify({ signalId, symbol, entry, sl, t1, t2, qty, cmp, expiry, hmac }),
    { expirationTtl: 32400 }
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
const PIPE_BATCH_SIZE    = 10;    // symbols per OHLCV batch (P5: 5→10 so 150-symbol budget fits under 30s CPU)
const PIPE_BATCH_DELAY   = 200;   // ms between OHLCV batches (P5: 300→200; 15 batches × 200ms ≈ 3s delay)
const PIPE_MIN_CANDLES   = 220;   // min daily bars (EMA200 needs 200; +20 margin for reliable seed)
const PIPE_RS_THRESHOLD  = 55;    // RS percentile cutoff for Stream A pass
const PIPE_MAX_SECTOR_N  = 5;     // max candidates per sector in final output.
                                 // Recall fix (11-Jun): 3→5. Momentum winners cluster by sector
                                 // (sector rotation), so a cap of 3 silently dropped already-qualified
                                 // winners during strong sector moves — exactly the regime this strategy
                                 // trades. Raising to 5 admits MORE stocks that already passed every
                                 // quality filter; it does not admit any lower-quality stock (pure recall
                                 // gain, zero precision cost). S5/S6/scoring unchanged.
const PIPE_TOP_N         = 20;    // max candidates to write to KV for browser
const PIPE_SIGNAL_TOP    = 5;     // max signals dispatched via Telegram per run
// Critical Fix 1: 365 calendar days ≈ 260 trading days — sufficient for reliable EMA200
const PIPE_OHLCV_RANGE   = 365;   // days of history to fetch (1 year, EMA200-safe)

// ═══════════════════════════════════════════════════════════════════════════════
// D1 HISTORY CACHE (Option 2) — constants. See full design in QuantEdge_D1_Design.md
// Stores daily OHLCV bars in Cloudflare D1 so the rate-limited 365-bar historical
// fetch is replaced by a single cheap bulk-quote per day. Flag-gated; OFF until KV
// key USE_D1_CACHE === 'true'. With flag off / QE_DB unbound, the pipeline runs
// EXACTLY as before (live fetch). Nothing breaks unflipped.
// ═══════════════════════════════════════════════════════════════════════════════
const D1_BARS_STORED     = 400;   // bars to keep per symbol (≥220 needed; 400 = headroom)
const D1_BACKFILL_LIMIT  = 150;   // symbols per backfill chunk (rate-limited; click to continue)
const D1_FRESH_DAYS      = 6;     // D1 data must have a bar within N days, else fall back to live
const D1_BULK_QUOTE_SIZE = 200;   // symbols per bulk-quote call in daily update (Kite allows ~500)
const PIPE_NIFTY_TTL_MS  = 4 * 60 * 60 * 1000; // nifty cache valid 4h
// Critical Fix 2 (P5-revised): Cap OHLCV processing.
// Paid plan = 1,000 subrequests/invocation (not 50) and 30s CPU. Per-symbol cost
// is now 1 subrequest (token-map removed the per-symbol /quote). The binding
// limits at scale are CPU wall-time and Kite's historical rate limit, NOT
// subrequests. Cap raised 80→150 to match PIPE_HISTORY_BUDGET; if this were left
// below the history budget it would silently re-cap before the budget applied.
// Sorted by volume desc so highest-liquidity stocks process first. Symbols beyond
// cap are survivorship-logged as OHLCV_CAP_EXCEEDED.
const PIPE_MAX_OHLCV_CAP = 150;   // max symbols entering OHLCV fetch (P5: 80→150, matches history budget)
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

  // D1 refactor: indicator computation extracted into a shared function so the
  // live-fetch path and the D1-cache path feed IDENTICAL candles into IDENTICAL
  // math — guaranteeing byte-identical output by construction (Phase B verifies).
  return pipeComputeIndicatorsFromCandles(symbol, candles);
}

// ─── Shared indicator computation (used by BOTH live fetch and D1 cache) ───────
// candles: array of [timestamp, open, high, low, close, volume]. This is the
// SINGLE source of indicator math. Do not duplicate this logic anywhere.
function pipeComputeIndicatorsFromCandles(symbol, candles) {
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

// ═══════════════════════════════════════════════════════════════════════════════
// D1 HISTORY CACHE LAYER (Option 2)
// All functions defensively no-op / fall back to live fetch if QE_DB is unbound.
// ═══════════════════════════════════════════════════════════════════════════════

// Is the D1 cache enabled? Requires BOTH the binding present AND the KV flag 'true'.
async function d1Enabled(env) {
  if (!env.QE_DB) return false;
  try {
    const flag = await env.KITE_STORE.get("USE_D1_CACHE");
    return flag === "true";
  } catch (_) { return false; }
}

// Read a symbol's candles from D1 in the [ts,o,h,l,c,v] shape the math expects.
// Returns null if absent, too few bars, or stale (latest bar older than D1_FRESH_DAYS).
async function d1ReadCandles(env, symbol) {
  if (!env.QE_DB) return null;
  try {
    const rs = await env.QE_DB
      .prepare("SELECT bar_date,o,h,l,c,v FROM ohlcv_daily WHERE symbol=?1 ORDER BY bar_date ASC")
      .bind(symbol.toUpperCase())
      .all();
    const rows = (rs && rs.results) || [];
    if (rows.length < PIPE_MIN_CANDLES) return null;

    // Freshness: latest stored bar must be recent, else the symbol fell out of
    // daily updates and we must not compute on stale data — fall back to live.
    const latest = rows[rows.length - 1].bar_date;             // 'YYYY-MM-DD'
    const latestMs = new Date(latest + "T00:00:00Z").getTime();
    if (Date.now() - latestMs > D1_FRESH_DAYS * 86400000) return null;

    // Build [timestamp, o, h, l, c, v]. Timestamp is synthetic (unused by the math).
    // PARITY-CRITICAL: the live path fetches by CALENDAR DATE (from = now −
    // PIPE_OHLCV_RANGE days). EMA is recursive, so to be byte-identical the D1 path
    // must compute on the SAME bar set — i.e. the same calendar-date cutoff, NOT a
    // fixed bar count (holiday counts vary). We store 400 bars for headroom but only
    // feed bars on/after the live cutoff into the math. (Verified: matching the bar
    // set drives EMA200 divergence to 0.00%; a fixed-count trim left ~0.16% residual.)
    const cutoffMs  = Date.now() - PIPE_OHLCV_RANGE * 86400000;
    const cutoffStr = new Date(cutoffMs).toISOString().slice(0, 10); // 'YYYY-MM-DD'
    const windowed  = rows.filter(function(r) { return r.bar_date >= cutoffStr; });
    // Guard: if the windowed set is too short (gappy history), use what we have but
    // never fewer than PIPE_MIN_CANDLES — fall back to live if even that fails.
    const use = (windowed.length >= PIPE_MIN_CANDLES) ? windowed : rows.slice(-PIPE_MIN_CANDLES);
    if (use.length < PIPE_MIN_CANDLES) return null;
    return use.map(function(r) {
      return [ new Date(r.bar_date + "T00:00:00Z").getTime() / 1000,
               r.o, r.h, r.l, r.c, r.v ];
    });
  } catch (e) {
    console.warn("[d1ReadCandles] " + symbol + ": " + (e && e.message));
    return null; // any D1 error → fall back to live
  }
}

// Fetch a symbol's full history from Kite (the rate-limited call) — used by backfill.
async function d1FetchHistory(env, token, symbol, instrToken) {
  if (!instrToken || instrToken <= 0) throw new Error("No token for " + symbol);
  const now      = new Date();
  const fromDate = new Date(now.getTime() - (D1_BARS_STORED + 30) * 86400000 * 1.5);
  const fromStr  = fromDate.toISOString().slice(0, 10);
  const toStr    = now.toISOString().slice(0, 10);
  const ctrl  = new AbortController();
  const timer = setTimeout(function() { ctrl.abort(); }, PIPE_SYMBOL_TIMEOUT_MS);
  let res;
  try {
    res = await fetch(
      `${KITE_API_BASE}/instruments/historical/${instrToken}/day?from=${fromStr}&to=${toStr}`,
      { headers: { "X-Kite-Version": "3", "Authorization": kiteAuthHeader(token) }, signal: ctrl.signal }
    );
  } finally { clearTimeout(timer); }
  if (!res.ok) throw new Error("Historical HTTP " + res.status);
  const data = await res.json();
  return (data && data.data && data.data.candles) || [];
}

// Write candles for one symbol into D1 (idempotent via PK). Keeps last D1_BARS_STORED.
async function d1WriteCandles(env, symbol, candles) {
  if (!env.QE_DB || !candles || !candles.length) return 0;
  const sym  = symbol.toUpperCase();
  const keep = candles.slice(-D1_BARS_STORED);
  const stmts = keep.map(function(c) {
    const barDate = new Date(c[0] * 1000).toISOString().slice(0, 10);
    return env.QE_DB.prepare(
      "INSERT INTO ohlcv_daily (symbol,bar_date,o,h,l,c,v) VALUES (?1,?2,?3,?4,?5,?6,?7) " +
      "ON CONFLICT(symbol,bar_date) DO UPDATE SET o=?3,h=?4,l=?5,c=?6,v=?7"
    ).bind(sym, barDate, c[1], c[2], c[3], c[4], c[5]);
  });
  await env.QE_DB.batch(stmts);
  return keep.length;
}

// Route: POST /d1/init — create the table (run once).
async function handleD1Init(env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound. Bind a D1 database named quantedge_history.", 400);
  try {
    await env.QE_DB.exec(
      "CREATE TABLE IF NOT EXISTS ohlcv_daily (symbol TEXT NOT NULL, bar_date TEXT NOT NULL, o REAL, h REAL, l REAL, c REAL, v INTEGER, PRIMARY KEY (symbol, bar_date))"
    );
    await env.QE_DB.exec(
      "CREATE INDEX IF NOT EXISTS idx_symbol_date ON ohlcv_daily(symbol, bar_date)"
    );
    return cors({ status: "success", message: "D1 table ohlcv_daily ready" });
  } catch (e) { return corsErr("D1 init failed: " + (e && e.message), 500); }
}

// Route: GET /d1/status — coverage snapshot.
async function handleD1Status(env) {
  if (!env.QE_DB) return cors({ status: "success", d1_bound: false, enabled: false });
  try {
    const flag = await env.KITE_STORE.get("USE_D1_CACHE");
    const cnt  = await env.QE_DB.prepare("SELECT COUNT(*) AS n FROM ohlcv_daily").first();
    const syms = await env.QE_DB.prepare("SELECT COUNT(DISTINCT symbol) AS n FROM ohlcv_daily").first();
    const late = await env.QE_DB.prepare("SELECT MAX(bar_date) AS d FROM ohlcv_daily").first();
    return cors({
      status: "success", d1_bound: true, enabled: flag === "true",
      total_rows: (cnt && cnt.n) || 0,
      symbols: (syms && syms.n) || 0,
      latest_bar: (late && late.d) || null,
    });
  } catch (e) { return corsErr("D1 status failed: " + (e && e.message), 500); }
}

// Route: POST /d1/backfill?offset=N&limit=L — chunked history populate.
// Returns next_offset; click until done==true. Rate-limited (uses historical fetch).
async function handleD1Backfill(request, env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound", 400);
  const token = await getToken(env);
  if (!token) return corsErr("No Kite token — log in first", 401);

  const url    = new URL(request.url);
  const offset = parseInt(url.searchParams.get("offset") || "0", 10);
  const limit  = parseInt(url.searchParams.get("limit") || String(D1_BACKFILL_LIMIT), 10);

  // Liquid universe + token map from KV (same source the pipeline uses).
  let universe = [], tokenMap = {};
  try {
    const u = await env.KITE_STORE.get("qe_db_universe");
    if (u) universe = JSON.parse(u);
    const t = await env.KITE_STORE.get("qe_db_token_map");
    if (t) tokenMap = JSON.parse(t);
  } catch (e) { return corsErr("Universe/token-map read failed: " + e.message, 500); }
  if (!universe.length) return corsErr("Empty universe — run /universe/refresh first", 400);

  const slice = universe.slice(offset, offset + limit);
  let ok = 0, fail = 0, bars = 0;
  const errors = {};          // reason → count
  const sampleErrors = [];    // first few "symbol: reason" for diagnosis
  let noTokenCount = 0;
  for (let i = 0; i < slice.length; i++) {
    const sym = slice[i];
    if (!tokenMap[sym]) noTokenCount++;   // diagnostic: is the token even present?
    try {
      const candles = await d1FetchHistory(env, token, sym, tokenMap[sym]);
      if (candles.length) { bars += await d1WriteCandles(env, sym, candles); ok++; }
      else {
        fail++;
        errors["empty_candles"] = (errors["empty_candles"] || 0) + 1;
        if (sampleErrors.length < 5) sampleErrors.push(sym + ": returned 0 candles");
      }
    } catch (e) {
      fail++;
      const msg = (e && e.message) || "unknown";
      // bucket the reason (strip symbol-specific text)
      const key = msg.replace(/for [A-Z0-9&-]+/i, "for <sym>");
      errors[key] = (errors[key] || 0) + 1;
      if (sampleErrors.length < 5) sampleErrors.push(sym + ": " + msg);
    }
  }
  const nextOffset = offset + limit;
  const done = nextOffset >= universe.length;
  return cors({
    status: "success",
    processed: slice.length, ok: ok, failed: fail, bars_written: bars,
    offset: offset, next_offset: done ? null : nextOffset,
    universe_size: universe.length, done: done,
    // ── DIAGNOSTICS (added to find why symbols fail) ──
    tokens_missing_in_slice: noTokenCount,
    token_map_size: Object.keys(tokenMap).length,
    error_breakdown: errors,
    sample_errors: sampleErrors,
    hint: done ? "Backfill complete. Run /d1/verify next." :
                 "Call again with offset=" + nextOffset,
  });
}

// One batch of backfill, returning structured progress (shared by auto-backfill).
async function d1BackfillBatch(env, token, universe, tokenMap, offset, limit) {
  const slice = universe.slice(offset, offset + limit);
  let ok = 0, fail = 0, bars = 0, noTok = 0;
  const sampleErrors = [];
  for (let i = 0; i < slice.length; i++) {
    const sym = slice[i];
    if (!tokenMap[sym]) noTok++;
    try {
      const candles = await d1FetchHistory(env, token, sym, tokenMap[sym]);
      if (candles.length) { bars += await d1WriteCandles(env, sym, candles); ok++; }
      else { fail++; if (sampleErrors.length < 3) sampleErrors.push(sym + ": 0 candles"); }
    } catch (e) {
      fail++;
      if (sampleErrors.length < 3) sampleErrors.push(sym + ": " + ((e && e.message) || "err"));
    }
  }
  return { ok, fail, bars, noTok, processed: slice.length, sampleErrors };
}

// Route: POST /d1/autobackfill — SELF-CHAINING. Triggers once; processes batches
// back-to-back within one request up to a wall-time budget, then schedules itself to
// continue from the next offset via ctx.waitUntil + self-fetch. Reports to Telegram.
// No manual clicking. Pass ?offset=0 to start (or omit). Safe: idempotent writes,
// resumes exactly where it stopped, stops at universe end.
async function handleD1AutoBackfill(request, env, ctx) {
  if (!env.QE_DB) return corsErr("QE_DB not bound", 400);
  const token = await getToken(env);
  if (!token) return corsErr("No Kite token — log in first", 401);

  const url     = new URL(request.url);
  let   offset  = parseInt(url.searchParams.get("offset") || "0", 10);
  const batch   = parseInt(url.searchParams.get("batch")  || "30", 10);   // per inner batch
  const budgetMs = 18000;  // ~18s of work per request, then chain (paid CPU headroom)

  let universe = [], tokenMap = {};
  try {
    const u = await env.KITE_STORE.get("qe_db_universe");
    if (u) universe = JSON.parse(u);
    const t = await env.KITE_STORE.get("qe_db_token_map");
    if (t) tokenMap = JSON.parse(t);
  } catch (e) { return corsErr("Universe/token-map read failed: " + e.message, 500); }
  if (!universe.length) return corsErr("Empty universe", 400);

  const start = Date.now();
  let totOk = 0, totFail = 0, totBars = 0, lastSample = [];
  // Process batches until we run low on time budget or finish the universe.
  while (offset < universe.length && (Date.now() - start) < budgetMs) {
    const r = await d1BackfillBatch(env, token, universe, tokenMap, offset, batch);
    totOk += r.ok; totFail += r.fail; totBars += r.bars; lastSample = r.sampleErrors;
    offset += batch;
  }

  const done = offset >= universe.length;
  const base = url.origin;

  // Telegram progress ping (best-effort; non-blocking)
  const pct = Math.min(100, Math.round((offset / universe.length) * 100));
  const msg = done
    ? `✅ D1 backfill COMPLETE\nLoaded ~${offset}/${universe.length} symbols. Run /d1/verify?symbol=RELIANCE to confirm parity, then set USE_D1_CACHE='true'.`
    : `⏳ D1 backfill ${pct}%\nProcessed up to ${offset}/${universe.length}. ok+${totOk} fail+${totFail} this leg. Continuing automatically…`;
  ctx.waitUntil(sendTelegram(env, msg).catch(function(){}));

  // Chain: fire-and-forget the next leg so it runs after this response returns.
  if (!done) {
    const nextUrl = base + "/d1/autobackfill?offset=" + offset + "&batch=" + batch;
    ctx.waitUntil(
      fetch(nextUrl, { method: "POST" }).catch(function(e){
        console.warn("[autobackfill] chain failed:", e && e.message);
      })
    );
  }

  return cors({
    status: "success",
    leg_processed_to: offset, universe_size: universe.length,
    leg_ok: totOk, leg_failed: totFail, leg_bars: totBars,
    percent: pct, done: done,
    sample_errors: lastSample,
    note: done ? "Backfill complete." :
          "Auto-continuing in background. Watch Telegram; check /d1/status for total_rows climbing.",
  });
}

// Route: POST /d1/update — daily incremental. Appends today's bar via bulk-quote.
// Wired to the 16:00 IST cron. Cheap: ~bulk calls, NOT the historical rate limit.
async function handleD1Update(env) {
  if (!env.QE_DB) return { ok: false, msg: "QE_DB not bound" };
  const token = await getToken(env);
  if (!token) return { ok: false, msg: "No token" };

  let universe = [];
  try {
    const u = await env.KITE_STORE.get("qe_db_universe");
    if (u) universe = JSON.parse(u);
  } catch (_) {}
  if (!universe.length) return { ok: false, msg: "Empty universe" };

  const today = new Date().toISOString().slice(0, 10);
  let written = 0, batches = 0;

  for (let i = 0; i < universe.length; i += D1_BULK_QUOTE_SIZE) {
    const batch = universe.slice(i, i + D1_BULK_QUOTE_SIZE);
    const istr  = batch.map(function(s) { return "i=NSE:" + encodeURIComponent(s); }).join("&");
    let qres;
    try {
      const r = await fetch(`${KITE_API_BASE}/quote?${istr}`,
        { headers: { "X-Kite-Version": "3", "Authorization": kiteAuthHeader(token) } });
      if (!r.ok) continue;
      qres = await r.json();
    } catch (_) { continue; }
    batches++;

    const data = (qres && qres.data) || {};
    const stmts = [];
    for (const key in data) {
      const q = data[key];
      const sym = key.replace(/^NSE:/, "").toUpperCase();
      const o = q.ohlc && q.ohlc.open, h = q.ohlc && q.ohlc.high,
            l = q.ohlc && q.ohlc.low,  c = q.last_price, v = q.volume;
      if (o == null || h == null || l == null || c == null) continue;
      stmts.push(env.QE_DB.prepare(
        "INSERT INTO ohlcv_daily (symbol,bar_date,o,h,l,c,v) VALUES (?1,?2,?3,?4,?5,?6,?7) " +
        "ON CONFLICT(symbol,bar_date) DO UPDATE SET o=?3,h=?4,l=?5,c=?6,v=?7"
      ).bind(sym, today, o, h, l, c, v || 0));
    }
    if (stmts.length) { await env.QE_DB.batch(stmts); written += stmts.length; }
  }

  // Optional: trim each symbol to D1_BARS_STORED would require per-symbol deletes;
  // skipped here (storage is ample). A weekly re-backfill keeps data adjusted (C-D1b).
  return { ok: true, written: written, batches: batches, date: today };
}

// Route: GET /d1/verify?symbol=X — PARITY CHECK (Phase B go/no-go).
// Computes indicators from D1 AND from a fresh live fetch, returns both + diffs.
async function handleD1Verify(request, env) {
  if (!env.QE_DB) return corsErr("QE_DB not bound", 400);
  const token = await getToken(env);
  if (!token) return corsErr("No Kite token — log in first", 401);

  const url = new URL(request.url);
  const sym = (url.searchParams.get("symbol") || "").toUpperCase();
  if (!sym) return corsErr("Provide ?symbol=SYMBOL", 400);

  let tokenMap = {};
  try { const t = await env.KITE_STORE.get("qe_db_token_map"); if (t) tokenMap = JSON.parse(t); } catch (_) {}

  // Live path
  let live = null, liveErr = null;
  try { live = await pipeFetchOhlcvSymbol(env, token, sym, tokenMap[sym]); }
  catch (e) { liveErr = e.message; }

  // D1 path (uses the SAME shared math via pipeComputeIndicatorsFromCandles)
  let d1 = null, d1Err = null;
  try {
    const candles = await d1ReadCandles(env, sym);
    if (!candles) d1Err = "No D1 candles (absent/too few/stale)";
    else d1 = pipeComputeIndicatorsFromCandles(sym, candles);
  } catch (e) { d1Err = e.message; }

  // Diff the key indicators
  const fields = ["ema20","ema50","ema200","rsi14","atr14","adx14","stBull",
                  "volRatio","prox52w","emaStackBull","lastClose","candleCount"];
  const diffs = {};
  let maxRelDiff = 0;
  if (live && d1) {
    for (const f of fields) {
      const a = live[f], b = d1[f];
      if (typeof a === "number" && typeof b === "number") {
        const rel = Math.abs(a) > 1e-9 ? Math.abs(a - b) / Math.abs(a) : Math.abs(a - b);
        diffs[f] = { live: a, d1: b, relDiff: +(rel * 100).toFixed(4) + "%" };
        if (rel > maxRelDiff) maxRelDiff = rel;
      } else {
        diffs[f] = { live: a, d1: b, match: a === b };
      }
    }
  }
  // candleCount will differ (live ~248 vs D1 up to 400) — that's expected and
  // does NOT affect indicators (they use trailing windows). Flag it as info.
  const verdict = (live && d1 && maxRelDiff < 0.005) ? "PASS (within 0.5%)"
                : (live && d1) ? "REVIEW (diff >0.5% — investigate)"
                : "INCOMPLETE (one side missing)";

  return cors({
    status: "success", symbol: sym, verdict: verdict,
    max_rel_diff_pct: +(maxRelDiff * 100).toFixed(4),
    note: "candleCount differs by design (D1 stores more bars); indicators use trailing windows so values still match.",
    diffs: diffs, liveError: liveErr, d1Error: d1Err,
  });
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

  // D1: resolve the cache flag ONCE per run (not per symbol). If on, the per-symbol
  // path tries D1 first and falls back to live fetch on any miss/stale/error.
  const d1On = await d1Enabled(env);
  let d1Hits = 0, d1Misses = 0;
  audit.log("S4_OHLCV", "", "D1MODE", d1On ? "D1 cache ENABLED (live fallback on miss)" : "D1 cache OFF (live fetch)");

  for (let i = 0; i < symbols.length; i += PIPE_BATCH_SIZE) {
    const batch = symbols.slice(i, i + PIPE_BATCH_SIZE);

    const batchResults = await Promise.all(
      batch.map(async function(sym) {
        try {
          const instrToken = tokenMap[sym];
          let ohlcv = null, fromD1 = false;
          // D1 FAST PATH: read stored candles, compute via the SHARED math.
          if (d1On) {
            const candles = await d1ReadCandles(env, sym);
            if (candles) { ohlcv = pipeComputeIndicatorsFromCandles(sym, candles); fromD1 = true; }
          }
          // FALLBACK: live fetch (also the only path when flag off / QE_DB unbound).
          if (!ohlcv) ohlcv = await pipeFetchOhlcvSymbol(env, token, sym, instrToken);
          return { sym: sym, ok: true, data: ohlcv, fromD1: fromD1 };
        } catch (e) {
          return { sym: sym, ok: false, error: e.message };
        }
      })
    );

    for (let bi = 0; bi < batchResults.length; bi++) {
      const br = batchResults[bi];
      if (br.fromD1) d1Hits++; else if (br.ok) d1Misses++;
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
        { expirationTtl: 32400 }
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

    // Per-signal callback buttons (Watch / Skip), labelled with the symbol.
    // Telegram hard-limits callback_data to 64 bytes/button. The full trade
    // payload was ~175 bytes → Telegram rejected the whole message (send FAIL,
    // signalCount 0). Carry ONLY {a:action, s:signalId} (~32 bytes); the callback
    // handler reads the full signal back from KV (qe_signal_<signalId>, written above).
    keyboardRows.push([
      { text: "👀 " + c.symbol, callback_data: JSON.stringify({ a: "WATCH",  s: signalId }) },
      { text: "❌ Skip",        callback_data: JSON.stringify({ a: "REJECT", s: signalId }) },
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
const PIPE_HISTORY_BUDGET   = 150; // max symbols into S4 per cron run.
                                   // P5 (paid plan): was 34, sized purely to hit the free-tier
                                   // 50-subrequest cap (1 heartbeat + 13 bhav + 1 nifty + 34 OHLCV
                                   // + 1 signals = 50). Paid tier = 1,000 subrequests, so that
                                   // ceiling is gone. New math: ~13 bhav + 1 nifty + 150 OHLCV +
                                   // overhead ≈ 166/1000 (83% headroom). Binding limits are now
                                   // 30s CPU (≈18s worst-case at BATCH_SIZE=10/DELAY=200) and Kite's
                                   // historical rate limit (one-retry-on-429 + survivorship log
                                   // absorb throttling). 150 is the safe per-run ceiling; wider
                                   // coverage comes from more runs/day (cross-run dedup exists),
                                   // not bigger single runs. Lifts daily coverage ~3.7%→~30% of the
                                   // liquid universe. Selection/ordering/scoring logic UNCHANGED.

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
  const universe = await pipeLoadUniverse(env, audit);
  if (!universe) {
    await writePipeStatus(env, "FAILED", 0, { runId: runId, error: "Universe not found in KV" });
    await env.KITE_STORE.put("qe_pipe_audit", JSON.stringify(audit.getAll().slice(0, 500)));
    await sendTelegram(env,
      `⚠️ <b>Pipeline Failed — Stage 1</b>\nUniverse not in KV. Run /universe/refresh first.`);
    return { ok: false, error: "Universe not found" };
  }

  // ── Stage 2: Bhav Copy ───────────────────────────────────────────────────────
  const bhav = await pipeBhavCopy(env, token, universe, audit, survive);

  // ── Stage 3: Stream A Fast (bhav-only) ──────────────────────────────────────
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

  // Per-run history budget. Manual runs use a smaller budget than crons.
  // P5 (paid plan): was 30 (free-tier: 13 bhav + 30 history + 4 telegram = 47/50).
  // Paid tier removes that cap; raised to 60 — still conservative for an on-demand
  // run (faster turnaround than a full 150 cron run, lighter Kite-rate pressure).
  const historyBudget = skipDedup ? 60 : PIPE_HISTORY_BUDGET;

  // Select top N for this run's S4 history fetch
  const selected     = notYetAnalysed.slice(0, historyBudget);
  const ohlcvQueue    = selected.map(function(r) { return r.sym; });
  const ohlcvDropped  = notYetAnalysed.slice(historyBudget).map(function(r) { return r.sym; });

  // ── Observability only (traceability): full rank-list snapshot ──────────────
  // Records the rank/selection decision that was ALREADY made above. Pure
  // observer: reads finalized values, writes one KV key, assigns nothing back
  // into the pipeline. Does NOT touch filters/scoring/ranking/budget/selection.
  // KV put is NOT a subrequest → zero impact on the 50-subrequest cap.
  // Lets any future symbol be traced at the S3→S3B boundary from one key:
  //   presence  → passed Stream A Fast
  //   r         → momentum rank
  //   sel:false → excluded by history budget
  // Keyed by IST-date + runId so runs don't overwrite; 7-day TTL bounds growth.
  try {
    const selectedSet = {};
    for (let qi = 0; qi < ohlcvQueue.length; qi++) selectedSet[ohlcvQueue[qi]] = true;
    const rankSnapshot = ranked.map(function(r, idx) {
      const b = bhav[r.sym];
      return {
        s:   r.sym,
        r:   idx + 1,
        m:   r.momentum,
        sel: !!selectedSet[r.sym],
        px:  (b && typeof b.last_price === "number") ? b.last_price : null,
      };
    });
    const istDate = new Date(Date.now() + 5.5 * 60 * 60 * 1000)
      .toISOString().slice(0, 10).replace(/-/g, "");
    await env.KITE_STORE.put(
      "qe_pipe_rank_" + istDate + "_" + runId,
      JSON.stringify(rankSnapshot),
      { expirationTtl: 7 * 24 * 60 * 60 }); // 7 days
  } catch (e) {
    console.warn("[rankSnapshot] non-fatal:", e && e.message);
  }
  // ── End observability block ────────────────────────────────────────────────

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
    // Update dedup map: only symbols that PASSED (became candidates) are marked
    // "analysed today". Recall fix (11-Jun): previously every SELECTED symbol (all
    // that entered OHLCV) was marked, so a stock that FAILED the morning run was
    // deduped and never re-examined — even if it broke out cleanly by the afternoon
    // run (the freshest early-confirmed-breakout archetype). Now a failed symbol
    // stays eligible for later runs; it must still clear every S5/S6 filter to
    // signal, so this is pure recall gain with zero precision cost. Symbols absent
    // from the map get freshness 100 next run (correct: a fresh setup deserves a
    // full-priority look). dsBySym (built above) holds exactly the passing set.
    // skipDedup (manual): do NOT write — a manual re-run must not consume the
    // crons' daily coverage pool or affect their dedup state.
    if (!skipDedup) {
      for (let si = 0; si < selected.length; si++) {
        if (dsBySym[selected[si].sym]) {                       // only passers
          analysedToday[selected[si].sym] = selected[si].momentum;
        }
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
  const authErr = await requireApiAuth(request, env); if (authErr) return authErr; // A4a
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
    var industry = null;

    // ── Industry label ────────────────────────────────────────────────────────
    // Screener page contains: <a href="/screen/raw/?query=Industry+Name+%3D+...">Label</a>
    // The query param is the unique anchor — extract the link text as the label.
    var indMatch = /screen\/raw\/[^"]*[Ii]ndustry[^"]*"[^>]*>([^<]+)<\/a>/i.exec(html);
    if (indMatch) industry = indMatch[1].trim();

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
             industry: industry, _source: "screener_worker" };
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
    if (cron === "*/5 4-10 * * 2-6") {
      ctx.waitUntil(monitorPositions(env));
    }

    // 10:30 UTC Mon–Fri = 16:00 IST — Daily summary
    if (cron === "30 10 * * 2-6") {
      ctx.waitUntil(sendDailySummary(env));
      // D1: append today's bar to the history cache after close (cheap bulk-quote).
      // No-op if QE_DB unbound. Independent of USE_D1_CACHE so the cache stays
      // warm/current even while you're still verifying before flipping the flag on.
      ctx.waitUntil(handleD1Update(env));
    }

    // 03:00 UTC Sunday = 08:30 IST Sunday — Weekly universe rebuild
    if (cron === "0 3 * * 0") {
      ctx.waitUntil(buildUniverse(env));
    }
  },

  // ── HTTP fetch handler ──────────────────────────────────────────────────────
  async fetch(request, env, ctx) {
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

    // ══ D1 HISTORY CACHE ROUTES (Option 2) ═══════════════════════════════════
    if (path === "/d1/init"     && method === "POST") { return handleD1Init(env); }
    if (path === "/d1/status"   && method === "GET")  { return handleD1Status(env); }
    if (path === "/d1/backfill" && method === "POST") { return handleD1Backfill(request, env); }
    if (path === "/d1/autobackfill" && method === "POST") { return handleD1AutoBackfill(request, env, ctx); }
    if (path === "/d1/update"   && method === "POST") {
      const r = await handleD1Update(env);
      return cors({ status: r.ok ? "success" : "error", ...r });
    }
    if (path === "/d1/verify"   && method === "GET")  { return handleD1Verify(request, env); }

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
