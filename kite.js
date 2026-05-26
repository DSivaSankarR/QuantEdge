/**
 * QuantEdge Cloudflare Worker — kite.js v2.5
 * Changelog v2.5 (26-May-2026):
 *   - Added POST /gtt/create  → places a single-leg GTT BUY order via Kite API
 *   - Added GET  /gtt/list    → fetches all active GTTs from Kite
 *   - Added DELETE /gtt/delete/:id → cancels a GTT by trigger_id
 * All GTT calls route through Worker (static IP constraint from Apr-2025 mandate)
 */

const KITE_API_BASE = "https://api.kite.trade";
const API_KEY = "x9atdliuwa1evccb";
const KV_TOKEN_KEY = "kite_access_token";

// ─── CORS headers ────────────────────────────────────────────────────────────
const CORS = {
  "Access-Control-Allow-Origin": "*",
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

// ─── Kite API proxy helper ────────────────────────────────────────────────────
async function kiteRequest(method, path, body, token) {
  const url = `${KITE_API_BASE}${path}`;
  const headers = {
    "X-Kite-Version": "3",
    Authorization: kiteAuthHeader(token),
  };

  let fetchOptions = { method, headers };

  if (body && method !== "GET") {
    // Kite expects form-encoded for POST/PUT/DELETE
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    fetchOptions.body = new URLSearchParams(body).toString();
  }

  const resp = await fetch(url, fetchOptions);
  const data = await resp.json();
  return { ok: resp.ok, status: resp.status, data };
}

// ─── Route handler ────────────────────────────────────────────────────────────
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    // Preflight
    if (method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS });
    }

    // ── GET / — Root handler (status + OHLCV + fundamentals) ──────────────────
    // Preserves full backward compatibility with all index.html calls
    if ((path === "/" || path === "") && method === "GET") {
      const symbol   = url.searchParams.get("symbol");
      const interval = url.searchParams.get("interval");
      const range    = url.searchParams.get("range");
      const type     = url.searchParams.get("type");

      // ── Status check (no params) ──
      // index.html checkKiteStatus() calls GET / and expects {"kite":"connected"}
      if (!symbol) {
        const token = await env.KITE_STORE.get(KV_TOKEN_KEY);
        return cors({
          kite:    token ? "connected" : "disconnected",
          status:  "success",
          version: "2.5"
        });
      }

      // ── Fundamentals fetch (?symbol=X&type=fundamentals) ──
      if (type === "fundamentals") {
        try {
          const token = await getToken(env);
          // Clean symbol — strip .NS suffix if present
          const cleanSym = symbol.replace(/\.NS$/, "").toUpperCase();
          const { ok, data } = await kiteRequest(
            "GET", `/quote?i=NSE:${encodeURIComponent(cleanSym)}`, null, token
          );
          if (!ok) throw new Error(data.message || "Quote failed");
          const q = data.data?.[`NSE:${cleanSym}`];
          if (!q) throw new Error("No data for " + cleanSym);
          return cors({
            status: "success",
            source: "kite",
            symbol: cleanSym,
            fundamentals: {
              last_price:    q.last_price,
              volume:        q.volume,
              average_price: q.average_price,
              oi:            q.oi,
              net_change:    q.net_change,
              ohlc:          q.ohlc
            }
          });
        } catch (e) {
          // Fallback — return empty so index.html handles gracefully
          return cors({ status: "error", source: "kite", message: e.message }, 200);
        }
      }

      // ── OHLCV fetch (?symbol=X&interval=Y&range=Z) ──
      // index.html fetchOHLCV calls this for all stock analysis
      try {
        const token = await getToken(env);
        const cleanSym = symbol.replace(/\.NS$/, "").toUpperCase();

        // Map range to Kite from/to dates
        const now   = new Date();
        const msDay = 86400000;
        const rangeMap = {
          "5d":  5,   "1mo": 30,  "3mo": 90,
          "6mo": 180, "1y":  365, "2y":  730, "5y": 1825
        };
        const days   = rangeMap[range] || 365;
        const from   = new Date(now - days * msDay);
        const fromStr = from.toISOString().slice(0, 10);
        const toStr   = now.toISOString().slice(0, 10);

        // Map interval to Kite interval
        const intervalMap = {
          "1d": "day", "1wk": "week", "1mo": "month",
          "5m": "5minute", "15m": "15minute", "60m": "60minute"
        };
        const kiteInterval = intervalMap[interval] || "day";

        // Need instrument token for historical API
        // Use quote endpoint to get instrument_token first
        const quoteRes = await kiteRequest("GET", `/quote?i=NSE:${encodeURIComponent(cleanSym)}`, null, token);
        if (!quoteRes.ok) throw new Error("Quote failed: " + quoteRes.data.message);
        const instrToken = quoteRes.data.data?.[`NSE:${cleanSym}`]?.instrument_token;
        if (!instrToken) throw new Error("No instrument token for " + cleanSym);

        const histRes = await kiteRequest(
          "GET",
          `/instruments/historical/${instrToken}/${kiteInterval}?from=${fromStr}&to=${toStr}`,
          null, token
        );
        if (!histRes.ok) throw new Error("Historical fetch failed");

        const candles = histRes.data.data?.candles || [];
        // Return in Yahoo Finance format for backward compat with index.html parser
        return cors({
          status: "success",
          source: "kite",
          chart: {
            result: [{
              meta: { symbol: cleanSym, currency: "INR", exchangeName: "NSE" },
              timestamp: candles.map(c => Math.floor(new Date(c[0]).getTime() / 1000)),
              indicators: {
                quote: [{
                  open:   candles.map(c => c[1]),
                  high:   candles.map(c => c[2]),
                  low:    candles.map(c => c[3]),
                  close:  candles.map(c => c[4]),
                  volume: candles.map(c => c[5])
                }]
              }
            }],
            error: null
          }
        });

      } catch (e) {
        // If Kite fails, return error so index.html falls back to Yahoo Finance
        return cors({
          status: "error",
          source: "kite_error",
          message: e.message,
          chart: { result: null, error: e.message }
        }, 200); // 200 so index.html gets the body and handles fallback
      }
    }

    // ── /login ────────────────────────────────────────────────────────────────
    if (path === "/login" && method === "GET") {
      const loginUrl = `https://kite.zerodha.com/connect/login?api_key=${API_KEY}&v=3`;
      return Response.redirect(loginUrl, 302);
    }

    // ── /callback ─────────────────────────────────────────────────────────────
    if (path === "/callback" && method === "GET") {
      const requestToken = url.searchParams.get("request_token");
      if (!requestToken) return corsErr("Missing request_token");

      const apiSecret = await env.KITE_STORE.get("api_secret");
      if (!apiSecret) return corsErr("API secret not configured in KV");

      // SHA256 checksum: api_key + request_token + api_secret
      const raw = `${API_KEY}${requestToken}${apiSecret}`;
      const hashBuffer = await crypto.subtle.digest(
        "SHA-256",
        new TextEncoder().encode(raw)
      );
      const checksum = Array.from(new Uint8Array(hashBuffer))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("");

      const resp = await fetch(`${KITE_API_BASE}/session/token`, {
        method: "POST",
        headers: {
          "X-Kite-Version": "3",
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: new URLSearchParams({
          api_key: API_KEY,
          request_token: requestToken,
          checksum,
        }).toString(),
      });

      const data = await resp.json();
      if (!resp.ok) return corsErr(data.message || "Session generation failed", 401);

      const accessToken = data.data.access_token;
      await env.KITE_STORE.put(KV_TOKEN_KEY, accessToken);

      return new Response(
        `<html><body style="font-family:monospace;padding:2rem">
          <h2>✅ Kite Login Successful</h2>
          <p>Access token stored. QuantEdge KITE badge will show ✓</p>
          <p><a href="https://dsivasankarr.github.io/QuantEdge">→ Open QuantEdge</a></p>
        </body></html>`,
        { status: 200, headers: { "Content-Type": "text/html" } }
      );
    }

    // ── /token (check) ────────────────────────────────────────────────────────
    if (path === "/token" && method === "GET") {
      const token = await env.KITE_STORE.get(KV_TOKEN_KEY);
      return cors({ status: "success", has_token: !!token });
    }

    // ── /quote ────────────────────────────────────────────────────────────────
    if (path === "/quote" && method === "GET") {
      const symbol = url.searchParams.get("symbol");
      if (!symbol) return corsErr("Missing symbol parameter");

      try {
        const token = await getToken(env);
        const { ok, data } = await kiteRequest(
          "GET",
          `/quote?i=NSE:${encodeURIComponent(symbol)}`,
          null,
          token
        );
        if (!ok) return corsErr(data.message || "Quote fetch failed", 502);
        return cors({ status: "success", data: data.data });
      } catch (e) {
        return corsErr(e.message, 401);
      }
    }

    // ── /instruments/NSE ─────────────────────────────────────────────────────
    if (path === "/instruments/NSE" && method === "GET") {
      try {
        const token = await getToken(env);
        const resp = await fetch(`${KITE_API_BASE}/instruments/NSE`, {
          headers: {
            "X-Kite-Version": "3",
            Authorization: kiteAuthHeader(token),
          },
        });
        if (!resp.ok) return corsErr("Instruments fetch failed", 502);
        const csv = await resp.text();
        return new Response(csv, {
          headers: { "Content-Type": "text/csv", ...CORS },
        });
      } catch (e) {
        return corsErr(e.message, 401);
      }
    }

    // ════════════════════════════════════════════════════════════════════════
    // GTT ENDPOINTS (v2.5)
    // ════════════════════════════════════════════════════════════════════════

    // ── POST /gtt/create ─────────────────────────────────────────────────────
    // Body (JSON): { symbol, cmp, entry, quantity }
    // Creates a single-leg GTT BUY:
    //   condition trigger  = entry price (fires when LTP crosses entry)
    //   order limit price  = entry price (LIMIT BUY at entry)
    //   product            = CNC (cash-and-carry for swing)
    if (path === "/gtt/create" && method === "POST") {
      let body;
      try {
        body = await request.json();
      } catch {
        return corsErr("Invalid JSON body");
      }

      const { symbol, cmp, entry, quantity } = body;

      if (!symbol || !cmp || !entry || !quantity) {
        return corsErr("Required fields: symbol, cmp, entry, quantity");
      }
      if (quantity <= 0) return corsErr("Quantity must be > 0");
      if (entry <= 0 || cmp <= 0) return corsErr("Price values must be > 0");

      // Trigger value = entry price (GTT fires when LTP touches entry)
      // Limit order price = entry price (small buffer not needed; LIMIT gives slippage control)
      const triggerPrice = parseFloat(entry).toFixed(2);
      const limitPrice   = parseFloat(entry).toFixed(2);
      const lastPrice    = parseFloat(cmp).toFixed(2);

      const condition = JSON.stringify({
        exchange: "NSE",
        tradingsymbol: symbol.toUpperCase(),
        trigger_values: [parseFloat(triggerPrice)],
        last_price: parseFloat(lastPrice),
      });

      const orders = JSON.stringify([
        {
          exchange: "NSE",
          tradingsymbol: symbol.toUpperCase(),
          transaction_type: "BUY",
          quantity: parseInt(quantity, 10),
          order_type: "LIMIT",
          product: "CNC",
          price: parseFloat(limitPrice),
        },
      ]);

      try {
        const token = await getToken(env);
        const { ok, data } = await kiteRequest(
          "POST",
          "/gtt/triggers",
          { type: "single", condition, orders },
          token
        );

        if (!ok) {
          return corsErr(
            data.message || "GTT creation failed at Kite API",
            resp?.status || 502
          );
        }

        const triggerId = data.data?.trigger_id;
        return cors({
          status: "success",
          trigger_id: triggerId,
          message: `GTT created for ${symbol.toUpperCase()} @ ₹${triggerPrice} | Qty: ${quantity}`,
          kite_url: `https://kite.zerodha.com/gtt`,
        });
      } catch (e) {
        return corsErr(e.message, e.message.includes("token") ? 401 : 502);
      }
    }

    // ── GET /gtt/list ─────────────────────────────────────────────────────────
    // Returns all active GTTs from Kite account
    if (path === "/gtt/list" && method === "GET") {
      try {
        const token = await getToken(env);
        const { ok, data } = await kiteRequest("GET", "/gtt/triggers", null, token);
        if (!ok) return corsErr(data.message || "GTT list fetch failed", 502);

        // Filter only active GTTs and shape the response
        const gtts = (data.data || [])
          .filter((g) => g.status === "active")
          .map((g) => ({
            trigger_id: g.id,
            symbol: g.condition?.tradingsymbol,
            trigger_price: g.condition?.trigger_values?.[0],
            order_price: g.orders?.[0]?.price,
            quantity: g.orders?.[0]?.quantity,
            product: g.orders?.[0]?.product,
            type: g.orders?.[0]?.transaction_type,
            created_at: g.created_at,
            status: g.status,
          }));

        return cors({ status: "success", count: gtts.length, gtts });
      } catch (e) {
        return corsErr(e.message, 401);
      }
    }

    // ── DELETE /gtt/delete/:id ────────────────────────────────────────────────
    // Cancels a specific GTT by trigger_id
    if (path.startsWith("/gtt/delete/") && method === "DELETE") {
      const triggerId = path.split("/gtt/delete/")[1];
      if (!triggerId || isNaN(triggerId)) return corsErr("Invalid trigger_id");

      try {
        const token = await getToken(env);
        const resp = await fetch(`${KITE_API_BASE}/gtt/triggers/${triggerId}`, {
          method: "DELETE",
          headers: {
            "X-Kite-Version": "3",
            Authorization: kiteAuthHeader(token),
          },
        });
        const data = await resp.json();
        if (!resp.ok) return corsErr(data.message || "GTT delete failed", 502);

        return cors({
          status: "success",
          message: `GTT #${triggerId} cancelled successfully`,
        });
      } catch (e) {
        return corsErr(e.message, 401);
      }
    }

    // ── 404 ───────────────────────────────────────────────────────────────────
    return corsErr(`Unknown route: ${method} ${path}`, 404);
  },
};
