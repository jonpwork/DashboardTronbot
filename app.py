"""
TRON FOREX BOT v3 — Multi-Símbolo + Ordens Manuais + Performance
BTC/ETH/SOL/BNB/XRP + Bybit Spot + Futuros + Ordens Limitadas
"""

import requests, time, os, json, base64, threading, hmac, hashlib
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone, timedelta

# ── Lê .env ─────────────────────────────────────────────────
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())
    print("[ENV] Variaveis carregadas do .env")

# ══════════ CONFIG ════════════════════════════════════════════
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID          = os.environ.get("CHAT_ID", "")
GROQ_KEY         = os.environ.get("GROQ_API_KEY", "")
GITHUB_TOKEN     = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO      = os.environ.get("GITHUB_REPO", "")
GITHUB_FILE      = "memory.json"

BYBIT_API_KEY    = os.environ.get("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")
BYBIT_MODE       = os.environ.get("BYBIT_MODE", "real").lower()
BYBIT_LEVERAGE   = int(os.environ.get("BYBIT_LEVERAGE", "5"))

BYBIT_URL = ("https://api-testnet.bybit.com"
             if BYBIT_MODE == "testnet"
             else "https://api.bybit.com")

# BingX removida — usar apenas Bybit

SYMBOLS = {
    # Tier 1 — Alta liquidez
    "BTCUSDT":  {"qty": float(os.environ.get("QTY_BTC",   "0.001")),  "kraken": "XBTUSDT",  "min_wave": 30},
    "ETHUSDT":  {"qty": float(os.environ.get("QTY_ETH",   "0.01")),   "kraken": "ETHUSDT",  "min_wave": 2},
    "SOLUSDT":  {"qty": float(os.environ.get("QTY_SOL",   "0.1")),    "kraken": "SOLUSDT",  "min_wave": 1},
    "XRPUSDT":  {"qty": float(os.environ.get("QTY_XRP",   "10")),     "kraken": "XRPUSDT",  "min_wave": 0.05},
    "BNBUSDT":  {"qty": float(os.environ.get("QTY_BNB",   "0.01")),   "kraken": "BNBUSDT",  "min_wave": 1},
    # Tier 2 — Boa liquidez
    "DOGEUSDT": {"qty": float(os.environ.get("QTY_DOGE",  "100")),    "kraken": "XDGUSD",   "min_wave": 0.005},
    "ADAUSDT":  {"qty": float(os.environ.get("QTY_ADA",   "20")),     "kraken": "ADAUSDT",  "min_wave": 0.02},
    "AVAXUSDT": {"qty": float(os.environ.get("QTY_AVAX",  "0.1")),    "kraken": "AVAXUSDT", "min_wave": 0.5},
    "DOTUSDT":  {"qty": float(os.environ.get("QTY_DOT",   "1")),      "kraken": "DOTUSD",   "min_wave": 0.1},
    "LINKUSDT": {"qty": float(os.environ.get("QTY_LINK",  "1")),      "kraken": "LINKUSDT", "min_wave": 0.1},
    # Tier 3 — Volume sólido
    "LTCUSDT":  {"qty": float(os.environ.get("QTY_LTC",   "0.1")),    "kraken": "LTCUSDT",  "min_wave": 0.5},
    "ATOMUSDT": {"qty": float(os.environ.get("QTY_ATOM",  "1")),      "kraken": "ATOMUSDT", "min_wave": 0.1},
    "NEARUSDT": {"qty": float(os.environ.get("QTY_NEAR",  "2")),      "kraken": "NEARUSDT", "min_wave": 0.05},
    "APTUSDT":  {"qty": float(os.environ.get("QTY_APT",   "1")),      "kraken": "APTUSDT",  "min_wave": 0.1},
    "SUIUSDT":  {"qty": float(os.environ.get("QTY_SUI",   "5")),      "kraken": "SUIUSDT",  "min_wave": 0.02},
    "OPUSDT":   {"qty": float(os.environ.get("QTY_OP",    "2")),      "kraken": "OPUSDT",   "min_wave": 0.05},
    "ARBUSDT":  {"qty": float(os.environ.get("QTY_ARB",   "5")),      "kraken": "ARBUSD",   "min_wave": 0.02},
    "TRXUSDT":  {"qty": float(os.environ.get("QTY_TRX",   "50")),     "kraken": "TRXUSD",   "min_wave": 0.005},
    "TONUSDT":  {"qty": float(os.environ.get("QTY_TON",   "1")),      "kraken": "TONUSDT",  "min_wave": 0.05},
    "PEPEUSDT": {"qty": float(os.environ.get("QTY_PEPE",  "5000000")),"kraken": "PEPEUSD",  "min_wave": 0.000001},
}

CHECK_INTERVAL    = 60       # segundos entre cada ciclo de análise
SHORT_COOLDOWN    = 120      # cooldown curto p/ não repetir a MESMA entrada (entry/stop/alvo)
RISCO_USDT        = float(os.environ.get("RISCO_USDT", "1.07"))
MIN_WR_FILTER     = float(os.environ.get("MIN_WR_FILTER", "40"))
MIN_TRADES_RANK   = int(os.environ.get("MIN_TRADES_RANK", "3"))
PORT              = int(os.environ.get("PORT", "8080"))
SWING_N           = 5
SWING_M1          = 2    # lookback menor → mais pivôs detectados no M1
SWING_M5          = 3

last_signal_time = {}
last_update_id   = 0
_leverage_set    = set()

memory = {
    "analyses":     [],
    "signals":      [],
    "zone_tol":     0.08,
    "total_prints": 0,
    "last_update":  ""
}

# ─── HTTP SERVER ─────────────────────────────────────────────
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0]

        # ── Dashboard HTML ───────────────────────────────────
        if path in ("/", "/index.html", "/dashboard.html"):
            for fname in ("index.html", "dashboard.html"):
                fpath = os.path.join(_BASE_DIR, fname)
                if os.path.exists(fpath):
                    with open(fpath, "rb") as f:
                        body = f.read()
                    self._respond(200, "text/html; charset=utf-8", body)
                    return
            self._respond(404, "text/plain", b"index.html nao encontrado na pasta do bot")
            return

        # ── API: status geral ────────────────────────────────
        elif path == "/status":
            sinais   = memory.get("signals", [])
            wins     = [s for s in sinais if s.get("status") == "win"]
            losses   = [s for s in sinais if s.get("status") == "loss"]
            abertos  = [s for s in sinais if s.get("status") == "aberto"]
            total    = len(wins) + len(losses)
            wr       = round(len(wins)/total*100, 1) if total > 0 else 0
            pnl_brl  = sum(s.get("pnl_brl", 0) for s in wins + losses)

            # Posições com P&L acumulado
            pos_list = []
            for s in abertos:
                pos_list.append({
                    "sym":   s.get("symbol",""),
                    "dir":   s.get("direcao",""),
                    "entry": s.get("entrada", 0),
                    "stop":  s.get("stop", 0),
                    "alvo":  s.get("alvo", 0),
                    "rr":    s.get("rr", 0),
                    "tipo":  s.get("tipo",""),
                    "qty":   SYMBOLS.get(s.get("symbol",""),{}).get("qty", 0),
                    "pnl":   0,  # P&L ao vivo vem do Bybit — ver /posicoes
                })

            # Últimas notícias do cache interno
            news_out = []
            for n in _news_cache.get("items", [])[:15]:
                news_out.append({
                    "title":  n.get("title",""),
                    "source": n.get("source",""),
                    "emoji":  n.get("emoji",""),
                    "link":   n.get("link",""),
                    "score":  n.get("score", 1),
                    "ts":     int(time.time()*1000),
                })

            payload = {
                "ok":        True,
                "mode":      BYBIT_MODE,
                "leverage":  BYBIT_LEVERAGE,
                "symbols":   list(SYMBOLS.keys()),
                "wins":      len(wins),
                "losses":    len(losses),
                "wr":        wr,
                "total_pnl": round(pnl_brl, 2),
                "balance":   0,   # preenchido pelo /balance
                "positions": pos_list,
                "signals":   sinais[-50:],
                "news":      news_out,
                "ts":        br_now("%d/%m/%Y %H:%M"),
            }
            body = json.dumps(payload, ensure_ascii=False).encode()
            self._respond(200, "application/json", body)
            return

        # ── API: saldo Bybit ao vivo ─────────────────────────
        elif path == "/balance":
            try:
                r = bybit_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
                coins = r.get("result",{}).get("list",[{}])[0].get("coin",[])
                usdt  = next((float(c.get("equity") or c.get("walletBalance",0))
                              for c in coins if c["coin"]=="USDT"), 0)
                body  = json.dumps({"ok":True,"usdt":round(usdt,4)}).encode()
            except Exception as e:
                body = json.dumps({"ok":False,"error":str(e)}).encode()
            self._respond(200, "application/json", body)
            return

        # ── API: posições abertas Bybit ao vivo ──────────────
        elif path == "/positions":
            try:
                r    = bybit_get("/v5/position/list",
                                 {"category":"linear","settleCoin":"USDT"})
                lista = [p for p in r.get("result",{}).get("list",[])
                         if float(p.get("size",0)) > 0]
                out  = [{"sym":p["symbol"],"side":p["side"],
                         "size":p["size"],"entry":p.get("avgPrice",0),
                         "pnl":p.get("unrealisedPnl",0),
                         "sl":p.get("stopLoss",0),"tp":p.get("takeProfit",0)}
                        for p in lista]
                body = json.dumps({"ok":True,"positions":out}).encode()
            except Exception as e:
                body = json.dumps({"ok":False,"error":str(e)}).encode()
            self._respond(200, "application/json", body)
            return

        # ── Healthcheck ──────────────────────────────────────
        elif path == "/health":
            self._respond(200, "text/plain", b"OK")
            return

        else:
            self._respond(404, "text/plain", b"404 Not Found")

    def _respond(self, code, ctype, body):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a): pass   # silencia logs no terminal

def run_server():
    try:
        srv = HTTPServer(("0.0.0.0", PORT), Handler)
        print(f"[HTTP] Dashboard em http://localhost:{PORT}")
        srv.serve_forever()
    except Exception as e:
        print(f"[HTTP] Erro ao iniciar servidor: {e}")

# ─── TELEGRAM ────────────────────────────────────────────────
def send_telegram(msg, chat_id=None):
    cid = chat_id or CHAT_ID
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
            timeout=10)
        print(f"[TG] {msg[:80].strip()}")
    except Exception as e:
        print(f"Erro TG: {e}")

def get_updates():
    global last_update_id
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": last_update_id + 1, "timeout": 2}, timeout=8)
        ups = r.json().get("result", [])
        if ups: last_update_id = ups[-1]["update_id"]
        return ups
    except: return []

def download_photo(file_id):
    r  = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getFile",
                      params={"file_id": file_id}, timeout=10)
    fp = r.json()["result"]["file_path"]
    return requests.get(f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{fp}",
                        timeout=20).content

# ═══════════════════════════════════════════════════════════════
#  BYBIT V5 — assinatura corrigida
# ═══════════════════════════════════════════════════════════════
def _headers_get(params):
    ts  = str(int(time.time() * 1000))
    rw  = "5000"
    qs  = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    sig = hmac.new(BYBIT_API_SECRET.encode(),
                   (ts + BYBIT_API_KEY + rw + qs).encode(),
                   hashlib.sha256).hexdigest()
    return {"X-BAPI-API-KEY": BYBIT_API_KEY, "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-SIGN": sig, "X-BAPI-RECV-WINDOW": rw}

def _headers_post(body_str):
    ts  = str(int(time.time() * 1000))
    rw  = "5000"
    sig = hmac.new(BYBIT_API_SECRET.encode(),
                   (ts + BYBIT_API_KEY + rw + body_str).encode(),
                   hashlib.sha256).hexdigest()
    return {"X-BAPI-API-KEY": BYBIT_API_KEY, "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-SIGN": sig, "X-BAPI-RECV-WINDOW": rw,
            "Content-Type": "application/json"}

def bybit_get(path, params=None):
    if not BYBIT_API_KEY: return None
    params = params or {}
    try:
        r = requests.get(f"{BYBIT_URL}{path}", params=params,
                         headers=_headers_get(params), timeout=15)
        return r.json()
    except Exception as e:
        print(f"[BYBIT GET] {e}"); return None

def bybit_post(path, payload):
    if not BYBIT_API_KEY: return None
    body = json.dumps(payload, separators=(',', ':'))
    try:
        r = requests.post(f"{BYBIT_URL}{path}",
                          headers=_headers_post(body), data=body, timeout=15)
        return r.json()
    except Exception as e:
        print(f"[BYBIT POST] {e}"); return None

def set_cross_margin(symbol):
    """Define margem cruzada (cross) para o símbolo. retCode 110026 = já está no modo correto."""
    r = bybit_post("/v5/position/switch-margin-mode", {
        "category": "linear", "symbol": symbol,
        "tradeMode": 0,               # 0 = Cross Margin
        "buyLeverage": str(BYBIT_LEVERAGE),
        "sellLeverage": str(BYBIT_LEVERAGE)})
    ok = r and r.get("retCode") in (0, 110026)
    if not ok:
        print(f"[MARGIN] {symbol} switch-margin-mode: {r}")
    return ok

def set_leverage(symbol):
    if symbol in _leverage_set: return
    set_cross_margin(symbol)          # garante margem cruzada antes de setar alavancagem
    r = bybit_post("/v5/position/set-leverage", {
        "category": "linear", "symbol": symbol,
        "buyLeverage": str(BYBIT_LEVERAGE), "sellLeverage": str(BYBIT_LEVERAGE)})
    if r and r.get("retCode") in (0, 110043):
        _leverage_set.add(symbol)

# ── Ordens ───────────────────────────────────────────────────
def order_spot(symbol, side, qty):
    r = bybit_post("/v5/order/create", {
        "category": "spot", "symbol": symbol, "side": side,
        "orderType": "Market", "qty": str(qty), "timeInForce": "IOC"})
    if r and r.get("retCode") == 0:
        return {"ok": True, "order_id": r["result"].get("orderId", "")}
    return {"ok": False, "error": (r.get("retMsg", "?") if r else "sem resposta")}

def order_futures(symbol, side, qty, sl=None, tp=None):
    set_leverage(symbol)
    p = {"category": "linear", "symbol": symbol, "side": side,
         "orderType": "Market", "qty": str(qty), "timeInForce": "IOC"}
    if sl: p["stopLoss"]   = str(round(float(sl), 6))
    if tp: p["takeProfit"] = str(round(float(tp), 6))
    r = bybit_post("/v5/order/create", p)
    if r and r.get("retCode") == 0:
        return {"ok": True, "order_id": r["result"].get("orderId", "")}
    return {"ok": False, "error": (r.get("retMsg", "?") if r else "sem resposta")}

def order_limit(category, symbol, side, qty, price, sl=None, tp=None):
    if category == "linear": set_leverage(symbol)
    p = {"category": category, "symbol": symbol, "side": side,
         "orderType": "Limit", "qty": str(qty),
         "price": str(round(float(price), 6)), "timeInForce": "GTC"}
    if sl and category == "linear": p["stopLoss"]   = str(round(float(sl), 6))
    if tp and category == "linear": p["takeProfit"] = str(round(float(tp), 6))
    r = bybit_post("/v5/order/create", p)
    if r and r.get("retCode") == 0:
        return {"ok": True, "order_id": r["result"].get("orderId", "")}
    return {"ok": False, "error": (r.get("retMsg", "?") if r else "sem resposta")}

def close_futures_symbol(symbol):
    r = bybit_get("/v5/position/list", {"category": "linear", "symbol": symbol})
    if not r or r.get("retCode") != 0: return False, "Erro ao buscar posicao"
    closed = 0
    for pos in r.get("result", {}).get("list", []):
        size = float(pos.get("size", 0))
        if size == 0: continue
        side_c = "Sell" if pos["side"] == "Buy" else "Buy"
        bybit_post("/v5/order/create", {
            "category": "linear", "symbol": symbol, "side": side_c,
            "orderType": "Market", "qty": str(size),
            "reduceOnly": True, "timeInForce": "IOC"})
        closed += 1
    return closed > 0, f"{closed} posicao(oes) fechada(s)"

def close_futures_all():
    r = bybit_get("/v5/position/list", {"category": "linear", "settleCoin": "USDT"})
    if not r or r.get("retCode") != 0: return
    for pos in r.get("result", {}).get("list", []):
        size = float(pos.get("size", 0))
        if size == 0: continue
        side_c = "Sell" if pos["side"] == "Buy" else "Buy"
        bybit_post("/v5/order/create", {
            "category": "linear", "symbol": pos["symbol"], "side": side_c,
            "orderType": "Market", "qty": str(size),
            "reduceOnly": True, "timeInForce": "IOC"})

def cancel_open_orders(symbol, category="linear"):
    r = bybit_post("/v5/order/cancel-all", {"category": category, "symbol": symbol})
    if r and r.get("retCode") == 0: return {"ok": True}
    return {"ok": False, "error": (r.get("retMsg", "?") if r else "sem resposta")}

def _spot_dec(symbol):
    d = {"BTCUSDT":6,"ETHUSDT":5,"SOLUSDT":3,"BNBUSDT":3,
         "XRPUSDT":2,"DOGEUSDT":0,"ADAUSDT":1}
    return d.get(symbol, 4)

def _floor_qty(qty, symbol):
    f = 10 ** _spot_dec(symbol)
    return int(qty * f) / f

def sell_all_spot(symbol):
    coin = symbol.replace("USDT", "")
    r = bybit_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
    if not r or r.get("retCode") != 0: return {"ok": False, "error": "erro saldo"}
    coins = r.get("result", {}).get("list", [{}])[0].get("coin", [])
    bal   = next((float(c.get("walletBalance", 0)) for c in coins if c["coin"] == coin), 0)
    if bal <= 0: return {"ok": False, "error": f"Sem saldo de {coin}"}
    qty = _floor_qty(bal * 0.999, symbol)
    if qty <= 0: return {"ok": False, "error": "Saldo insuficiente"}
    return order_spot(symbol, "Sell", qty)

def broker_open_auto(symbol, direction, stop, target):
    side = "Buy" if direction == "BUY" else "Sell"
    return order_futures(symbol, side, SYMBOLS[symbol]["qty"], sl=stop, tp=target)
# ══════════════════════════════════════════════════════════════
#  BingX REMOVIDA — apenas Bybit
# ══════════════════════════════════════════════════════════════




def broker_account():
    return bybit_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})

def broker_positions():
    return bybit_get("/v5/position/list", {"category": "linear", "settleCoin": "USDT"})

def _parse_sym(s):
    s = s.upper()
    return s if s.endswith("USDT") else s + "USDT"

# ─── KRAKEN DATA ─────────────────────────────────────────────
TF_MAP = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240}

def _bybit_candles(symbol, tf, limit):
    """Fallback: candles via Bybit public API."""
    interval_map = {"1m":1,"5m":5,"15m":15,"1h":60,"4h":240}
    iv = interval_map.get(tf, 60)
    try:
        r = requests.get("https://api.bybit.com/v5/market/kline",
            params={"category":"linear","symbol":symbol,
                    "interval":str(iv),"limit":str(limit)}, timeout=15)
        r.raise_for_status()
        d = r.json()
        if d.get("retCode") != 0: raise Exception(d.get("retMsg","?"))
        rows = d["result"]["list"]
        rows = list(reversed(rows))
        return [{"open":float(k[1]),"high":float(k[2]),
                 "low":float(k[3]),"close":float(k[4])}
                for k in rows]
    except Exception as e:
        print(f"[BYBIT CANDLE] {symbol}/{tf}: {e}"); return []

# Pares que o Kraken nao suporta — usar Bybit
_BYBIT_ONLY = {"SUIUSDT","APTUSDT","OPUSDT","TONUSDT","PEPEUSDT",
               "NEARUSDT","ARBUSDT","AVAXUSDT"}

def get_candles(symbol, tf, limit=120):
    if symbol in _BYBIT_ONLY:
        return _bybit_candles(symbol, tf, limit)
    kp = SYMBOLS.get(symbol, {}).get("kraken", symbol)
    try:
        r = requests.get("https://api.kraken.com/0/public/OHLC",
                         params={"pair": kp, "interval": TF_MAP.get(tf, 60)}, timeout=15)
        r.raise_for_status()
        d = r.json()
        if d.get("error") and d["error"]: raise Exception(str(d["error"]))
        key = [k for k in d["result"] if k != "last"][0]
        return [{"open": float(k[1]), "high": float(k[2]),
                 "low": float(k[3]), "close": float(k[4])}
                for k in d["result"][key][-limit:]]
    except Exception as e:
        print(f"[KRAKEN] {symbol}/{tf}: {e}")
        return _bybit_candles(symbol, tf, limit)

# ─── GITHUB MEMORIA ──────────────────────────────────────────
def gh_h():
    return {"Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"}

def load_memory():
    global memory
    if not GITHUB_TOKEN or not GITHUB_REPO: return
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
        r   = requests.get(url, headers=gh_h(), timeout=10)
        if r.status_code == 200:
            memory = json.loads(base64.b64decode(r.json()["content"]).decode())
            print(f"[MEM] {memory['total_prints']} prints")
    except Exception as e: print(f"[MEM] {e}")

def save_memory():
    if not GITHUB_TOKEN or not GITHUB_REPO: return
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
        ct  = base64.b64encode(json.dumps(memory, indent=2, ensure_ascii=False).encode()).decode()
        r   = requests.get(url, headers=gh_h(), timeout=10)
        pl  = {"message": f"mem:{memory['total_prints']}", "content": ct}
        if r.status_code == 200: pl["sha"] = r.json()["sha"]
        requests.put(url, headers=gh_h(), json=pl, timeout=15)
    except Exception as e: print(f"[MEM save] {e}")

# ─── GROQ VISION ─────────────────────────────────────────────
VISION_PROMPT = ('Analise este grafico de trading. Retorne APENAS JSON valido:\n'
                 '{"timeframe":"","tendencia":"up/down/neutral","tipo_onda":"",'
                 '"nivel_entrada":0,"nivel_stop":0,"nivel_alvo":0,"correcao_pct":0,'
                 '"observacoes":"","padroes":[],"qualidade_setup":"alta/media/baixa"}')

def analyze_image(img_bytes):
    if not GROQ_KEY: raise Exception("GROQ_API_KEY nao configurada")
    b64 = base64.b64encode(img_bytes).decode()
    r = requests.post("https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
        json={"model": "meta-llama/llama-4-scout-17b-16e-instruct",
              "messages": [{"role": "user", "content": [
                  {"type": "text", "text": VISION_PROMPT},
                  {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
              ]}], "max_tokens": 800, "temperature": 0.1}, timeout=30)
    r.raise_for_status()
    t = r.json()["choices"][0]["message"]["content"].strip()
    return json.loads(t.replace("```json", "").replace("```", "").strip())

def process_image(img_bytes, chat_id, caption=""):
    send_telegram("Analisando grafico...", chat_id)
    try: a = analyze_image(img_bytes)
    except Exception as e: send_telegram(f"Erro: {e}", chat_id); return
    a["data"] = br_now("%d/%m/%Y %H:%M")
    memory["analyses"].append(a)
    if len(memory["analyses"]) > 100: memory["analyses"] = memory["analyses"][-100:]
    memory["total_prints"] += 1; memory["last_update"] = a["data"]
    save_memory()
    tend = a.get("tendencia", "—"); qual = a.get("qualidade_setup", "—")
    te = "📈" if tend == "up" else ("📉" if tend == "down" else "↔️")
    qe = "🟢" if qual == "alta" else ("🟡" if qual == "media" else "🔴")
    ep = a.get("nivel_entrada"); sp = a.get("nivel_stop"); alvo = a.get("nivel_alvo")
    msg = (f"✅ <b>Grafico analisado!</b>\n📊 {a.get('timeframe','—')} | {tend.upper()} {te}\n"
           f"{qe} Qualidade: <b>{qual.upper()}</b>\n💡 {a.get('observacoes','—')}\n")
    if ep:   msg += f"💰 ${float(ep):,.4f}\n"
    if sp:   msg += f"🛑 ${float(sp):,.4f}\n"
    if alvo: msg += f"🎯 ${float(alvo):,.4f}\n"
    msg += f"🧠 {memory['total_prints']} prints"
    send_telegram(msg, chat_id)

# ═══════════════════════════════════════════════════════════════
#  ANALISE TECNICA
#  M1 → Fluxo de Topos e Fundos   (entrada em 50% da nova pernada)
#  M5 → Elliott completo           (ondas impulsivas + correções ABC/ABCDE)
# ═══════════════════════════════════════════════════════════════

MIN_RR   = 1.0   # RR mínimo apenas para descartar setups com alvo pior que o risco
                  # (alvo/stop agora vêm dos níveis TÉCNICOS de topos/fundos —
                  #  não há mais projeção forçada para 1:3)
SWING_N  = 5     # lookback pivô para TFs mais longos
SWING_M1 = 3     # lookback pivô para M1
SWING_M5 = 3     # lookback pivô para M5

# ─────────────────────────────────────────────────────────────
#  UTILIDADES BÁSICAS
# ─────────────────────────────────────────────────────────────
def find_pivots(c, n):
    """Detecta pivôs de topo e fundo numa lista de candles."""
    h, l = [], []
    for i in range(n, len(c) - n):
        wh = [c[j]["high"] for j in range(i-n, i+n+1)]
        wl = [c[j]["low"]  for j in range(i-n, i+n+1)]
        if c[i]["high"] == max(wh): h.append((i, c[i]["high"]))
        if c[i]["low"]  == min(wl): l.append((i, c[i]["low"]))
    return h, l

def get_trend(c, n=SWING_N):
    """
    Tendência por sequência de topos e fundos.
    Para M1 (n=2): aceita até 2 pivôs para identificar direção.
    Fallback: compara fechamento recente vs médio quando pivôs insuficientes.
    """
    h, l = find_pivots(c, n)
    # Com 3+ pivôs de cada lado: lógica clássica HH+HL / LH+LL
    if len(h) >= 3 and len(l) >= 3:
        rh = h[-3:]; rl = l[-3:]
        hh = rh[2][1]>rh[1][1]>rh[0][1]; hl = rl[2][1]>rl[1][1]>rl[0][1]
        lh = rh[2][1]<rh[1][1]<rh[0][1]; ll = rl[2][1]<rl[1][1]<rl[0][1]
        if hh and hl: return "up"
        if lh and ll: return "down"
        if hh or hl:  return "up"
        if lh or ll:  return "down"
    # Com 2 pivôs: compara par mais recente
    if len(h) >= 2 and len(l) >= 2:
        h_up = h[-1][1] > h[-2][1]; l_up = l[-1][1] > l[-2][1]
        if h_up and l_up:   return "up"
        if not h_up and not l_up: return "down"
    # Fallback: preço atual vs média dos últimos N candles
    if len(c) >= 10:
        closes = [x["close"] for x in c[-20:]]
        mid    = sum(closes[:10]) / 10
        cur    = closes[-1]
        if cur > mid * 1.0003: return "up"
        if cur < mid * 0.9997: return "down"
    return "neutral"

def apply_rr(entry, stop, target, direction):
    """
    Não força mais RR 1:3. O alvo TÉCNICO (próximo topo/fundo relevante)
    é respeitado como está. Só corrige se o alvo estiver do lado errado
    (ex.: alvo abaixo da entrada numa operação de compra) — nesse caso
    espelha o risco como alvo mínimo de segurança.
    """
    risco = abs(entry - stop)
    if risco == 0: return target
    if direction == "up" and target <= entry:
        return entry + risco
    if direction == "down" and target >= entry:
        return entry - risco
    return target

def _build_signal(entry_price, stop, alvo, direction, tipo, wave_start=None, wave_end=None):
    """Monta dict padrão de sinal. Alvo/stop são níveis TÉCNICOS
    (próximos topos/fundos). Só descarta se RR < MIN_RR (sanity check)."""
    risco = abs(entry_price - stop)
    if risco == 0: return None
    alvo = apply_rr(entry_price, stop, alvo, direction)
    rr   = abs(alvo - entry_price) / risco
    if rr < MIN_RR: return None
    d = {"entry": entry_price, "stop": stop, "alvo": alvo,
         "rr": round(rr, 1), "tipo": tipo, "direcao": direction}
    if wave_start is not None: d["wave_start"] = wave_start
    if wave_end   is not None: d["wave_end"]   = wave_end
    return d

# ─────────────────────────────────────────────────────────────
#  M1 — FLUXO DE TOPOS E FUNDOS  (múltiplas pernadas)
#
#  Melhoria: varre TODAS as pernadas recentes (não apenas a última),
#  tanto em alta quanto em baixa, simultaneamente.
#  Entrada em 50% ± tolerância de cada pernada identificada.
#  Stop na ORIGEM da pernada. Alvo TÉCNICO (próximo topo/fundo).
# ─────────────────────────────────────────────────────────────
def m1_swing_entries(candles, min_wave=0):
    """
    M1 — Micro fluxo de topos e fundos.
    Retorna LISTA com todas as oportunidades simultâneas:

    1. SWING 50%    — pernada recente; entrada em 35-65% com stop na origem
    2. PADRÃO W     — fundo duplo (dois fundos em nível próximo); entrada no
                      rompimento do pescoço (topo entre os dois fundos)
    3. PADRÃO M     — topo duplo (dois topos em nível próximo); entrada na
                      quebra do pescoço (fundo entre os dois topos)
    4. MICRO PULLBACK — mini impulso seguido de retração rápida (1-3 candles);
                        entra quando retração termina (~50% do mini impulso)

    Todos os setups: stop técnico na ORIGEM + alvo TÉCNICO (próximo topo/fundo).
    """
    if len(candles) < 8: return []
    n    = SWING_M1
    h, l = find_pivots(candles, n)
    if not h or not l: return []
    cur     = candles[-1]["close"]
    results = []

    # ══════════════════════════════════════════════════════════
    # 1. SWING 50% — varredura de todas as pernadas recentes
    # ══════════════════════════════════════════════════════════
    for li, lv in l[-6:]:   # apenas as últimas 6 para não sobrecarregar
        for hi, hv in [(hi,hv) for hi,hv in h if hi > li][-2:]:
            wave = hv - lv
            if wave < min_wave: continue
            if not (lv + wave*0.35 <= cur <= lv + wave*0.65): continue
            if cur <= lv: continue
            s = _build_signal(cur, lv*0.9994, hv,
                              "up", "M1_SWING_LONG", lv, hv)
            if s: results.append(s)

    for hi, hv in h[-6:]:
        for li, lv in [(li,lv) for li,lv in l if li > hi][-2:]:
            wave = hv - lv
            if wave < min_wave: continue
            if not (lv + wave*0.35 <= cur <= lv + wave*0.65): continue
            if cur >= hv: continue
            s = _build_signal(cur, hv*1.0006, lv,
                              "down", "M1_SWING_SHORT", hv, lv)
            if s: results.append(s)

    # ══════════════════════════════════════════════════════════
    # 2. PADRÃO W — FUNDO DUPLO
    #
    #  Estrutura: L1 → H(pescoço) → L2 ≈ L1 → preço volta a subir
    #  L2 deve estar entre 95% e 105% do nível de L1 (fundos no mesmo nível)
    #  Entrada: rompimento do pescoço (preço ≥ pescoço)
    #  Stop: abaixo do mínimo entre L1 e L2
    #  Alvo: pescoço + (pescoço - mínimo) × MIN_RR
    # ══════════════════════════════════════════════════════════
    if len(l) >= 2 and len(h) >= 1:
        for i in range(len(l)-1):
            L1_idx, L1_val = l[i]
            # Pescoço: topo entre L1 e L2
            topos_entre = [(hi,hv) for hi,hv in h if hi > L1_idx]
            if not topos_entre: continue
            pescoço_idx, pescoço_val = topos_entre[0]   # primeiro topo após L1
            # L2: fundo após o pescoço
            fundos_pos = [(li,lv) for li,lv in l if li > pescoço_idx]
            if not fundos_pos: continue
            L2_idx, L2_val = fundos_pos[0]   # primeiro fundo após pescoço
            # Filtro: L2 próximo de L1 (±6%)
            nivel_diff = abs(L2_val - L1_val) / (L1_val + 1e-9)
            if nivel_diff > 0.06: continue
            # Largura mínima (não pode ser tudo no mesmo candle)
            if L2_idx - L1_idx < 3: continue
            # Amplitude mínima (pescoço - mínimo dos fundos)
            min_fundo = min(L1_val, L2_val)
            amp = pescoço_val - min_fundo
            if amp < min_wave * 0.5: continue
            # Entrada: preço acabou de subir do L2 / está perto do pescoço
            dist_pescoço = (pescoço_val - cur) / (amp + 1e-9)
            # Aceita: abaixo do pescoço (aguardando rompimento) ou logo acima
            if not (-0.20 <= dist_pescoço <= 0.35): continue
            # Apenas se L2 é recente (nas últimas posições do seq)
            if L2_idx < len(candles) - 20: continue
            stop = min_fundo * 0.9993
            alvo = pescoço_val + amp
            s = _build_signal(cur, stop, alvo, "up",
                              "M1_W_LONG",
                              wave_start=min_fundo, wave_end=pescoço_val)
            if s:
                s["pescoço"] = pescoço_val
                s["L1"] = L1_val; s["L2"] = L2_val
                results.append(s)

    # ══════════════════════════════════════════════════════════
    # 3. PADRÃO M — TOPO DUPLO
    #
    #  Estrutura: H1 → L(pescoço) → H2 ≈ H1 → preço cai
    #  H2 entre 95% e 105% de H1
    #  Entrada: quebra do pescoço (preço ≤ pescoço)
    #  Stop: acima do máximo entre H1 e H2
    #  Alvo: pescoço - (máximo - pescoço) × MIN_RR
    # ══════════════════════════════════════════════════════════
    if len(h) >= 2 and len(l) >= 1:
        for i in range(len(h)-1):
            H1_idx, H1_val = h[i]
            fundos_entre = [(li,lv) for li,lv in l if li > H1_idx]
            if not fundos_entre: continue
            pescoço_idx, pescoço_val = fundos_entre[0]
            topos_pos = [(hi,hv) for hi,hv in h if hi > pescoço_idx]
            if not topos_pos: continue
            H2_idx, H2_val = topos_pos[0]
            nivel_diff = abs(H2_val - H1_val) / (H1_val + 1e-9)
            if nivel_diff > 0.06: continue
            if H2_idx - H1_idx < 3: continue
            max_topo = max(H1_val, H2_val)
            amp = max_topo - pescoço_val
            if amp < min_wave * 0.5: continue
            dist_pescoço = (cur - pescoço_val) / (amp + 1e-9)
            if not (-0.20 <= dist_pescoço <= 0.35): continue
            if H2_idx < len(candles) - 20: continue
            stop = max_topo * 1.0007
            alvo = pescoço_val - amp
            s = _build_signal(cur, stop, alvo, "down",
                              "M1_M_SHORT",
                              wave_start=max_topo, wave_end=pescoço_val)
            if s:
                s["pescoço"] = pescoço_val
                s["H1"] = H1_val; s["H2"] = H2_val
                results.append(s)

    # ══════════════════════════════════════════════════════════
    # 4. MICRO PULLBACK — retração rápida após mini impulso
    #
    #  Lógica: identifica mini impulso (mín 3 candles de subida/descida
    #  contínua) seguido de retração de 40-65% em 1-4 candles.
    #  Entrada na zona de 50% da retração.
    #  Stop: extremo do mini impulso (fundo para long, topo para short).
    # ══════════════════════════════════════════════════════════
    if len(candles) >= 12:
        closes  = [c["close"] for c in candles]
        highs   = [c["high"]  for c in candles]
        lows    = [c["low"]   for c in candles]
        N = len(closes)

        # ── MICRO PULLBACK LONG ─────────────────────────────
        # Mini impulso: mínimo 3 candles consecutivos subindo
        for imp_start in range(N-8, N-3):
            if imp_start < 1: continue
            imp_end = imp_start
            for k in range(imp_start+1, min(imp_start+6, N)):
                if closes[k] > closes[k-1]: imp_end = k
                else: break
            if imp_end - imp_start < 2: continue
            imp_low  = min(lows[imp_start:imp_end+1])
            imp_high = max(highs[imp_start:imp_end+1])
            imp_size = imp_high - imp_low
            if imp_size < min_wave * 0.3: continue
            # Retração após o impulso
            retr_end = N - 1
            retr_low  = min(lows[imp_end:retr_end+1]) if imp_end < retr_end else imp_high
            retrac = (imp_high - retr_low) / (imp_size + 1e-9)
            if not (0.38 <= retrac <= 0.68): continue
            # Preço perto do fundo da retração (zona de entrada)
            zona_50 = imp_high - imp_size * 0.50
            if abs(cur - zona_50) / (imp_size + 1e-9) > 0.15: continue
            if cur <= imp_low: continue
            stop = imp_low * 0.9993
            alvo = imp_high
            s = _build_signal(cur, stop, alvo, "up",
                              "M1_PULLBACK_LONG",
                              wave_start=imp_low, wave_end=imp_high)
            if s: results.append(s)
            break   # um por ciclo

        # ── MICRO PULLBACK SHORT ────────────────────────────
        for imp_start in range(N-8, N-3):
            if imp_start < 1: continue
            imp_end = imp_start
            for k in range(imp_start+1, min(imp_start+6, N)):
                if closes[k] < closes[k-1]: imp_end = k
                else: break
            if imp_end - imp_start < 2: continue
            imp_high = max(highs[imp_start:imp_end+1])
            imp_low  = min(lows[imp_start:imp_end+1])
            imp_size = imp_high - imp_low
            if imp_size < min_wave * 0.3: continue
            retr_end  = N - 1
            retr_high = max(highs[imp_end:retr_end+1]) if imp_end < retr_end else imp_low
            retrac = (retr_high - imp_low) / (imp_size + 1e-9)
            if not (0.38 <= retrac <= 0.68): continue
            zona_50 = imp_low + imp_size * 0.50
            if abs(cur - zona_50) / (imp_size + 1e-9) > 0.15: continue
            if cur >= imp_high: continue
            stop = imp_high * 1.0007
            alvo = imp_low
            s = _build_signal(cur, stop, alvo, "down",
                              "M1_PULLBACK_SHORT",
                              wave_start=imp_high, wave_end=imp_low)
            if s: results.append(s)
            break

    # ── Remove duplicatas (mesma direção + stop muito próximo) ──
    seen = set(); unique = []
    for r in results:
        key = (r["direcao"], round(r["stop"] / (abs(r["stop"])*0.001+1e-9)))
        if key not in seen:
            seen.add(key); unique.append(r)
    return unique

# ─────────────────────────────────────────────────────────────
#  BOTH — ORDENS SIMULTÂNEAS (vértice de compressão)
#
#  Detecta figuras de compressão/convergência em M1 e M5:
#  triângulo, cunha, ou qualquer vértice onde os topos caem
#  e os fundos sobem (amplitude comprimindo).
#  Quando o preço está no vértice → dispara LONG + SHORT ao
#  mesmo tempo. O lado perdedor é cancelado automaticamente
#  quando o vencedor atingir o alvo.
# ─────────────────────────────────────────────────────────────
def detect_compression(candles, n=2, min_wave=0):
    """
    Detecta compressão (convergência de topos e fundos).
    Retorna dict com resistencia, suporte, amplitude, qualidade
    e se o preço está no vértice (last_zone).
    """
    if len(candles) < 15: return None
    h, l = find_pivots(candles, n)
    if len(h) < 3 or len(l) < 3: return None

    # Requer ao menos 3 topos decrescentes + 3 fundos crescentes
    th = [v for _, v in h[-4:]]   # últimos 4 topos
    tl = [v for _, v in l[-4:]]   # últimos 4 fundos

    topos_caindo  = all(th[i] > th[i+1] for i in range(len(th)-1))
    fundos_subindo = all(tl[i] < tl[i+1] for i in range(len(tl)-1))

    if not (topos_caindo and fundos_subindo): return None

    res  = th[-1]   # última resistência (topo mais baixo)
    sup  = tl[-1]   # último suporte (fundo mais alto)
    amp  = res - sup
    if amp < min_wave: return None

    cur  = candles[-1]["close"]
    # Qualidade: quanto mais o preço está dentro do 1/3 central do vértice
    dist_norm = abs(cur - (res + sup) / 2) / (amp + 1e-9)
    qualidade = "alta" if dist_norm < 0.25 else ("media" if dist_norm < 0.45 else None)
    if not qualidade: return None

    # Amplitude histórica para projeção de alvo
    amp_inicial = th[0] - tl[0]

    return {
        "resistencia":   res,
        "suporte":       sup,
        "amplitude":     amp,
        "amp_inicial":   amp_inicial,
        "qualidade":     qualidade,
        "cur":           cur,
    }

def both_entries(candles, min_wave=0, tf_label="m1"):
    """
    Gera par de ordens simultâneas para breakout em qualquer direção.
    Retorna (entry_long, entry_short) ou (None, None).
    Long:  entrada = cur, stop = suporte - margem, alvo = resistencia + amp_proj
    Short: entrada = cur, stop = resistencia + margem, alvo = suporte - amp_proj
    """
    comp = detect_compression(candles, n=2, min_wave=min_wave)
    if not comp: return None, None

    cur   = comp["cur"]
    res   = comp["resistencia"]
    sup   = comp["suporte"]
    amp   = comp["amplitude"]
    amp_i = comp["amp_inicial"]

    # Projeção de alvo: amplitude da formação projetada a partir do rompimento
    proj = max(amp_i, amp * MIN_RR)

    # LONG: stop abaixo do suporte, alvo breakout de alta
    stop_l = sup * 0.9994
    alvo_l = res + proj
    risco_l = abs(cur - stop_l)
    if risco_l > 0 and abs(alvo_l - cur) / risco_l >= MIN_RR:
        long_ = _build_signal(cur, stop_l, alvo_l, "up",
                              f"BOTH_LONG_{tf_label.upper()}",
                              wave_start=sup, wave_end=res)
    else:
        long_ = None

    # SHORT: stop acima da resistência, alvo breakout de baixa
    stop_s = res * 1.0006
    alvo_s = sup - proj
    risco_s = abs(stop_s - cur)
    if risco_s > 0 and abs(cur - alvo_s) / risco_s >= MIN_RR:
        short_ = _build_signal(cur, stop_s, alvo_s, "down",
                               f"BOTH_SHORT_{tf_label.upper()}",
                               wave_start=res, wave_end=sup)
    else:
        short_ = None

    return long_, short_

# ═══════════════════════════════════════════════════════════════
#  NOVOS MÓDULOS DE ANÁLISE — ALVOS MAIORES
#  ┌─────────────────────────────────────────────────────────┐
#  │ 1. H1/H4 Swing Structure  — alvos nos níveis de H1/H4  │
#  │ 2. Fibonacci Extension    — 1.272 / 1.618 / 2.618      │
#  │ 3. Order Blocks + FVG     — liquidez institucional       │
#  │ 4. Break of Structure     — BOS + reteste               │
#  │ Stop SEMPRE técnico (topos/fundos M1)                   │
#  └─────────────────────────────────────────────────────────┘
#  Regra geral: stop = topo/fundo M1 mais próximo;
#               alvo = estrutura H1/H4 mais próxima ou extensão fib
# ═══════════════════════════════════════════════════════════════

def _m1_technical_stop(candles_m1, direction, cur, min_wave):
    """
    Retorna stop técnico baseado em topos/fundos do M1.
    Long  → fundo M1 mais próximo abaixo do preço atual.
    Short → topo M1 mais próximo acima do preço atual.
    """
    h, l = find_pivots(candles_m1, SWING_M1)
    if direction == "up":
        # Fundos abaixo do preço (candidatos a stop)
        fundos = [v for _, v in l if v < cur * 0.9998]
        if not fundos: return cur * (1 - 0.003)   # fallback 0.3%
        return max(fundos) * 0.9993               # fundo mais próximo
    else:
        topos = [v for _, v in h if v > cur * 1.0002]
        if not topos: return cur * (1 + 0.003)
        return min(topos) * 1.0007

# ─────────────────────────────────────────────────────────────
# 1. H1 / H4 SWING STRUCTURE ENTRIES
#    Lógica: identifica estrutura de topos/fundos em H1 e H4.
#    Entrada quando M1 confirma direção + stop M1 técnico.
#    Alvo: próximo nível de estrutura H1 ou H4 (mínimo 2R).
# ─────────────────────────────────────────────────────────────
def htf_structure_entries(candles_m1, candles_h1, candles_h4, min_wave, symbol=""):
    """
    Detecta entradas alinhadas com estrutura H1/H4.
    Stop técnico no topo/fundo M1. Alvo no próximo nível H1/H4.
    Retorna lista de sinais com RR tipicamente 3-8.
    """
    if len(candles_m1) < 10: return []
    results = []
    cur = candles_m1[-1]["close"]

    for tf_label, candles_htf, n_piv in [("H1", candles_h1, 3), ("H4", candles_h4, 2)]:
        if len(candles_htf) < 10: continue
        h_htf, l_htf = find_pivots(candles_htf, n_piv)
        if not h_htf or not l_htf: continue

        trend_htf = get_trend(candles_htf, n_piv)
        if trend_htf == "neutral": continue

        # Níveis de resistência e suporte HTF
        res_levels = sorted([v for _, v in h_htf[-6:]], reverse=True)
        sup_levels = sorted([v for _, v in l_htf[-6:]])

        if trend_htf == "up":
            # Procura próximo alvo de resistência ACIMA do preço atual
            alvos = [r for r in res_levels if r > cur * 1.002]
            if not alvos: continue
            alvo_htf = min(alvos)   # primeira resistência acima

            # Suporte HTF mais próximo abaixo — zona de entrada
            sups_abaixo = [s for s in sup_levels if s < cur * 0.999]
            if not sups_abaixo: continue
            sup_htf = max(sups_abaixo)

            # Confirma: preço está acima do suporte e abaixo da resistência
            if not (sup_htf < cur < alvo_htf): continue

            # Stop técnico M1
            stop = _m1_technical_stop(candles_m1, "up", cur, min_wave)
            risco = cur - stop
            if risco <= 0 or risco > (alvo_htf - cur): continue

            # Exige RR mínimo de 2.0
            rr_calc = (alvo_htf - cur) / risco
            if rr_calc < 2.0: continue

            s = _build_signal(cur, stop, alvo_htf, "up",
                              f"HTF_{tf_label}_LONG",
                              wave_start=sup_htf, wave_end=alvo_htf)
            if s:
                s["htf_trend"] = trend_htf
                s["tf_label"]  = tf_label
                results.append(s)

        elif trend_htf == "down":
            alvos = [s for s in sup_levels if s < cur * 0.998]
            if not alvos: continue
            alvo_htf = max(alvos)   # primeiro suporte abaixo

            res_acima = [r for r in res_levels if r > cur * 1.001]
            if not res_acima: continue
            res_htf = min(res_acima)

            if not (alvo_htf < cur < res_htf): continue

            stop = _m1_technical_stop(candles_m1, "down", cur, min_wave)
            risco = stop - cur
            if risco <= 0 or risco > (cur - alvo_htf): continue

            rr_calc = (cur - alvo_htf) / risco
            if rr_calc < 2.0: continue

            s = _build_signal(cur, stop, alvo_htf, "down",
                              f"HTF_{tf_label}_SHORT",
                              wave_start=res_htf, wave_end=alvo_htf)
            if s:
                s["htf_trend"] = trend_htf
                s["tf_label"]  = tf_label
                results.append(s)

    return results

# ─────────────────────────────────────────────────────────────
# 2. FIBONACCI EXTENSION — alvos 1.272 / 1.618 / 2.618
#    Detecta impulso + correção válida (≥50% retração).
#    Projeta extensões fib do impulso como alvo.
#    Stop: fundo/topo M1 técnico.
#    RR típico: 3-10.
# ─────────────────────────────────────────────────────────────
FIB_EXTENSIONS = [1.272, 1.618, 2.000, 2.618]
FIB_MIN_RR     = 2.0

def fib_extension_entries(candles_m1, candles_ref, min_wave):
    """
    Busca padrões impulso→correção e projeta extensões fibonacci.
    candles_ref: M5 ou H1 para identificar o impulso mãe.
    Stop: topo/fundo técnico M1.
    """
    if len(candles_ref) < 15 or len(candles_m1) < 10: return []
    seq  = _pivots_sequence(candles_ref, SWING_M5)
    if len(seq) < 4: return []
    cur  = candles_m1[-1]["close"]
    results = []

    for i in range(len(seq) - 3):
        p0, p1, p2, p3 = seq[i], seq[i+1], seq[i+2], seq[i+3]
        recent = (i + 3 >= len(seq) - 2)
        if not recent: continue

        # ── Impulso de ALTA + correção ────────────────────────
        if p0[2]=="L" and p1[2]=="H" and p2[2]=="L":
            imp   = p1[1] - p0[1]
            corr  = p1[1] - p2[1]
            if imp < min_wave: continue
            corr_pct = corr / (imp + 1e-9)
            if not (0.382 <= corr_pct <= 0.786): continue
            # Preço deve estar perto do fim da correção (p2)
            if abs(cur - p2[1]) / (imp + 1e-9) > 0.15: continue
            if cur <= p0[1]: continue   # abaixo do início do impulso → inválido

            stop = _m1_technical_stop(candles_m1, "up", cur, min_wave)
            risco = cur - stop
            if risco <= 0: continue

            # Projeta extensões a partir do início da correção (p2) + comprimento do impulso
            for fib in FIB_EXTENSIONS:
                alvo = p0[1] + imp * fib   # extensão sobre o impulso mãe
                if alvo <= cur: continue
                rr_calc = (alvo - cur) / risco
                if rr_calc < FIB_MIN_RR: continue
                s = _build_signal(cur, stop, alvo, "up",
                                  f"FIB_{fib:.3f}_LONG",
                                  wave_start=p0[1], wave_end=p1[1])
                if s:
                    s["fib_level"]  = fib
                    s["corr_pct"]   = round(corr_pct, 2)
                    s["impulso_orig"] = p0[1]
                    s["impulso_fim"]  = p1[1]
                    results.append(s)
                break   # só o primeiro nível fib válido por impulso

        # ── Impulso de BAIXA + correção ───────────────────────
        if p0[2]=="H" and p1[2]=="L" and p2[2]=="H":
            imp   = p0[1] - p1[1]
            corr  = p2[1] - p1[1]
            if imp < min_wave: continue
            corr_pct = corr / (imp + 1e-9)
            if not (0.382 <= corr_pct <= 0.786): continue
            if abs(cur - p2[1]) / (imp + 1e-9) > 0.15: continue
            if cur >= p0[1]: continue

            stop = _m1_technical_stop(candles_m1, "down", cur, min_wave)
            risco = stop - cur
            if risco <= 0: continue

            for fib in FIB_EXTENSIONS:
                alvo = p0[1] - imp * fib
                if alvo >= cur: continue
                rr_calc = (cur - alvo) / risco
                if rr_calc < FIB_MIN_RR: continue
                s = _build_signal(cur, stop, alvo, "down",
                                  f"FIB_{fib:.3f}_SHORT",
                                  wave_start=p0[1], wave_end=p1[1])
                if s:
                    s["fib_level"]  = fib
                    s["corr_pct"]   = round(corr_pct, 2)
                    s["impulso_orig"] = p0[1]
                    s["impulso_fim"]  = p1[1]
                    results.append(s)
                break

    return results

# ─────────────────────────────────────────────────────────────
# 3. ORDER BLOCKS + FAIR VALUE GAP (FVG)
#    Order Block: vela de baixa antes de impulso de alta (OB Bull)
#                 vela de alta antes de impulso de baixa (OB Bear)
#    FVG: gap entre high de N-2 e low de N (alta) = zona de vácuo
#    Entrada: quando preço retorna ao OB ou FVG
#    Stop: extremo do OB / fundo técnico M1
#    Alvo: estrutura seguinte (próximo topo/fundo relevante)
# ─────────────────────────────────────────────────────────────
OB_MIN_IMPULSE_MULT = 1.5   # impulso deve ser 1.5× o tamanho do OB
OB_MIN_RR           = 2.0

def order_block_entries(candles_m1, candles_ref, min_wave):
    """
    Detecta Order Blocks e Fair Value Gaps com alvos de estrutura.
    candles_ref: M5 para identificar impulsos institucionais.
    Stop: fundo/topo M1 técnico.
    """
    if len(candles_ref) < 10 or len(candles_m1) < 10: return []
    cur     = candles_m1[-1]["close"]
    results = []
    N       = len(candles_ref)

    # ── ORDER BLOCKS ─────────────────────────────────────────
    for i in range(2, N - 2):
        ob   = candles_ref[i]
        ob_h = ob["high"]; ob_l = ob["low"]
        ob_sz = ob_h - ob_l
        if ob_sz < min_wave * 0.3: continue

        # OB Bullish: vela de baixa seguida de impulso de alta forte
        if ob["close"] < ob["open"]:   # vela de baixa
            imp_h = max(c["high"] for c in candles_ref[i+1:i+4])
            imp_l = min(c["low"]  for c in candles_ref[i+1:i+4])
            impulso = imp_h - ob_h
            if impulso < ob_sz * OB_MIN_IMPULSE_MULT: continue
            # Preço retornou à zona do OB (ob_l a ob_h)
            if not (ob_l * 0.999 <= cur <= ob_h * 1.001): continue
            # Alvo: estrutura à direita (próximo topo)
            h_ref, _ = find_pivots(candles_ref[i+1:], 2)
            if not h_ref: continue
            alvo = max(v for _, v in h_ref)
            if alvo <= cur * 1.002: continue
            stop = _m1_technical_stop(candles_m1, "up", cur, min_wave)
            risco = cur - stop
            if risco <= 0: continue
            rr_calc = (alvo - cur) / risco
            if rr_calc < OB_MIN_RR: continue
            s = _build_signal(cur, stop, alvo, "up",
                              "OB_BULL",
                              wave_start=ob_l, wave_end=ob_h)
            if s:
                s["ob_high"] = ob_h; s["ob_low"] = ob_l
                results.append(s)

        # OB Bearish: vela de alta seguida de impulso de baixa forte
        elif ob["close"] > ob["open"]:
            imp_l = min(c["low"]  for c in candles_ref[i+1:i+4])
            impulso = ob_l - imp_l
            if impulso < ob_sz * OB_MIN_IMPULSE_MULT: continue
            if not (ob_l * 0.999 <= cur <= ob_h * 1.001): continue
            _, l_ref = find_pivots(candles_ref[i+1:], 2)
            if not l_ref: continue
            alvo = min(v for _, v in l_ref)
            if alvo >= cur * 0.998: continue
            stop = _m1_technical_stop(candles_m1, "down", cur, min_wave)
            risco = stop - cur
            if risco <= 0: continue
            rr_calc = (cur - alvo) / risco
            if rr_calc < OB_MIN_RR: continue
            s = _build_signal(cur, stop, alvo, "down",
                              "OB_BEAR",
                              wave_start=ob_h, wave_end=ob_l)
            if s:
                s["ob_high"] = ob_h; s["ob_low"] = ob_l
                results.append(s)

    # ── FAIR VALUE GAPS (FVG) ─────────────────────────────────
    for i in range(2, N - 1):
        c_prev  = candles_ref[i - 2]
        c_mid   = candles_ref[i - 1]
        c_next  = candles_ref[i]

        # FVG Bullish: high[i-2] < low[i] — gap entre velas separadas por impulso
        if c_prev["high"] < c_next["low"]:
            fvg_top = c_next["low"]
            fvg_bot = c_prev["high"]
            fvg_sz  = fvg_top - fvg_bot
            if fvg_sz < min_wave * 0.2: continue
            # Preço retornou ao FVG
            if not (fvg_bot * 0.999 <= cur <= fvg_top * 1.001): continue
            # Alvo: estrutura acima
            h_ref, _ = find_pivots(candles_ref[i:], 2)
            if not h_ref: continue
            alvo = max(v for _, v in h_ref)
            if alvo <= cur * 1.001: continue
            stop = _m1_technical_stop(candles_m1, "up", cur, min_wave)
            risco = cur - stop
            if risco <= 0: continue
            if (alvo - cur) / risco < OB_MIN_RR: continue
            s = _build_signal(cur, stop, alvo, "up",
                              "FVG_BULL",
                              wave_start=fvg_bot, wave_end=fvg_top)
            if s: results.append(s)

        # FVG Bearish: low[i-2] > high[i]
        if c_prev["low"] > c_next["high"]:
            fvg_bot = c_next["high"]
            fvg_top = c_prev["low"]
            fvg_sz  = fvg_top - fvg_bot
            if fvg_sz < min_wave * 0.2: continue
            if not (fvg_bot * 0.999 <= cur <= fvg_top * 1.001): continue
            _, l_ref = find_pivots(candles_ref[i:], 2)
            if not l_ref: continue
            alvo = min(v for _, v in l_ref)
            if alvo >= cur * 0.999: continue
            stop = _m1_technical_stop(candles_m1, "down", cur, min_wave)
            risco = stop - cur
            if risco <= 0: continue
            if (cur - alvo) / risco < OB_MIN_RR: continue
            s = _build_signal(cur, stop, alvo, "down",
                              "FVG_BEAR",
                              wave_start=fvg_top, wave_end=fvg_bot)
            if s: results.append(s)

    # Deduplica por direção + alvo próximo
    seen, unique = set(), []
    for r in results:
        k = (r["direcao"], round(r["alvo"] / (abs(r["alvo"]) * 0.005 + 1e-9)))
        if k not in seen:
            seen.add(k); unique.append(r)
    return unique

# ─────────────────────────────────────────────────────────────
# 4. BREAK OF STRUCTURE (BOS) + RETESTE
#    BOS: rompimento limpo de topo/fundo estrutural.
#    Entrada: reteste da zona rompida (antigo topo vira suporte).
#    Stop: fundo/topo M1 técnico abaixo/acima da zona.
#    Alvo: próxima estrutura (topo/fundo seguinte do HTF).
#    RR típico: 3-6.
# ─────────────────────────────────────────────────────────────
BOS_RETEST_TOL = 0.004   # 0.4% de tolerância para reteste da zona

def bos_entries(candles_m1, candles_ref, min_wave):
    """
    Break of Structure: detecta rompimento de nível estrutural e
    entrada no reteste. Stop M1 técnico. Alvo na próxima estrutura.
    """
    if len(candles_ref) < 15 or len(candles_m1) < 10: return []
    cur     = candles_m1[-1]["close"]
    results = []
    h_ref, l_ref = find_pivots(candles_ref, SWING_M5)
    if len(h_ref) < 3 or len(l_ref) < 3: return []

    # ── BOS BULLISH: rompimento de topo + reteste ────────────
    # Condição: topo mais recente (h[-1]) foi rompido (preço passou acima)
    # e agora retorna para retestá-lo como suporte
    for i in range(len(h_ref) - 1):
        topo_idx, topo_val = h_ref[i]
        # Confirma que houve rompimento: algum candle posterior fechou acima
        rompimento = any(
            candles_ref[j]["close"] > topo_val * 1.001
            for j in range(topo_idx + 1, len(candles_ref))
        )
        if not rompimento: continue
        # Preço atual está retestando a zona do topo rompido
        if not (topo_val * (1 - BOS_RETEST_TOL) <= cur <= topo_val * (1 + BOS_RETEST_TOL)):
            continue
        # Alvo: próximo topo estrutural acima
        alvos_acima = [v for idx, v in h_ref if v > topo_val * 1.005]
        if not alvos_acima: continue
        alvo = min(alvos_acima)
        stop = _m1_technical_stop(candles_m1, "up", cur, min_wave)
        risco = cur - stop
        if risco <= 0: continue
        if (alvo - cur) / risco < 2.0: continue
        s = _build_signal(cur, stop, alvo, "up",
                          "BOS_BULL",
                          wave_start=stop, wave_end=topo_val)
        if s:
            s["bos_level"] = topo_val
            results.append(s)

    # ── BOS BEARISH: rompimento de fundo + reteste ───────────
    for i in range(len(l_ref) - 1):
        fundo_idx, fundo_val = l_ref[i]
        rompimento = any(
            candles_ref[j]["close"] < fundo_val * 0.999
            for j in range(fundo_idx + 1, len(candles_ref))
        )
        if not rompimento: continue
        if not (fundo_val * (1 - BOS_RETEST_TOL) <= cur <= fundo_val * (1 + BOS_RETEST_TOL)):
            continue
        alvos_abaixo = [v for idx, v in l_ref if v < fundo_val * 0.995]
        if not alvos_abaixo: continue
        alvo = max(alvos_abaixo)
        stop = _m1_technical_stop(candles_m1, "down", cur, min_wave)
        risco = stop - cur
        if risco <= 0: continue
        if (cur - alvo) / risco < 2.0: continue
        s = _build_signal(cur, stop, alvo, "down",
                          "BOS_BEAR",
                          wave_start=stop, wave_end=fundo_val)
        if s:
            s["bos_level"] = fundo_val
            results.append(s)

    return results

# ─────────────────────────────────────────────────────────────
#  ANALYZE SYMBOL — versão expandida
#  (lógica original inalterada + novos módulos adicionados)
# ─────────────────────────────────────────────────────────────
def analyze_symbol(symbol):
    cfg = SYMBOLS[symbol]
    cm  = get_candles(symbol, "1m",  80)
    c5  = get_candles(symbol, "5m", 120)
    c1h = get_candles(symbol, "1h",  72)   # 3 dias de H1
    c4h = get_candles(symbol, "4h",  60)   # 10 dias de H4
    if not cm and not c5: return None
    price = (cm or c5)[-1]["close"]

    m1_trend = get_trend(cm, SWING_M1) if cm else "neutral"
    m5_trend = get_trend(c5, SWING_M5) if c5 else "neutral"
    h1_trend = get_trend(c1h, 3)       if c1h else "neutral"
    h4_trend = get_trend(c4h, 2)       if c4h else "neutral"

    # ── M1: todas as pernadas long + short (ORIGINAL) ────────
    entries_m1 = m1_swing_entries(cm, cfg["min_wave"]) if cm else []

    # ── M1: compressão → both (ORIGINAL) ────────────────────
    both_m1_l, both_m1_s = both_entries(cm, cfg["min_wave"], "m1") if cm else (None, None)

    # ── M5: Elliott completo (ORIGINAL) ──────────────────────
    entries_m5 = m5_elliott_entries(c5, cfg["min_wave"]) if c5 else []

    # ── M5: compressão → both (ORIGINAL) ────────────────────
    both_m5_l, both_m5_s = both_entries(c5, cfg["min_wave"], "m5") if c5 else (None, None)

    # ── NOVOS: alvos maiores ──────────────────────────────────
    # 1. HTF Structure (H1/H4) — RR 2-8
    entries_htf = htf_structure_entries(cm or [], c1h or [], c4h or [],
                                        cfg["min_wave"], symbol) if cm else []

    # 2. Fibonacci Extensions (M5 como ref) — RR 2-10
    entries_fib = fib_extension_entries(cm or [], c5 or [], cfg["min_wave"]) if cm and c5 else []

    # 3. Order Blocks + FVG (M5 como ref) — RR 2-6
    entries_ob  = order_block_entries(cm or [], c5 or [], cfg["min_wave"])  if cm and c5 else []

    # 4. Break of Structure (H1 como ref) — RR 2-6
    entries_bos = bos_entries(cm or [], c1h or [], cfg["min_wave"]) if cm and c1h else []

    # Agrega todas as entradas de alto RR
    entries_htf_all = entries_htf + entries_fib + entries_ob + entries_bos

    # Melhor entry M5 (ORIGINAL)
    _pri = {"EW_ABC_C_LONG":4,"EW_ABC_C_SHORT":4,
            "EW_TRI_E_LONG":3,"EW_TRI_E_SHORT":3,
            "EW_W2_LONG":2,"EW_W2_SHORT":2,
            "EW_W4_LONG":2,"EW_W4_SHORT":2}
    best_m5 = max(entries_m5, key=lambda e: _pri.get(e["tipo"],1)) if entries_m5 else None

    # Melhor entry HTF (por RR decrescente)
    best_htf = max(entries_htf_all, key=lambda e: e.get("rr", 0)) if entries_htf_all else None

    # both_m1 / both_m5 como par (ORIGINAL)
    both_m1 = (both_m1_l, both_m1_s) if (both_m1_l or both_m1_s) else None
    both_m5 = (both_m5_l, both_m5_s) if (both_m5_l or both_m5_s) else None

    # Entry principal: prioriza HTF (maior alvo) > M1 > M5
    best_m1   = entries_m1[0] if entries_m1 else None
    best_entry = best_htf or best_m1 or best_m5

    return {
        "symbol":        symbol,
        "price":         price,
        "m1_trend":      m1_trend,
        "m5_trend":      m5_trend,
        "h1_trend":      h1_trend,
        "h4_trend":      h4_trend,
        "entry":         best_entry,
        "entries_m1":    entries_m1,
        "entry_m1":      best_m1,
        "entries_m5":    entries_m5,
        "best_m5":       best_m5,
        "both_m1":       both_m1,
        "both_m5":       both_m5,
        # Novos campos
        "entries_htf":   entries_htf,
        "entries_fib":   entries_fib,
        "entries_ob":    entries_ob,
        "entries_bos":   entries_bos,
        "entries_htf_all": entries_htf_all,
        "best_htf":      best_htf,
    }
#
#  Ondas IMPULSIVAS (1-2-3-4-5):
#    • Detecta sequência 5 ondas alternadas T/F.
#    • Entrada na onda 2 (retração de 38-62% da onda 1)
#      e na onda 4 (retração de 38-50% da onda 3).
#    • Após onda 5 completa → espera correção ABC.
#
#  Correções ABC (e variante ABCDE/triângulo):
#    • Onda A: perna impulsiva contra tendência principal.
#    • Onda B: retração parcial de A (38-62%).
#    • Onda C: perna igual/maior que A (fibonacci).
#      → Entrada ao FINAL da onda C para retomar tendência.
#    • Variante triângulo (ABCDE):
#      → Cada perna converge; entrada no final da onda E.
#
#  Entradas DENTRO da correção (pernadas A, B, C):
#    • Cada perna de A, B ou C é operable como mini-swing.
#    • Entrada em 50% de cada pernada com stop na origem.
# ─────────────────────────────────────────────────────────────

def _pivots_sequence(candles, n=SWING_M5):
    """Retorna lista de pivôs ordenados por índice: (idx, price, 'H'|'L')."""
    h, l = find_pivots(candles, n)
    seq = [(i, p, "H") for i, p in h] + [(i, p, "L") for i, p in l]
    seq.sort(key=lambda x: x[0])
    # Remove pivôs adjacentes do mesmo tipo (mantém alternância)
    cleaned = []
    for item in seq:
        if cleaned and cleaned[-1][2] == item[2]:
            # Mantém o mais extremo
            if item[2] == "H":
                cleaned[-1] = item if item[1] > cleaned[-1][1] else cleaned[-1]
            else:
                cleaned[-1] = item if item[1] < cleaned[-1][1] else cleaned[-1]
        else:
            cleaned.append(item)
    return cleaned

def _wave_in_zone(entry, wave_start, wave_end, lo=0.38, hi=0.62):
    """True se entry está na zona de retração fibonacci [lo, hi] da onda."""
    sz = abs(wave_end - wave_start)
    if sz == 0: return False
    ret = abs(entry - wave_end) / sz
    return lo <= ret <= hi

def m5_elliott_entries(candles, min_wave=0):
    """
    Analisa M5/M1 por Elliott com lógica completa de correções ABC/ABCDE.

    REGRA CENTRAL (conforme gráfico):
      • Identifica o IMPULSO principal (pernada mãe)
      • A correção ABC deve recuar MÍNIMO 50% do impulso
      • Dentro da correção: entradas em 50% de cada pernada (A, B, C)
        com stop na ORIGEM de cada pernada
      • No FIM DE C: entrada para retomar o fluxo principal
      • Triângulo ABCDE: entradas nas pernadas internas + fim de E
      • Alvos/stops TÉCNICOS (topos e fundos) em todas as entradas

    Tipos retornados:
      EW_W2 / EW_W4        — ondas impulsivas
      EW_ABC_C             — fim de C (retoma fluxo principal)
      EW_ABC_LEG_A/B/C     — 50% de cada pernada interna da correção
      EW_TRI_LEG / EW_TRI_E — pernadas e fim de E no triângulo
    """
    if len(candles) < 20: return []
    seq = _pivots_sequence(candles, SWING_M5)
    if len(seq) < 4: return []
    cur     = candles[-1]["close"]
    entries = []
    tol_leg = 0.15   # tolerância de 15% em torno do 50% de cada pernada
    tol_fim = 0.12   # tolerância de 12% para "estar perto do fim de C"

    def _sig(entry_p, stop_p, direction, tipo, ws=None, we=None,
             impulso_orig=None, impulso_fim=None, corr_pct=None):
        risco = abs(entry_p - stop_p)
        if risco == 0: return None
        # Alvo técnico: usa o pivô de referência (we) quando definido
        # e está do lado correto da entrada; senão usa impulso_fim.
        alvo = None
        for cand in (we, impulso_fim):
            if cand is None: continue
            if direction == "up" and cand > entry_p:
                alvo = cand; break
            if direction == "down" and cand < entry_p:
                alvo = cand; break
        if alvo is None:
            alvo = entry_p + risco if direction == "up" else entry_p - risco
        alvo = apply_rr(entry_p, stop_p, alvo, direction)
        s = _build_signal(entry_p, stop_p, alvo, direction, tipo, ws, we)
        if s and impulso_orig is not None:
            s["impulso_orig"] = impulso_orig
            s["impulso_fim"]  = impulso_fim
            s["corr_pct"]     = round(corr_pct or 0, 2)
        return s

    def _in_50(cur_, wave_start, wave_end, tol=tol_leg):
        """True se cur está em [35%,65%] da pernada wave_start→wave_end."""
        sz = abs(wave_end - wave_start)
        if sz == 0: return False
        retrace = abs(cur_ - wave_end) / sz   # quanto já recuou
        return (0.50 - tol) <= retrace <= (0.50 + tol)

    # ════════════════════════════════════════════════════════
    # 1. ONDAS IMPULSIVAS — W2 e W4
    # ════════════════════════════════════════════════════════
    for i in range(len(seq) - 3):
        p0, p1, p2, p3 = seq[i], seq[i+1], seq[i+2], seq[i+3]

        # Impulso de ALTA: L-H-L-H
        if p0[2]=="L" and p1[2]=="H" and p2[2]=="L" and p3[2]=="H":
            w1 = p1[1] - p0[1]
            w2 = p1[1] - p2[1]
            if w1 < min_wave: continue
            if not (0.30 <= w2/(w1+1e-9) <= 0.79): continue
            if p2[1] <= p0[1]: continue
            # Entrada na W2: preço perto do fundo de W2
            if abs(cur - p2[1])/(w1+1e-9) <= 0.12 and i+3 == len(seq)-1:
                s = _sig(cur, p2[1]*0.9993, "up", "EW_W2_LONG", p0[1], p1[1])
                if s: entries.append(s)
            # Onda 4
            if i+5 < len(seq):
                p4, p5 = seq[i+4], seq[i+5]
                if p4[2]=="L" and p5[2]=="H":
                    w3 = p3[1]-p2[1]; w4 = p3[1]-p4[1]
                    if w3 < min_wave: continue
                    if not (0.20 <= w4/(w3+1e-9) <= 0.55): continue
                    if p4[1] <= p2[1]: continue
                    if abs(cur-p4[1])/(w3+1e-9) <= 0.12 and i+4 == len(seq)-2:
                        s = _sig(cur, p4[1]*0.9993, "up", "EW_W4_LONG", p2[1], p3[1])
                        if s: entries.append(s)

        # Impulso de BAIXA: H-L-H-L
        if p0[2]=="H" and p1[2]=="L" and p2[2]=="H" and p3[2]=="L":
            w1 = p0[1]-p1[1]; w2 = p2[1]-p1[1]
            if w1 < min_wave: continue
            if not (0.30 <= w2/(w1+1e-9) <= 0.79): continue
            if p2[1] >= p0[1]: continue
            if abs(cur-p2[1])/(w1+1e-9) <= 0.12 and i+3 == len(seq)-1:
                s = _sig(cur, p2[1]*1.0007, "down", "EW_W2_SHORT", p0[1], p1[1])
                if s: entries.append(s)
            if i+5 < len(seq):
                p4, p5 = seq[i+4], seq[i+5]
                if p4[2]=="H" and p5[2]=="L":
                    w3 = p2[1]-p3[1]; w4 = p4[1]-p3[1]
                    if w3 < min_wave: continue
                    if not (0.20 <= w4/(w3+1e-9) <= 0.55): continue
                    if p4[1] >= p2[1]: continue
                    if abs(cur-p4[1])/(w3+1e-9) <= 0.12 and i+4 == len(seq)-2:
                        s = _sig(cur, p4[1]*1.0007, "down", "EW_W4_SHORT", p2[1], p3[1])
                        if s: entries.append(s)

    # ════════════════════════════════════════════════════════
    # 2. CORREÇÃO ABC com validação de 50% do IMPULSO MÃE
    #
    #  Estrutura buscada:
    #    IMPULSO: P_imp0 → P_imp1  (pernada mãe)
    #    CORREÇÃO:
    #      A: P_imp1 → pA  (contra o impulso)
    #      B: pA     → pB  (repique parcial)
    #      C: pB     → pC  (novo extremo, mín 80% de A)
    #
    #  Filtro obrigatório: (A+B_net) ≥ 50% do impulso mãe
    #  Stop de cada pernada: ORIGEM da pernada (não projetado)
    # ════════════════════════════════════════════════════════
    for i in range(1, len(seq) - 3):
        # Impulso mãe: pivô antes de A
        p_imp0 = seq[i-1]; p_imp1 = seq[i]
        pA1    = seq[i+1]; pB1    = seq[i+2]; pC1 = seq[i+3]

        # ── ABC após impulso de ALTA (correção de baixa) ──────
        # Impulso: L→H, Correção: H→L(A)→H(B)→L(C)
        if (p_imp0[2]=="L" and p_imp1[2]=="H" and
                pA1[2]=="L" and pB1[2]=="H" and pC1[2]=="L"):

            imp   = p_imp1[1] - p_imp0[1]   # tamanho do impulso mãe
            A_sz  = p_imp1[1] - pA1[1]
            B_sz  = pB1[1]    - pA1[1]
            C_sz  = pB1[1]    - pC1[1]
            if imp  < min_wave: continue
            if A_sz < min_wave * 0.3: continue

            # ── Filtro: correção total ≥ 50% do impulso ──────
            corr_total = p_imp1[1] - pC1[1]   # queda total do topo até C
            corr_pct   = corr_total / (imp + 1e-9)
            if corr_pct < 0.45: continue       # corrigiu menos de 45% — ignora

            # ── Validações Elliott ────────────────────────────
            if not (0.25 <= B_sz/(A_sz+1e-9) <= 0.88): continue
            if C_sz/(A_sz+1e-9) < 0.75: continue
            # C não deve ultrapassar o início de A (p_imp1)
            if pC1[1] >= p_imp1[1]: continue

            recent = (i+3 >= len(seq)-2)   # esta correção é recente?

            # ── a) FIM DE C → comprar para retomar alta ───────
            if abs(cur - pC1[1])/(A_sz+1e-9) <= tol_fim and recent:
                stop = min(pC1[1], p_imp0[1]) * 0.9992  # stop abaixo do mínimo de C
                s = _sig(cur, stop, "up", "EW_ABC_C_LONG",
                         p_imp1[1], pC1[1],
                         impulso_orig=p_imp0[1], impulso_fim=p_imp1[1],
                         corr_pct=corr_pct)
                if s: entries.append(s)

            # ── b) 50% DA PERNADA A (short dentro da correção) ─
            if A_sz > min_wave * 0.3 and i+1 <= len(seq)-1:
                if _in_50(cur, p_imp1[1], pA1[1]) and i+1 >= len(seq)-2:
                    stop = p_imp1[1] * 1.0008   # stop na origem de A (topo)
                    s = _sig(cur, stop, "down", "EW_ABC_LEG_A_SHORT",
                             p_imp1[1], pA1[1],
                             impulso_orig=p_imp0[1], impulso_fim=p_imp1[1],
                             corr_pct=corr_pct)
                    if s: entries.append(s)

            # ── c) 50% DA PERNADA B (long dentro da correção) ─
            if B_sz > min_wave * 0.3 and i+2 <= len(seq)-1:
                if _in_50(cur, pA1[1], pB1[1]) and i+2 >= len(seq)-2:
                    stop = pA1[1] * 0.9992   # stop na origem de B (fundo de A)
                    s = _sig(cur, stop, "up", "EW_ABC_LEG_B_LONG",
                             pA1[1], pB1[1],
                             impulso_orig=p_imp0[1], impulso_fim=p_imp1[1],
                             corr_pct=corr_pct)
                    if s: entries.append(s)

            # ── d) 50% DA PERNADA C (short dentro da correção) ─
            if C_sz > min_wave * 0.3 and i+3 <= len(seq)-1:
                if _in_50(cur, pB1[1], pC1[1]) and i+3 >= len(seq)-2:
                    stop = pB1[1] * 1.0008   # stop na origem de C (topo de B)
                    s = _sig(cur, stop, "down", "EW_ABC_LEG_C_SHORT",
                             pB1[1], pC1[1],
                             impulso_orig=p_imp0[1], impulso_fim=p_imp1[1],
                             corr_pct=corr_pct)
                    if s: entries.append(s)

        # ── ABC após impulso de BAIXA (correção de alta) ──────
        # Impulso: H→L, Correção: L→H(A)→L(B)→H(C)
        if (p_imp0[2]=="H" and p_imp1[2]=="L" and
                pA1[2]=="H" and pB1[2]=="L" and pC1[2]=="H"):

            imp   = p_imp0[1] - p_imp1[1]
            A_sz  = pA1[1]    - p_imp1[1]
            B_sz  = pA1[1]    - pB1[1]
            C_sz  = pC1[1]    - pB1[1]
            if imp  < min_wave: continue
            if A_sz < min_wave * 0.3: continue

            corr_total = pC1[1] - p_imp1[1]
            corr_pct   = corr_total / (imp + 1e-9)
            if corr_pct < 0.45: continue

            if not (0.25 <= B_sz/(A_sz+1e-9) <= 0.88): continue
            if C_sz/(A_sz+1e-9) < 0.75: continue
            if pC1[1] <= p_imp1[1]: continue

            recent = (i+3 >= len(seq)-2)

            # Fim de C → vender para retomar baixa
            if abs(cur - pC1[1])/(A_sz+1e-9) <= tol_fim and recent:
                stop = max(pC1[1], p_imp0[1]) * 1.0008
                s = _sig(cur, stop, "down", "EW_ABC_C_SHORT",
                         p_imp1[1], pC1[1],
                         impulso_orig=p_imp0[1], impulso_fim=p_imp1[1],
                         corr_pct=corr_pct)
                if s: entries.append(s)

            # 50% de A (long dentro)
            if A_sz > min_wave * 0.3 and i+1 <= len(seq)-1:
                if _in_50(cur, p_imp1[1], pA1[1]) and i+1 >= len(seq)-2:
                    stop = p_imp1[1] * 0.9992
                    s = _sig(cur, stop, "up", "EW_ABC_LEG_A_LONG",
                             p_imp1[1], pA1[1],
                             impulso_orig=p_imp0[1], impulso_fim=p_imp1[1],
                             corr_pct=corr_pct)
                    if s: entries.append(s)

            # 50% de B (short dentro)
            if B_sz > min_wave * 0.3 and i+2 <= len(seq)-1:
                if _in_50(cur, pA1[1], pB1[1]) and i+2 >= len(seq)-2:
                    stop = pA1[1] * 1.0008
                    s = _sig(cur, stop, "down", "EW_ABC_LEG_B_SHORT",
                             pA1[1], pB1[1],
                             impulso_orig=p_imp0[1], impulso_fim=p_imp1[1],
                             corr_pct=corr_pct)
                    if s: entries.append(s)

            # 50% de C (long dentro)
            if C_sz > min_wave * 0.3 and i+3 <= len(seq)-1:
                if _in_50(cur, pB1[1], pC1[1]) and i+3 >= len(seq)-2:
                    stop = pB1[1] * 0.9992
                    s = _sig(cur, stop, "up", "EW_ABC_LEG_C_LONG",
                             pB1[1], pC1[1],
                             impulso_orig=p_imp0[1], impulso_fim=p_imp1[1],
                             corr_pct=corr_pct)
                    if s: entries.append(s)

    # ════════════════════════════════════════════════════════
    # 3. TRIÂNGULO ABCDE — pernadas internas + fim de E
    #    Correção em 5 pernadas convergentes.
    #    Filtro: amplitude total ≥ 50% do impulso anterior.
    # ════════════════════════════════════════════════════════
    for i in range(len(seq) - 4):
        pts  = seq[i:i+5]
        tipos = [p[2] for p in pts]
        vals  = [p[1] for p in pts]
        alt_ok = all(tipos[j] != tipos[j+1] for j in range(4))
        if not alt_ok: continue
        szs  = [abs(vals[j+1]-vals[j]) for j in range(4)]
        conv = szs[0] > szs[1] > szs[2] > szs[3]
        if not conv: continue
        if szs[0] < min_wave: continue

        # Impulso mãe (pivô antes do triângulo)
        imp_sz = 0
        if i > 0:
            pm = seq[i-1]
            imp_sz = abs(pts[0][1] - pm[1])

        # Filtro 50% do impulso
        tri_amp = abs(vals[0] - vals[-1])
        if imp_sz > 0 and tri_amp / (imp_sz + 1e-9) < 0.40: continue

        last_pt = pts[-1]
        recent  = (i+4 >= len(seq)-2)

        # Fim de E
        if abs(cur - last_pt[1]) / (szs[0]+1e-9) <= 0.12 and recent:
            if last_pt[2] == "L":
                s = _sig(cur, last_pt[1]*0.9992, "up",
                         "EW_TRI_E_LONG", pts[0][1], last_pt[1])
                if s: entries.append(s)
            else:
                s = _sig(cur, last_pt[1]*1.0008, "down",
                         "EW_TRI_E_SHORT", pts[0][1], last_pt[1])
                if s: entries.append(s)

        # Pernadas internas do triângulo (50% de cada)
        for k in range(4):
            p_start = pts[k]; p_end = pts[k+1]
            sz_k    = szs[k]
            if sz_k < min_wave * 0.3: continue
            if k+1 != len(seq) - i - (len(seq)-i-5) - 1: pass  # só pernadas recentes
            if not (i+k+1 >= len(seq)-2): continue
            if _in_50(cur, p_start[1], p_end[1]):
                dir_k  = "down" if p_end[1] < p_start[1] else "up"
                stop_k = p_start[1] * (1.0008 if dir_k=="down" else 0.9992)
                tipo_k = f"EW_TRI_LEG_{chr(65+k)}_{'LONG' if dir_k=='up' else 'SHORT'}"
                s = _sig(cur, stop_k, dir_k, tipo_k, p_start[1], p_end[1])
                if s: entries.append(s)

    return entries

# ─────────────────────────────────────────────────────────────
#  ANALYZE SYMBOL  (único ponto de entrada do main_loop)
# ─────────────────────────────────────────────────────────────
def send_photo_telegram(img_bytes, caption, chat_id=None):
    """Envia imagem PNG (bytes) com legenda para o Telegram."""
    cid = chat_id or CHAT_ID
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data={"chat_id": cid, "caption": caption,
                  "parse_mode": "HTML"},
            files={"photo": ("chart.png", img_bytes, "image/png")},
            timeout=20)
    except Exception as e:
        print(f"[CHART] Erro sendPhoto: {e}")

def build_signal_chart(sym, candles, entry_dict, tf_label="M1"):
    """
    Gera gráfico de velas com Pillow (leve, sem matplotlib).
    Funciona no Termux/Android sem compilação.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        import io
    except ImportError:
        print("[CHART] Pillow nao instalado: pip install Pillow --break-system-packages")
        return None

    if not candles or len(candles) < 5:
        return None

    ep   = entry_dict["entry"]
    sp   = entry_dict["stop"]
    tp   = entry_dict["alvo"]
    dir_ = entry_dict["direcao"]
    tipo = entry_dict.get("tipo", "")
    rr   = entry_dict.get("rr", 0)
    ws   = entry_dict.get("wave_start")
    we   = entry_dict.get("wave_end")

    W, H   = 900, 500
    PAD_L  = 10; PAD_R = 165; PAD_T = 50; PAD_B = 30
    CW     = W - PAD_L - PAD_R
    CH     = H - PAD_T - PAD_B

    BG     = (13, 17, 23)
    C_UP   = (38, 166, 154);  C_DN  = (239, 83, 80)
    C_EP   = (66, 165, 245);  C_SL  = (239, 83, 80); C_TP2 = (102, 187, 106)
    C_WAV  = (255, 152, 0);   C_50  = (180, 180, 180)
    C_GRID = (30, 37, 50);    C_TXT = (200, 200, 200); C_PIV = (255, 152, 0)

    c_all = candles[-55:] if len(candles) > 55 else candles
    N = len(c_all)

    all_p = ([x["high"] for x in c_all] + [x["low"] for x in c_all]
             + [ep, sp, tp] + ([ws] if ws else []) + ([we] if we else []))
    p_min = min(all_p); p_max = max(all_p)
    p_rng = (p_max - p_min) or 1.0
    mg    = p_rng * 0.10
    p_min -= mg; p_max += mg; p_rng = p_max - p_min

    def py(price):
        return int(PAD_T + CH * (1.0 - (price - p_min) / p_rng))

    def px(i):
        return int(PAD_L + (i + 0.5) * CW / (N + 4))

    cw = max(2, int(CW / (N + 4) * 0.6))

    img  = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    try:
        paths = ["/system/fonts/DroidSans.ttf",
                 "/system/fonts/Roboto-Regular.ttf",
                 "/data/fonts/Roboto-Regular.ttf"]
        bpaths = ["/system/fonts/DroidSans-Bold.ttf",
                  "/system/fonts/Roboto-Bold.ttf"]
        fsm = None
        for p in paths:
            try: fsm = ImageFont.truetype(p, 12); break
            except: pass
        fbig = None
        for p in bpaths:
            try: fbig = ImageFont.truetype(p, 15); break
            except: pass
        if not fsm:  fsm  = ImageFont.load_default()
        if not fbig: fbig = fsm
    except Exception:
        fsm = fbig = ImageFont.load_default()

    # Grid
    for frac in (0.25, 0.5, 0.75):
        yg = int(PAD_T + CH * frac)
        draw.line([(PAD_L, yg), (PAD_L+CW, yg)], fill=C_GRID, width=1)

    # Faixas risco/lucro com overlay RGBA
    def _fill_band(y1, y2, color_rgb, alpha):
        ov = Image.new("RGBA", (W, H), (0,0,0,0))
        d2 = ImageDraw.Draw(ov)
        d2.rectangle([(PAD_L, min(y1,y2)), (PAD_L+CW, max(y1,y2))],
                     fill=color_rgb + (alpha,))
        return Image.alpha_composite(img.convert("RGBA"), ov).convert("RGB")

    img = _fill_band(py(ep), py(sp), C_SL,  35)
    img = _fill_band(py(ep), py(tp), C_TP2, 22)
    draw = ImageDraw.Draw(img)

    # Nivel 50% da pernada
    if ws is not None and we is not None:
        meio = (ws + we) / 2
        for lv, col, lbl in [(ws, C_WAV, f"Orig {ws:,.2f}"),
                              (we, C_WAV, f"Ext  {we:,.2f}"),
                              (meio, C_50, "50%")]:
            yv = py(lv)
            for xd in range(PAD_L, PAD_L+CW, 8):
                draw.line([(xd, yv), (min(xd+4, PAD_L+CW), yv)], fill=col, width=1)
            draw.text((PAD_L+CW+4, yv-6), lbl, fill=col, font=fsm)

    # Pivots
    try:
        hp, lp = find_pivots(c_all, SWING_M1)
        ph = {i for i,_ in hp}; pl = {i for i,_ in lp}
    except Exception:
        ph = pl = set()

    # Candles
    for i, cv in enumerate(c_all):
        o_, h_, l_, cl = cv["open"], cv["high"], cv["low"], cv["close"]
        col = C_UP if cl >= o_ else C_DN
        x   = px(i)
        draw.line([(x, py(h_)), (x, py(l_))], fill=col, width=1)
        yt = min(py(o_), py(cl)); yb = max(py(o_), py(cl))
        draw.rectangle([(x-cw, yt), (x+cw, max(yb, yt+1))], fill=col)
        if i in ph:
            yp = py(h_)-9
            draw.polygon([(x,yp),(x-5,yp+7),(x+5,yp+7)], fill=C_PIV)
        if i in pl:
            yp = py(l_)+9
            draw.polygon([(x,yp),(x-5,yp-7),(x+5,yp-7)], fill=C_PIV)

    # Linhas SL/EP/TP
    for price, col, w2 in [(sp,C_SL,2),(ep,C_EP,2),(tp,C_TP2,2)]:
        draw.line([(PAD_L, py(price)), (PAD_L+CW, py(price))], fill=col, width=w2)

    # Labels direita
    lx = PAD_L+CW+6
    draw.text((lx, py(tp)-8), f"TP {tp:,.4f}", fill=C_TP2, font=fsm)
    draw.text((lx, py(ep)-8), f"EP {ep:,.4f}", fill=C_EP,  font=fsm)
    draw.text((lx, py(sp)-8), f"SL {sp:,.4f}", fill=C_SL,  font=fsm)

    # Seta direcao
    ax2 = px(N-1) + cw + 18
    if dir_ == "up":
        draw.line([(ax2, py(ep)+8),(ax2, py(tp)+12)], fill=C_TP2, width=3)
        draw.polygon([(ax2,py(tp)+12),(ax2-6,py(tp)+22),(ax2+6,py(tp)+22)],
                     fill=C_TP2)
    else:
        draw.line([(ax2, py(ep)-8),(ax2, py(tp)-12)], fill=C_SL, width=3)
        draw.polygon([(ax2,py(tp)-12),(ax2-6,py(tp)-22),(ax2+6,py(tp)-22)],
                     fill=C_SL)

    # Titulo
    acao = "LONG ↑" if dir_ == "up" else "SHORT ↓"
    tcor = C_TP2 if dir_ == "up" else C_SL
    draw.text((PAD_L+4, 12), f"{sym}  {tf_label}  {acao}  |  RR 1:{rr}  |  {tipo}",
              fill=tcor, font=fbig)

    # Caixa resumo
    risco_v = abs(ep-sp); lucro_v = abs(tp-ep)
    lines = [f"Entrada: {ep:,.4f}",
             f"Stop:    {sp:,.4f}  (-{risco_v:,.4f})",
             f"Alvo:    {tp:,.4f}  (+{lucro_v:,.4f})",
             f"RR:      1:{rr}",
             f"Setup:   {tipo}"]
    bx=W-8; by=10; bw=222; blh=16
    bhb = len(lines)*blh+14
    ov2 = Image.new("RGBA",(W,H),(0,0,0,0))
    d3  = ImageDraw.Draw(ov2)
    d3.rectangle([(bx-bw,by),(bx,by+bhb)], fill=(26,31,46,210))
    img = Image.alpha_composite(img.convert("RGBA"), ov2).convert("RGB")
    draw = ImageDraw.Draw(img)
    for li, ln in enumerate(lines):
        draw.text((bx-bw+6, by+6+li*blh), ln, fill=C_TXT, font=fsm)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf.read()


def _place_order(sym, entry_dict, data, ts, modo):
    """Coloca uma ordem individual e retorna sinal para memory."""
    ep   = entry_dict["entry"]
    sp   = entry_dict["stop"]
    tp   = entry_dict["alvo"]
    dir_ = entry_dict["direcao"]
    tipo = entry_dict.get("tipo", "")
    risco = abs(ep - sp)
    if risco == 0: return None
    rr = round(abs(tp - ep) / risco, 1)
    if rr < MIN_RR: return None

    side_by = "Buy" if dir_ == "up" else "Sell"
    res = order_futures(sym, side_by, SYMBOLS[sym]["qty"], sl=sp, tp=tp)
    bi  = ""
    sinal = {"id": len(memory["signals"])+1, "symbol": sym,
             "direcao": dir_, "entrada": ep, "stop": sp, "alvo": tp,
             "risco": risco, "rr": rr, "data": ts,
             "status": "aberto", "resultado": None, "order_id": None, "tipo": tipo}
    if res and res.get("ok"):
        sinal["order_id"] = res["order_id"]
        bi = f"🏦 [{modo}] ✅ <code>{res['order_id']}</code>"
    elif res:
        bi = f"❌ {res.get('error','?')}"
    memory["signals"].append(sinal)
    if len(memory["signals"]) > 200: memory["signals"] = memory["signals"][-200:]
    return sinal, bi

def fire_signal(data):
    sym  = data["symbol"]
    ts   = br_now()
    modo = "TESTNET 🟡" if BYBIT_MODE == "testnet" else "REAL 🔴"
    m1t  = data.get("m1_trend","?").upper()
    m5t  = data.get("m5_trend","?").upper()

    def _fmt_sinal(emoji, acao, entry_dict, bi_line):
        ep  = entry_dict["entry"]; sp = entry_dict["stop"]
        tp  = entry_dict["alvo"];  rr = entry_dict["rr"]
        tipo = entry_dict.get("tipo","")
        ws  = entry_dict.get("wave_start"); we = entry_dict.get("wave_end")
        perna = f"\n📏 ${ws:,.4f}→${we:,.4f}" if ws and we else ""
        return (f"{emoji} <b>{acao}</b> — {sym}\n"
                f"📌 <code>{tipo}</code>\n"
                f"💰 ${ep:,.4f}  🛑 ${sp:,.4f}  🎯 ${tp:,.4f}\n"
                f"📐 RR 1:{rr}{perna}\n"
                f"🕐 M1:{m1t} M5:{m5t}\n"
                f"{bi_line}")

    def _enviar_com_chart(caption, entry_dict, candles, tf_label):
        """Tenta enviar com gráfico; fallback para texto puro."""
        chart = build_signal_chart(sym, candles, entry_dict, tf_label)
        if chart:
            send_photo_telegram(chart, caption)
        else:
            send_telegram(caption)

    # ── BOTH: ordens simultâneas (compressão detectada) ──────
    both = data.get("both_m1") or data.get("both_m5")
    if both:
        long_, short_ = both
        tag = "M1" if data.get("both_m1") else "M5"
        if long_ and short_:
            r_l = _place_order(sym, long_,  data, ts, modo)
            r_s = _place_order(sym, short_, data, ts, modo)
            bl  = r_l[1] if r_l else "❌"
            bs  = r_s[1] if r_s else "❌"
            ep  = long_["entry"]
            sup = long_.get("wave_start", 0)
            res = long_.get("wave_end", 0)
            caption = (
                f"⚡ <b>BOTH [{tag}]</b> — {sym}\n"
                f"📐 Compressão — long + short simultâneos\n"
                f"💰 Entrada: <b>${ep:,.4f}</b>\n"
                f"📈 Long  SL:${long_['stop']:,.4f}  TP:${long_['alvo']:,.4f}  RR 1:{long_['rr']}\n"
                f"📉 Short SL:${short_['stop']:,.4f}  TP:${short_['alvo']:,.4f}  RR 1:{short_['rr']}\n"
                f"🏗️ Sup:${sup:,.4f}  Res:${res:,.4f}\n"
                f"{bl}\n{bs}\n"
                f"⏰ {ts} (UTC-3)" + ASSINATURA)
            # Gráfico para o lado longo (referência visual)
            tf_raw = "1m" if tag == "M1" else "5m"
            candles_chart = get_candles(sym, tf_raw, 80)
            _enviar_com_chart(caption, long_, candles_chart or [], tag)
            save_memory()
            return

    # ── DIRECIONAL: entrada única ─────────────────────────────
    entry = data.get("entry")
    if not entry: return
    dir_  = entry["direcao"]
    emoji = "✅" if dir_ == "up" else "🔴"
    acao  = "COMPRA" if dir_ == "up" else "VENDA"
    r = _place_order(sym, entry, data, ts, modo)
    if not r: return
    sinal, bi_line = r
    caption = (_fmt_sinal(emoji, acao, entry, bi_line) +
               f"\n⏰ {ts} (UTC-3)" + ASSINATURA)
    # Detecta timeframe pelo tipo do sinal
    tipo = entry.get("tipo", "")
    tf_raw    = "1m" if "M1" in tipo else "5m"
    tf_label  = "M1" if "M1" in tipo else "M5"
    candles_chart = get_candles(sym, tf_raw, 80)
    _enviar_com_chart(caption, entry, candles_chart or [], tf_label)
    save_memory()

def check_signals(price_map):
    ab = [s for s in memory.get("signals", []) if s["status"] == "aberto"]
    if not ab: return
    alt = False
    for s in ab:
        p   = price_map.get(s.get("symbol", "BTCUSDT"))
        if not p: continue
        sym  = s.get("symbol", ""); ts = br_now("%d/%m/%Y %H:%M")
        qty  = SYMBOLS.get(sym, {}).get("qty", RISCO_USDT)
        dir_ = s.get("direcao", "up")
        hit_tp = (dir_=="up" and p>=s["alvo"])   or (dir_=="down" and p<=s["alvo"])
        hit_sl = (dir_=="up" and p<=s["stop"])   or (dir_=="down" and p>=s["stop"])
        if hit_tp:
            usd, brl = calc_pnl_brl(sym, s["entrada"], s["alvo"], qty, dir_)
            s["status"]="win"; s["resultado"]=f"+{s['rr']}R"
            s["pnl_usd"]=usd; s["pnl_brl"]=brl; s["fechamento"]=ts; alt=True
            pontos = abs(s["alvo"] - s["entrada"])
            send_telegram(
                f"🏆 <b>TAKE PROFIT!</b> {sym} ✅\n"
                f"📐 {pontos:,.4f} pts × {qty} lote\n"
                f"💵 +${usd:.4f}  💚 <b>+R${brl:.2f}</b>\n"
                f"📊 RR {s['resultado']}")
        elif hit_sl:
            usd, brl = calc_pnl_brl(sym, s["entrada"], s["stop"], qty, dir_)
            s["status"]="loss"; s["resultado"]="-1R"
            s["pnl_usd"]=-abs(usd); s["pnl_brl"]=-abs(brl); s["fechamento"]=ts; alt=True
            pontos = abs(s["stop"] - s["entrada"])
            send_telegram(
                f"🛑 <b>STOP LOSS</b> {sym} ❌\n"
                f"📐 {pontos:,.4f} pts × {qty} lote\n"
                f"💵 -${abs(usd):.4f}  🔴 <b>-R${abs(brl):.2f}</b>")
    if alt: save_memory()

# ═══════════════════════════════════════════════════════════════
#  COMANDOS
# ═══════════════════════════════════════════════════════════════
def _ok_msg(res, descricao):
    if res and res.get("ok"):
        return f"✅ {descricao}\n🆔 <code>{res.get('order_id','')}</code>"
    return f"❌ Erro: {res.get('error','?') if res else 'sem resposta'}"


_BRL_CACHE = {"rate": 5.70, "ts": 0}

def get_usd_brl():
    global _BRL_CACHE
    if time.time() - _BRL_CACHE["ts"] < 300:
        return _BRL_CACHE["rate"]
    try:
        r = requests.get("https://economia.awesomeapi.com.br/json/last/USD-BRL", timeout=8)
        rate = float(r.json()["USDBRL"]["bid"])
        _BRL_CACHE = {"rate": rate, "ts": time.time()}
        return rate
    except:
        return _BRL_CACHE["rate"]

def br_now(fmt="%d/%m/%Y %H:%M"):
    """Hora atual UTC-3 (Brasilia)."""
    return datetime.now(timezone(timedelta(hours=-3))).strftime(fmt)

_usd_brl_cache = {"rate": None, "ts": 0}
def usd_to_brl(usd):
    """Converte USD para BRL com cotação em tempo real (cache de 10min)."""
    now = time.time()
    if _usd_brl_cache["rate"] is None or now - _usd_brl_cache["ts"] > 600:
        try:
            r = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=5)
            rate = r.json()["rates"]["BRL"]
            _usd_brl_cache["rate"] = rate
            _usd_brl_cache["ts"]   = now
        except:
            _usd_brl_cache["rate"] = _usd_brl_cache["rate"] or 5.70
    return usd * _usd_brl_cache["rate"]

# ═══════════════════════════════════════════════════════════════
#  CÁLCULO DE LUCRO/PREJUÍZO  (pontos × lote% = USD → BRL)
#  Lógica: pontos = |saida - entrada|
#          lucro_usd = pontos × qty   (1 ponto = $1 por unidade)
#          lucro_brl = lucro_usd × cotação
# ═══════════════════════════════════════════════════════════════
def calc_pnl_brl(symbol, entrada, saida, qty, direcao):
    """Retorna (lucro_usd, lucro_brl). Negativo = prejuízo."""
    pontos    = abs(saida - entrada)
    lucro_usd = pontos * qty
    ganhou    = (direcao == "up"   and saida > entrada) or \
                (direcao != "up"   and saida < entrada)
    if not ganhou:
        lucro_usd = -lucro_usd
    rate      = get_usd_brl()
    lucro_brl = lucro_usd * rate
    return round(lucro_usd, 4), round(lucro_brl, 2)

# ═══════════════════════════════════════════════════════════════
#  NOTÍCIAS GLOBAIS — Bloomberg / Reuters / CoinDesk / Investing
# ═══════════════════════════════════════════════════════════════
# ─── FONTES RSS — Bloomberg (via FT proxy), Reuters, CoinDesk,
#     CoinTelegraph, Decrypt, Financial Times, WSJ Markets ─────
NEWS_SOURCES = [
    ("Reuters Markets",  "https://feeds.reuters.com/reuters/businessNews",              "🗞️"),
    ("Reuters Cripto",   "https://feeds.reuters.com/reuters/technologyNews",            "🗞️"),
    ("CoinDesk",         "https://www.coindesk.com/arc/outboundfeeds/rss/",            "🪙"),
    ("CoinTelegraph",    "https://cointelegraph.com/rss",                              "📡"),
    ("Decrypt",          "https://decrypt.co/feed",                                    "🔓"),
    ("FT Markets",       "https://www.ft.com/markets?format=rss",                     "📰"),
    ("Investing Cripto", "https://www.investing.com/rss/news_301.rss",                "📊"),
    ("Investing Macro",  "https://www.investing.com/rss/news_14.rss",                 "🏦"),
    ("Yahoo Finance",    "https://finance.yahoo.com/news/rssindex",                   "💹"),
    ("The Block",        "https://www.theblock.co/rss.xml",                           "🧱"),
]

# Palavras-chave em 3 níveis de urgência
_URGENT_KW = [
    "crash","hack","hacked","exploit","ban","banned","bankrupt","collapse",
    "emergency","seized","arrested","fraud","scam","rug pull","liquidation",
    "war","sanction","default","circuit breaker","flash crash","black swan",
    "rate hike","rate cut","fomc","emergency meeting","fed cut","fed hike",
    "sec charges","doj","cftc","lawsuit","indictment",
]
_HIGH_KW = [
    "bitcoin etf","ethereum etf","blackrock","fidelity","grayscale","spot etf",
    "jpmorgan","goldman sachs","morgan stanley","citadel","jane street",
    "treasury","yield","inflation","cpi","ppi","gdp","powell","lagarde",
    "binance","coinbase","kraken","bybit","okx","huobi","ftx","celsius",
    "usdt","tether","usdc","stablecoin","defi","defi protocol",
    "layer 2","rollup","zkp","ethereum upgrade","bitcoin halving",
    "whale","large transfer","fund flow","on-chain","open interest",
]
_RELEVANT_KW = [
    "bitcoin","btc","ethereum","eth","solana","sol","xrp","ripple","bnb",
    "crypto","blockchain","fed","federal reserve","interest rate","bank",
    "doge","dogecoin","ada","cardano","avax","avalanche","dot","polkadot",
    "link","chainlink","ltc","litecoin","atom","cosmos","near","apt","aptos",
    "sui","op","optimism","arb","arbitrum","trx","tron","ton","pepe",
    "nasdaq","sp500","dollar","dxy","macro","recession","rate","ecb",
]

_news_cache        = {"items": [], "ts": 0}
_news_urgent_sent  = set()   # títulos já enviados como urgente
NEWS_INTERVAL      = 1800    # cache de 30 min (antes era 1h)
NEWS_URGENT_CHECK  = 600     # checa urgentes a cada 10 min

def _score_news(title):
    """Retorna (score, urgente). Score: 3=urgente, 2=alto, 1=relevante, 0=ignorar."""
    t = title.lower()
    if any(kw in t for kw in _URGENT_KW):  return 3, True
    if any(kw in t for kw in _HIGH_KW):    return 2, False
    if any(kw in t for kw in _RELEVANT_KW):return 1, False
    return 0, False

def _parse_rss(url, source, emoji):
    try:
        import xml.etree.ElementTree as ET
        r = requests.get(url, timeout=12,
                         headers={"User-Agent": "Mozilla/5.0 TronForexBot/3.0"})
        r.raise_for_status()
        ns   = {"media": "http://search.yahoo.com/mrss/"}
        root = ET.fromstring(r.content)
        items = []
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link")  or "").strip()
            date  = (item.findtext("pubDate") or "").strip()
            desc  = (item.findtext("description") or "").strip()[:120]
            img   = ""
            for tag in ["media:thumbnail", "media:content"]:
                mt = item.find(tag, ns)
                if mt is not None:
                    img = mt.get("url",""); break
            if not img:
                enc = item.find("enclosure")
                if enc is not None and "image" in enc.get("type",""):
                    img = enc.get("url","")
            score, urgent = _score_news(title)
            if title and link and score > 0:
                items.append({"title": title, "link": link, "date": date,
                              "desc": desc, "source": source, "emoji": emoji,
                              "img": img, "score": score, "urgent": urgent})
        return items
    except Exception as e:
        print(f"[NEWS] {source}: {e}"); return []

def fetch_news(force=False):
    """Busca e classifica notícias — cache 30 min."""
    global _news_cache
    if not force and time.time() - _news_cache["ts"] < NEWS_INTERVAL:
        return _news_cache["items"]
    all_items = []
    threads = []
    results = {}
    def _fetch(src, url, em):
        results[(src,url,em)] = _parse_rss(url, src, em)
    for s in NEWS_SOURCES:
        t = threading.Thread(target=_fetch, args=s, daemon=True)
        threads.append(t); t.start()
    for t in threads: t.join(timeout=15)
    for v in results.values(): all_items.extend(v)
    # Deduplicar e ordenar por score desc
    seen, unique = set(), []
    for it in sorted(all_items, key=lambda x: x["score"], reverse=True):
        k = it["title"][:60].lower()
        if k not in seen:
            seen.add(k); unique.append(it)
    _news_cache = {"items": unique[:20], "ts": time.time()}
    return _news_cache["items"]

def check_urgent_news():
    """Verifica e envia notícias urgentes automaticamente."""
    global _news_urgent_sent
    items = fetch_news()
    for it in items:
        if not it.get("urgent"): continue
        key = it["title"][:60].lower()
        if key in _news_urgent_sent: continue
        _news_urgent_sent.add(key)
        caption = (
            f"🚨 <b>URGENTE — {it['source']}</b>\n"
            f"{it['title']}\n"
            f"{'📝 ' + it['desc'] if it.get('desc') else ''}\n"
            f"🔗 <a href=\"{it['link']}\">Leia mais</a>\n"
            f"⏰ {br_now('%d/%m %H:%M')} (UTC-3)"
        )
        if it.get("img"):
            send_photo_url_telegram(it["img"], caption)
        else:
            send_telegram(caption)

def send_photo_url_telegram(photo_url, caption, chat_id=None):
    """Envia foto por URL diretamente ao Telegram."""
    cid = chat_id or CHAT_ID
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            data={"chat_id": cid, "photo": photo_url,
                  "caption": caption, "parse_mode": "HTML"},
            timeout=15)
        if r.json().get("ok"): return
        # Fallback: foto falhou (URL bloqueada), envia texto
        send_telegram(caption, cid)
    except Exception as e:
        print(f"[TG PHOTO URL] {e}")
        send_telegram(caption, cid)

def send_news_telegram(chat_id=None, filtro=None):
    """Envia bloco de notícias via Telegram com link e imagem.
    filtro: None=todas | 'urgente' | 'alta' | símbolo ex: 'btc'
    """
    news = fetch_news(force=True)
    if filtro:
        f = filtro.lower()
        if f in ("urgente","urgent"):
            news = [n for n in news if n["score"] == 3]
        elif f in ("alta","high"):
            news = [n for n in news if n["score"] >= 2]
        else:
            news = [n for n in news if f in n["title"].lower()]
    if not news:
        send_telegram("📰 Nenhuma notícia relevante no momento.", chat_id)
        return
    rate = get_usd_brl()
    urgentes = sum(1 for n in news if n["score"] == 3)
    altas    = sum(1 for n in news if n["score"] == 2)
    send_telegram(
        f"📰 <b>Notícias Globais — Mercados & Cripto</b>\n"
        f"🚨 {urgentes} urgentes  ⚡ {altas} alta prioridade\n"
        f"💱 USD/BRL R${rate:.2f} | ⏰ {br_now('%d/%m %H:%M')} (UTC-3)\n"
        f"{'─'*28}", chat_id)
    icons = {3:"🚨", 2:"⚡", 1:"📰"}
    for it in news[:12]:
        prio  = icons.get(it["score"],"📰")
        desc_line = f"\n<i>{it['desc'][:100]}</i>" if it.get("desc") else ""
        caption = (
            f"{prio} {it['emoji']} <b>{it['source']}</b>\n"
            f"<b>{it['title']}</b>"
            f"{desc_line}\n"
            f"🔗 <a href=\"{it['link']}\">Leia mais</a>"
        )
        if it.get("img"):
            send_photo_url_telegram(it["img"], caption, chat_id)
        else:
            send_telegram(caption, chat_id)
        time.sleep(0.5)

ASSINATURA = "\n\n<b>Tron Forex Bot</b> - Dev: Jon Padilha"

def handle_command(text, chat_id):
    parts = text.strip().split()
    cmd   = parts[0].lower()

    # ── HELP ────────────────────────────────────────────────
    if cmd in ("/help", "/start", "/commands"):
        modo = "TESTNET 🟡" if BYBIT_MODE == "testnet" else "REAL 🔴"
        send_telegram(
            f"🤖 <b>TRON Bot v3</b> [{modo}]\n\n"
            "📊 <b>MERCADO:</b>\n"
            "/status · /analise · /performance\n"
            "/relatorio · /hoje · /saldo · /posicoes\n\n"
            "📰 <b>NOTÍCIAS:</b>\n"
            "/noticias         → todas (Reuters/CoinDesk/etc)\n"
            "/noticias btc     → filtrar por símbolo\n"
            "/noticias urgente → só urgentes\n"
            "/urgente          → alertas críticos\n\n"
            "🔬 <b>MICRO (M1+M5+Both):</b>\n"
            "/micro · /micro BTC\n"
            "/executar BTC up m1   → swing M1\n"
            "/executar BTC down m5 → Elliott M5\n"
            "/both BTC m1          → long+short simultâneos\n\n"
            "💵 <b>SPOT (a mercado):</b>\n"
            "/comprar BTC 0.001\n"
            "/vender BTC 0.001\n"
            "/vendertudo BTC\n\n"
            "⚡ <b>FUTUROS (a mercado):</b>\n"
            "/long BTC 0.001\n"
            "/short BTC 0.001\n"
            "/long BTC 0.001 sl=60000 tp=65000\n\n"
            "📌 <b>LIMITADAS (agenda no preco):</b>\n"
            "/ls BTC 0.001 62000     comprar spot\n"
            "/lsv BTC 0.001 65000    vender spot\n"
            "/ll BTC 0.001 62000     long futuros\n"
            "/lsh BTC 0.001 65000    short futuros\n\n"
            "🔒 <b>FECHAR / CANCELAR:</b>\n"
            "/fechar BTC\n"
            "/fechar tudo\n"
            "/cancelar BTC\n\n"
            "📸 Envie print para analise IA\n"
            "📐 Stop & Alvo TÉCNICOS (topos/fundos)", chat_id)

    # ── MICRO (M1 swing + M5 Elliott + Both) ────────────────
    elif cmd == "/micro":
        sym_list = [_parse_sym(parts[1])] if len(parts) > 1 else list(SYMBOLS.keys())
        send_telegram(f"🔬 Rastreando M1+M5+Both em {len(sym_list)} símbolo(s)...", chat_id)
        encontrados = 0
        _LABELS = {
            "EW_W2_LONG":"Onda 2 ↗","EW_W2_SHORT":"Onda 2 ↘",
            "EW_W4_LONG":"Onda 4 ↗","EW_W4_SHORT":"Onda 4 ↘",
            "EW_ABC_C_LONG":"Fim Onda C → retoma alta ↗",
            "EW_ABC_C_SHORT":"Fim Onda C → retoma baixa ↘",
            "EW_TRI_E_LONG":"Fim triângulo E ↗","EW_TRI_E_SHORT":"Fim triângulo E ↘",
            "EW_TRI_LEG_A_LONG":"Triângulo perna A ↗","EW_TRI_LEG_A_SHORT":"Triângulo perna A ↘",
            "EW_TRI_LEG_B_LONG":"Triângulo perna B ↗","EW_TRI_LEG_B_SHORT":"Triângulo perna B ↘",
            "EW_TRI_LEG_C_LONG":"Triângulo perna C ↗","EW_TRI_LEG_C_SHORT":"Triângulo perna C ↘",
            "EW_TRI_LEG_D_LONG":"Triângulo perna D ↗","EW_TRI_LEG_D_SHORT":"Triângulo perna D ↘",
            "EW_ABC_LEG_A_LONG":"Pernada A ↗","EW_ABC_LEG_A_SHORT":"Pernada A ↘",
            "EW_ABC_LEG_B_LONG":"Pernada B ↗","EW_ABC_LEG_B_SHORT":"Pernada B ↘",
            "EW_ABC_LEG_C_LONG":"Pernada C ↗","EW_ABC_LEG_C_SHORT":"Pernada C ↘",
            "M1_SWING_LONG":"Swing 50% ↗","M1_SWING_SHORT":"Swing 50% ↘",
            "M1_W_LONG":"Padrão W (fundo duplo) ↗",
            "M1_M_SHORT":"Padrão M (topo duplo) ↘",
            "M1_PULLBACK_LONG":"Micro pullback ↗",
            "M1_PULLBACK_SHORT":"Micro pullback ↘",
            # Novos — alto RR
            "HTF_H1_LONG":"🏗️ Estrutura H1 ↗","HTF_H1_SHORT":"🏗️ Estrutura H1 ↘",
            "HTF_H4_LONG":"🏗️ Estrutura H4 ↗","HTF_H4_SHORT":"🏗️ Estrutura H4 ↘",
            "FIB_1.272_LONG":"📐 Fib 1.272 ↗","FIB_1.272_SHORT":"📐 Fib 1.272 ↘",
            "FIB_1.618_LONG":"📐 Fib 1.618 ↗","FIB_1.618_SHORT":"📐 Fib 1.618 ↘",
            "FIB_2.000_LONG":"📐 Fib 2.000 ↗","FIB_2.000_SHORT":"📐 Fib 2.000 ↘",
            "FIB_2.618_LONG":"📐 Fib 2.618 ↗","FIB_2.618_SHORT":"📐 Fib 2.618 ↘",
            "OB_BULL":"🟩 Order Block Bull ↗","OB_BEAR":"🟥 Order Block Bear ↘",
            "FVG_BULL":"⬜ FVG Bull ↗","FVG_BEAR":"⬜ FVG Bear ↘",
            "BOS_BULL":"🔷 BOS reteste ↗","BOS_BEAR":"🔶 BOS reteste ↘",
        }
        for sym in sym_list:
            data = analyze_symbol(sym)
            if not data: continue
            m1t = data["m1_trend"].upper(); m5t = data["m5_trend"].upper()
            price = data["price"]; sym_short = sym.replace("USDT","")
            linhas = [f"🔬 <b>{sym}</b> ${price:,.4f}  M1:{m1t} M5:{m5t}"]

            # ── Both (ordens simultâneas) ─────────────────
            for tag, both in [("M1", data.get("both_m1")), ("M5", data.get("both_m5"))]:
                if not both: continue
                l_, s_ = both
                if l_ and s_:
                    ws = l_.get("wave_start",0); we = l_.get("wave_end",0)
                    linhas.append(
                        f"⚡ <b>BOTH [{tag}]</b> — Compressão\n"
                        f"  Sup:${ws:,.4f}  Res:${we:,.4f}\n"
                        f"  📈 Long  SL:${l_['stop']:,.4f} TP:${l_['alvo']:,.4f} RR 1:{l_['rr']}\n"
                        f"  📉 Short SL:${s_['stop']:,.4f} TP:${s_['alvo']:,.4f} RR 1:{s_['rr']}\n"
                        f"  ▶️ <code>/both {sym_short} {tag.lower()}</code>")

            # ── M1 todas as pernadas ──────────────────────
            for e in data.get("entries_m1", []):
                dir_ = e["direcao"]; arrow = "📈" if dir_ == "up" else "📉"
                label = _LABELS.get(e["tipo"], e["tipo"])
                extra = ""
                if e.get("pescoço"):
                    extra = f"\n  Pescoço: ${e['pescoço']:,.4f}"
                elif e.get("wave_start") and e.get("wave_end"):
                    extra = f"\n  Pernada: ${e['wave_start']:,.4f}→${e['wave_end']:,.4f}"
                linhas.append(
                    f"{arrow} <b>M1</b> — {label}\n"
                    f"  Entrada: ${e['entry']:,.4f}\n"
                    f"  Stop: ${e['stop']:,.4f}  Alvo: ${e['alvo']:,.4f} | RR 1:{e['rr']}"
                    f"{extra}\n"
                    f"  ▶️ <code>/executar {sym_short} {dir_} m1</code>")

            # ── M5 Elliott ────────────────────────────────
            for e in data.get("entries_m5", []):
                dir_ = e["direcao"]; arrow = "📈" if dir_ == "up" else "📉"
                label = _LABELS.get(e["tipo"], e["tipo"])
                corr  = f" [{e['corr_pct']*100:.0f}%corr]" if e.get("corr_pct") else ""
                imp_l = ""
                if e.get("impulso_orig") and e.get("impulso_fim"):
                    imp_l = f"\n  Impulso mãe: ${e['impulso_orig']:,.4f}→${e['impulso_fim']:,.4f}"
                linhas.append(
                    f"{arrow} <b>M5</b> — {label}{corr}\n"
                    f"  Entrada: ${e['entry']:,.4f}\n"
                    f"  Stop: ${e['stop']:,.4f}  Alvo: ${e['alvo']:,.4f} | RR 1:{e['rr']}"
                    f"{imp_l}\n"
                    f"  ▶️ <code>/executar {sym_short} {dir_} m5</code>")

            # ── HTF (H1/H4/Fib/OB/BOS) — alvos maiores ──────
            for e in data.get("entries_htf_all", []):
                dir_ = e["direcao"]; arrow = "🚀" if dir_ == "up" else "💣"
                label = _LABELS.get(e["tipo"], e["tipo"])
                fib_l = f" [Fib {e['fib_level']:.3f}]" if e.get("fib_level") else ""
                linhas.append(
                    f"{arrow} <b>HTF</b> — {label}{fib_l}\n"
                    f"  Entrada: ${e['entry']:,.4f}\n"
                    f"  Stop M1: ${e['stop']:,.4f}  Alvo: ${e['alvo']:,.4f} | RR 1:{e['rr']}\n"
                    f"  ▶️ <code>/long {sym_short} {SYMBOLS[sym]['qty']} sl={e['stop']:.4f} tp={e['alvo']:.4f}</code>")

            if len(linhas) > 1:
                encontrados += 1
                send_telegram("\n".join(linhas) + ASSINATURA, chat_id)
            elif len(sym_list) == 1:
                send_telegram(f"🔬 {sym} ${price:,.4f}\n⏳ Nenhum setup no momento.", chat_id)

        if encontrados == 0 and len(sym_list) > 1:
            send_telegram("⏳ Nenhum setup encontrado.", chat_id)
        elif encontrados > 0 and len(sym_list) > 1:
            send_telegram(f"🔬 {encontrados} símbolo(s) com setup.", chat_id)

    # ── BOTH — executa ordens simultâneas ───────────────────
    # Uso: /both BTC m1   ou   /both ETH m5
    elif cmd == "/both":
        if len(parts) < 2:
            send_telegram("Uso: /both BTC [m1|m5]", chat_id); return
        sym  = _parse_sym(parts[1])
        tf_  = parts[2].lower() if len(parts) > 2 else "m1"
        cfg  = SYMBOLS.get(sym)
        if not cfg: send_telegram(f"❌ {sym} não encontrado.", chat_id); return
        candles = get_candles(sym, "1m", 80) if tf_ == "m1" else get_candles(sym, "5m", 120)
        if not candles: send_telegram("❌ Erro ao buscar candles.", chat_id); return
        long_, short_ = both_entries(candles, cfg["min_wave"], tf_)
        if not long_ or not short_:
            send_telegram(f"⏳ Nenhuma compressão detectada em {sym} {tf_.upper()} no momento.", chat_id)
            return
        ts   = br_now()
        modo = "TESTNET 🟡" if BYBIT_MODE == "testnet" else "REAL 🔴"
        # Coloca as duas ordens
        data_fake = {"symbol": sym, "m1_trend":"?", "m5_trend":"?",
                     "both_m1": (long_, short_) if tf_=="m1" else None,
                     "both_m5": (long_, short_) if tf_=="m5" else None,
                     "entry": None}
        fire_signal(data_fake)
    elif cmd == "/executar":
        if len(parts) < 3:
            send_telegram("Uso: /executar BTC up [m1|m5]", chat_id); return
        sym  = _parse_sym(parts[1])
        dir_ = parts[2].lower()   # up ou down
        tf_  = parts[3].lower() if len(parts) > 3 else "m1"
        cfg  = SYMBOLS.get(sym)
        if not cfg: send_telegram(f"❌ Símbolo {sym} não encontrado.", chat_id); return
        if dir_ not in ("up", "down"):
            send_telegram("❌ Direção inválida. Use: up ou down", chat_id); return

        send_telegram(f"🔍 Buscando setup {tf_.upper()} {dir_.upper()} em {sym}...", chat_id)
        if tf_ == "m1":
            candles = get_candles(sym, "1m", 80)
            todas_m1 = m1_swing_entries(candles, cfg["min_wave"]) if candles else []
            filtradas = [e for e in todas_m1 if e.get("direcao") == dir_]
            entry = filtradas[0] if filtradas else None
        else:  # m5
            candles = get_candles(sym, "5m", 120)
            todas   = m5_elliott_entries(candles, cfg["min_wave"]) if candles else []
            # Filtra pela direção solicitada e pega o de maior prioridade
            _pri = {"EW_ABC_C_LONG":4,"EW_ABC_C_SHORT":4,
                    "EW_TRI_E_LONG":3,"EW_TRI_E_SHORT":3,
                    "EW_W2_LONG":2,"EW_W2_SHORT":2,"EW_W4_LONG":2,"EW_W4_SHORT":2}
            filtradas = [e for e in todas if e.get("direcao") == dir_]
            entry = max(filtradas, key=lambda e: _pri.get(e["tipo"],1)) if filtradas else None

        if not entry:
            send_telegram(
                f"⏳ Nenhum setup válido {tf_.upper()} {dir_.upper()} para {sym} no momento.\n"
                f"Nenhum setup técnico de topos/fundos encontrado na zona.", chat_id)
            return

        ep   = entry["entry"]; sp = entry["stop"]; tp = entry["alvo"]
        rr   = entry["rr"];    tipo = entry.get("tipo", "")
        modo = "TESTNET 🟡" if BYBIT_MODE == "testnet" else "REAL 🔴"
        side_by = "Buy" if dir_ == "up" else "Sell"
        res  = order_futures(sym, side_by, cfg["qty"], sl=sp, tp=tp)

        ts   = br_now()
        bi   = ""
        sinal = {"id": len(memory["signals"])+1, "symbol": sym,
                 "direcao": dir_, "entrada": ep, "stop": sp, "alvo": tp,
                 "risco": abs(ep-sp), "rr": rr,
                 "data": ts, "status": "aberto",
                 "resultado": None, "order_id": None, "tipo": tipo}
        if res and res.get("ok"):
            sinal["order_id"] = res["order_id"]
            bi = f"\n🏦 Bybit [{modo}] ✅ <code>{res['order_id']}</code>"
        else:
            bi = f"\n❌ Bybit: {res.get('error','?') if res else 'sem resposta'}"
        memory["signals"].append(sinal)
        if len(memory["signals"]) > 200: memory["signals"] = memory["signals"][-200:]
        save_memory()
        emoji = "✅" if dir_ == "up" else "🔴"
        acao  = "COMPRA" if dir_ == "up" else "VENDA"
        send_telegram(
            f"{emoji} <b>EXECUTADO {acao}</b> [{tf_.upper()}] — {sym}\n"
            f"💰 Entrada: <b>${ep:,.4f}</b>\n"
            f"🛑 Stop Técnico: <b>${sp:,.4f}</b>\n"
            f"🎯 Alvo Técnico: <b>${tp:,.4f}</b>\n"
            f"📐 R:R 1:<b>{rr}</b>\n"
            f"📌 {tipo}"
            f"{bi}\n"
            f"⏰ {ts} (UTC-3)" + ASSINATURA, chat_id)

    # ── STATUS ──────────────────────────────────────────────
    elif cmd == "/status":
        modo = "TESTNET 🟡" if BYBIT_MODE == "testnet" else "REAL 🔴"
        msg  = f"📡 <b>Status</b> [{modo}]\n"
        for sym in SYMBOLS:
            cm = get_candles(sym, "1m", 30)
            c5 = get_candles(sym, "5m", 60)
            if not cm and not c5: msg += f"⚠️ {sym}\n"; continue
            p   = (cm or c5)[-1]["close"]
            m1t = get_trend(cm, SWING_M1) if cm else "neutral"
            m5t = get_trend(c5, SWING_M5) if c5 else "neutral"
            em  = "🟢" if m1t=="up" else ("🔴" if m1t=="down" else
                  "🟢" if m5t=="up" else ("🔴" if m5t=="down" else "⚪"))
            msg += f"{em} <b>{sym}</b> ${p:,.4f} M1:{m1t.upper()} M5:{m5t.upper()}\n"
        msg += f"⏰ {br_now('%d/%m %H:%M')} UTC"
        send_telegram(msg, chat_id)

    # ── ANALISE ─────────────────────────────────────────────
    elif cmd == "/analise":
        msg = "📊 <b>Analise M1+M5</b>\n"
        for sym in SYMBOLS:
            d = analyze_symbol(sym)
            if not d: continue
            m1t = d["m1_trend"]; m5t = d["m5_trend"]
            p   = d["price"]
            em  = "📈" if m1t=="up" else ("📉" if m1t=="down" else
                  "📈" if m5t=="up" else ("📉" if m5t=="down" else "⚪"))
            n_m1  = "✅" if d.get("entry_m1") else "⏳"
            n_m5  = len(d.get("entries_m5", []))
            msg += (f"{em} <b>{sym}</b> ${p:,.4f}\n"
                    f"   M1:{m1t.upper()} {n_m1}  M5:{m5t.upper()} {n_m5}setup(s)\n")
        send_telegram(msg, chat_id)

    # ── PERFORMANCE ─────────────────────────────────────────
    elif cmd == "/performance":
        rate        = get_usd_brl()
        sinais      = memory.get("signals", [])
        dep_total   = float(os.environ.get("DEPOSITO_TOTAL_BRL", "55"))
        # Aceita tanto MANUAL_PROFITS quanto TRADFI_BRL
        manual_brl  = float(os.environ.get("MANUAL_PROFITS",
                       os.environ.get("TRADFI_BRL", "0")))
        # Saldo cripto via API
        saldo_usd   = 0.0
        r_s = bybit_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        if r_s and r_s.get("retCode") == 0:
            for acct in r_s.get("result", {}).get("list", []):
                eq = float(acct.get("totalEquity") or acct.get("totalWalletBalance") or 0)
                if eq > 0: saldo_usd = eq; break
        cripto_brl  = saldo_usd * rate
        total_brl   = cripto_brl + manual_brl
        lucro_brl   = total_brl - dep_total
        lucro_pct   = (lucro_brl / dep_total * 100) if dep_total > 0 else 0
        em          = "🟢" if lucro_brl >= 0 else "🔴"
        sp          = "+" if lucro_pct >= 0 else ""
        wins    = [s for s in sinais if s["status"] == "win"]
        losses  = [s for s in sinais if s["status"] == "loss"]
        abertos = [s for s in sinais if s["status"] == "aberto"]
        total_f = len(wins) + len(losses)
        wr      = (len(wins)/total_f*100) if total_f > 0 else 0
        r_net   = sum(float(s["rr"]) for s in wins) - len(losses)
        sym_stats = ""
        for sym in SYMBOLS:
            try:
                ss  = [s for s in sinais if s.get("symbol") == sym]
                if not ss: continue
                sw  = [s for s in ss if s["status"] == "win"]
                sl2 = [s for s in ss if s["status"] == "loss"]
                tf2 = len(sw) + len(sl2)
                wr2 = (len(sw)/tf2*100) if tf2 > 0 else 0
                rn2 = sum(float(s["rr"]) for s in sw) - len(sl2)
                sym_stats += f"• {sym}: {len(sw)}W/{len(sl2)}L WR:{wr2:.0f}% {rn2:+.1f}R\n"
            except: pass
        manual_line = f"👤 Jon (TradFi/Forex): R${manual_brl:,.2f}\n" if manual_brl > 0 else ""
        msg = (
            f"📊 <b>Performance Geral</b>\n"
            f"💼 Deposito: R${dep_total:,.2f} | 💱 R${rate:.2f}\n"
            f"{em} <b>Resultado: {sp}{lucro_pct:.1f}% ({sp}R${lucro_brl:,.2f})</b>\n"
            f"________________________\n"
            f"🤖 Cripto (Robo): R${cripto_brl:,.2f}\n"
            f"{manual_line}"
            f"💎 Total geral: R${total_brl:,.2f}\n"
            f"________________________\n"
            f"📈 {len(sinais)} sinais | ✅{len(wins)} ❌{len(losses)} ⏳{len(abertos)}\n"
            f"🎯 WR robo: <b>{wr:.1f}%</b> | 💰 <b>{r_net:+.2f}R</b>\n"
            f"________________________\n"
            f"{sym_stats}"
            f"________________________\n"
            f"⏰ {br_now('%d/%m %H:%M')} (UTC-3)"
        )
        send_telegram(msg + ASSINATURA, chat_id)

    elif cmd == "/ranking":
        perf = sym_performance()
        rankeados = sorted([(s,p) for s,p in perf.items() if p["trades"] >= MIN_TRADES_RANK],
                           key=lambda x: x[1]["wr"] or 0, reverse=True)
        novos = [(s,p) for s,p in perf.items() if p["trades"] < MIN_TRADES_RANK]
        msg = f"🏆 <b>Ranking</b> | WR mínimo: {MIN_WR_FILTER:.0f}%\n________________________\n"
        for i, (sym, p) in enumerate(rankeados, 1):
            ok, _ = sym_allowed(sym, perf)
            msg += f"{'✅' if ok else '❌'} {i}. {sym}: WR {p['wr']:.0f}% {p['wins']}W/{p['losses']}L {p['rnet']:+.1f}R\n"
        if novos:
            msg += f"\n🆕 Aprendendo:\n"
            for sym, p in novos:
                msg += f"• {sym}: {p['trades']} trades\n"
        msg += f"⏰ {br_now()} (UTC-3)"
        send_telegram(msg + ASSINATURA, chat_id)

        # ── RELATORIO ───────────────────────────────────────────
    elif cmd == "/relatorio":
        sinais = memory.get("signals", [])
        if not sinais: send_telegram("📊 Nenhum sinal ainda.", chat_id); return
        rate   = get_usd_brl()
        wins   = [s for s in sinais if s["status"] == "win"]
        losses = [s for s in sinais if s["status"] == "loss"]
        ab     = [s for s in sinais if s["status"] == "aberto"]
        tf2    = len(wins) + len(losses)
        wr     = (len(wins)/tf2*100) if tf2 > 0 else 0
        # P&L total usando pnl_brl salvo em check_signals (lógica pts×lote)
        total_brl = sum(s.get("pnl_brl", 0) for s in wins + losses)
        sinal_txt = "+" if total_brl >= 0 else ""
        send_telegram(
            f"📊 <b>Relatório Completo</b>\n"
            f"✅ {len(wins)}W  ❌ {len(losses)}L  ⏳ {len(ab)} abertos\n"
            f"🎯 WR: <b>{wr:.0f}%</b>\n"
            f"💰 P&L total: <b>{sinal_txt}R${total_brl:.2f}</b>\n"
            f"💱 USD/BRL R${rate:.2f}\n"
            f"⏰ {br_now('%d/%m %H:%M')} UTC", chat_id)
        for sym in SYMBOLS:
            ss = [s for s in sinais if s.get("symbol") == sym]
            if not ss: continue
            linhas = []
            sym_pnl = 0.0
            for s in ss[-10:]:
                em = "✅" if s["status"]=="win" else ("❌" if s["status"]=="loss" else "⏳")
                qty = SYMBOLS.get(sym, {}).get("qty", 0)
                if s["status"] == "win":
                    pnl_b = s.get("pnl_brl") or (abs(s["alvo"]-s["entrada"]) * qty * rate)
                    sym_pnl += pnl_b
                    fin = f"+R${pnl_b:.2f}"
                elif s["status"] == "loss":
                    pnl_b = s.get("pnl_brl") or -(abs(s["stop"]-s["entrada"]) * qty * rate)
                    sym_pnl += pnl_b
                    fin = f"-R${abs(pnl_b):.2f}"
                else:
                    fin = "aberto"
                pts = abs(s.get("alvo", s["entrada"]) - s["entrada"])
                linhas.append(
                    f"{em} #{s['id']} {'BUY' if s['direcao']=='up' else 'SELL'} "
                    f"${s['entrada']:,.4f} | {pts:.4f}pts×{qty}lote → {fin}")
            sym_sign = "+" if sym_pnl >= 0 else ""
            send_telegram(
                f"📋 <b>{sym}</b>  P&L: <b>{sym_sign}R${sym_pnl:.2f}</b>\n"
                + "\n".join(linhas), chat_id)

    # ── HOJE ────────────────────────────────────────────────
    elif cmd == "/hoje":
        sinais = memory.get("signals", [])
        hoje   = br_now("%d/%m/%Y")
        hs     = [s for s in sinais if s.get("data", "").startswith(hoje)]
        if not hs: send_telegram(f"📅 Nenhum sinal hoje ({hoje}).", chat_id); return
        wh = [s for s in hs if s["status"] == "win"]
        lh = [s for s in hs if s["status"] == "loss"]
        rn     = sum(float(s["rr"]) for s in wh) - len(lh)
        rn_usd = rn * RISCO_USDT
        rn_brl = usd_to_brl(rn_usd)
        wr = (len(wh)/max(len(wh)+len(lh), 1))*100
        def _linha(s):
            em = "✅" if s["status"]=="win" else "❌" if s["status"]=="loss" else "⏳"
            if s["status"] == "win":
                v = usd_to_brl(float(s["rr"]) * RISCO_USDT)
                fin = f"+R${v:.2f}"
            elif s["status"] == "loss":
                v = usd_to_brl(RISCO_USDT)
                fin = f"-R${v:.2f}"
            else:
                fin = "aberto"
            return (f"{em} #{s['id']} {s.get('symbol','')} "
                    f"{'BUY' if s['direcao']=='up' else 'SELL'}→{fin}")
        hist = "\n".join(_linha(s) for s in hs)
        sinal = "+" if rn_brl >= 0 else ""
        send_telegram(
            f"📅 <b>Hoje</b> ({hoje})\n"
            f"✅{len(wh)} ❌{len(lh)} WR:{wr:.0f}% 💰{sinal}R${rn_brl:.2f}\n{hist}", chat_id)

    # ── SALDO ───────────────────────────────────────────────
    elif cmd == "/saldo":
        r = broker_account()
        if not r or r.get("retCode") != 0:
            send_telegram(f"❌ {r}", chat_id); return
        coins  = r.get("result", {}).get("list", [{}])[0].get("coin", [])
        linhas = [f"• {c['coin']}: {float(c.get('equity') or c.get('walletBalance', 0)):.4f}"
                  for c in coins if float(c.get("equity") or c.get("walletBalance", 0)) > 0]
        modo = "TESTNET 🟡" if BYBIT_MODE == "testnet" else "REAL 🔴"
        send_telegram(f"💰 <b>Saldo [{modo}]</b>\n" + ("\n".join(linhas) or "Vazio"), chat_id)

    # ── POSICOES ────────────────────────────────────────────
    elif cmd == "/posicoes":
        r = broker_positions()
        if not r or r.get("retCode") != 0:
            send_telegram(f"❌ {r}", chat_id); return
        lista = [p for p in r.get("result", {}).get("list", []) if float(p.get("size", 0)) > 0]
        if not lista: send_telegram("📭 Nenhuma posicao aberta.", chat_id); return
        msg = "📊 <b>Posicoes</b>\n"
        for p in lista:
            pnl = float(p.get("unrealisedPnl", 0)); ep = float(p.get("avgPrice", 0))
            em  = "🟢" if pnl >= 0 else "🔴"
            msg += f"{em} {p['side']} {p.get('size')} {p['symbol']} ${ep:,.4f} PnL:${pnl:+.2f}\n"
        send_telegram(msg, chat_id)

    # ══ ORDENS SPOT ══════════════════════════════════════════

    elif cmd == "/comprar":
        if len(parts) < 3: send_telegram("Uso: /comprar BTC 0.001", chat_id); return
        sym = _parse_sym(parts[1]); qty = parts[2]
        res = order_spot(sym, "Buy", qty)
        send_telegram(_ok_msg(res, f"Spot BUY {qty} {sym}"), chat_id)

    elif cmd == "/vender":
        if len(parts) < 3: send_telegram("Uso: /vender BTC 0.001", chat_id); return
        sym = _parse_sym(parts[1]); qty = parts[2]
        res = order_spot(sym, "Sell", qty)
        send_telegram(_ok_msg(res, f"Spot SELL {qty} {sym}"), chat_id)

    elif cmd == "/vendertudo":
        if len(parts) < 2: send_telegram("Uso: /vendertudo BTC", chat_id); return
        sym = _parse_sym(parts[1])
        res = sell_all_spot(sym)
        send_telegram(_ok_msg(res, f"Spot SELL ALL {sym}"), chat_id)

    # ══ ORDENS FUTUROS ═══════════════════════════════════════

    elif cmd == "/long":
        if len(parts) < 3: send_telegram("Uso: /long BTC 0.001 [sl=60000] [tp=65000]", chat_id); return
        sym = _parse_sym(parts[1]); qty = parts[2]
        sl  = next((p.split("=")[1] for p in parts if p.lower().startswith("sl=")), None)
        tp  = next((p.split("=")[1] for p in parts if p.lower().startswith("tp=")), None)
        res = order_futures(sym, "Buy", qty, sl=sl, tp=tp)
        send_telegram(_ok_msg(res, f"Futuros LONG {qty} {sym}"), chat_id)

    elif cmd == "/short":
        if len(parts) < 3: send_telegram("Uso: /short BTC 0.001 [sl=65000] [tp=60000]", chat_id); return
        sym = _parse_sym(parts[1]); qty = parts[2]
        sl  = next((p.split("=")[1] for p in parts if p.lower().startswith("sl=")), None)
        tp  = next((p.split("=")[1] for p in parts if p.lower().startswith("tp=")), None)
        res = order_futures(sym, "Sell", qty, sl=sl, tp=tp)
        send_telegram(_ok_msg(res, f"Futuros SHORT {qty} {sym}"), chat_id)

    # ══ ORDENS LIMITADAS ═════════════════════════════════════

    elif cmd == "/ls":
        if len(parts) < 4: send_telegram("Uso: /ls BTC 0.001 62000", chat_id); return
        sym = _parse_sym(parts[1]); qty = parts[2]; price = parts[3]
        res = order_limit("spot", sym, "Buy", qty, price)
        send_telegram(_ok_msg(res, f"Limit Spot BUY {qty} {sym} @ ${float(price):,.2f}"), chat_id)

    elif cmd == "/lsv":
        if len(parts) < 4: send_telegram("Uso: /lsv BTC 0.001 65000", chat_id); return
        sym = _parse_sym(parts[1]); qty = parts[2]; price = parts[3]
        res = order_limit("spot", sym, "Sell", qty, price)
        send_telegram(_ok_msg(res, f"Limit Spot SELL {qty} {sym} @ ${float(price):,.2f}"), chat_id)

    elif cmd == "/ll":
        if len(parts) < 4: send_telegram("Uso: /ll BTC 0.001 62000 [sl=60000] [tp=65000]", chat_id); return
        sym = _parse_sym(parts[1]); qty = parts[2]; price = parts[3]
        sl  = next((p.split("=")[1] for p in parts if p.lower().startswith("sl=")), None)
        tp  = next((p.split("=")[1] for p in parts if p.lower().startswith("tp=")), None)
        res = order_limit("linear", sym, "Buy", qty, price, sl=sl, tp=tp)
        send_telegram(_ok_msg(res, f"Limit Long {qty} {sym} @ ${float(price):,.2f}"), chat_id)

    elif cmd == "/lsh":
        if len(parts) < 4: send_telegram("Uso: /lsh BTC 0.001 65000 [sl=67000] [tp=62000]", chat_id); return
        sym = _parse_sym(parts[1]); qty = parts[2]; price = parts[3]
        sl  = next((p.split("=")[1] for p in parts if p.lower().startswith("sl=")), None)
        tp  = next((p.split("=")[1] for p in parts if p.lower().startswith("tp=")), None)
        res = order_limit("linear", sym, "Sell", qty, price, sl=sl, tp=tp)
        send_telegram(_ok_msg(res, f"Limit Short {qty} {sym} @ ${float(price):,.2f}"), chat_id)

    # ══ FECHAR / CANCELAR ════════════════════════════════════

    elif cmd == "/fechar":
        arg = " ".join(parts[1:]).lower() if len(parts) > 1 else "tudo"
        if arg == "tudo":
            send_telegram("⚠️ Fechando todos futuros...", chat_id)
            close_futures_all()
            send_telegram("✅ Todos futuros fechados.", chat_id)
        else:
            sym = _parse_sym(parts[1])
            ok, msg2 = close_futures_symbol(sym)
            send_telegram(f"{'✅' if ok else '❌'} {msg2}", chat_id)

    elif cmd == "/cancelar":
        if len(parts) < 2: send_telegram("Uso: /cancelar BTC", chat_id); return
        sym = _parse_sym(parts[1])
        cancel_open_orders(sym, "linear")
        cancel_open_orders(sym, "spot")
        send_telegram(f"✅ Ordens pendentes canceladas: {sym}", chat_id)

    # ── NOTICIAS ─────────────────────────────────────────────
    elif cmd == "/noticias":
        filtro = parts[1] if len(parts) > 1 else None
        send_telegram("🔍 Buscando notícias...", chat_id)
        send_news_telegram(chat_id, filtro=filtro)

    elif cmd == "/urgente":
        news = [n for n in fetch_news(force=True) if n["score"] == 3]
        if not news:
            send_telegram("✅ Nenhuma notícia urgente no momento.", chat_id); return
        send_telegram(f"🚨 <b>{len(news)} notícia(s) urgente(s)</b>", chat_id)
        for it in news[:5]:
            desc_line = f"\n<i>{it['desc'][:100]}</i>" if it.get("desc") else ""
            caption = (f"🚨 <b>{it['source']}</b>\n<b>{it['title']}</b>"
                       f"{desc_line}\n🔗 <a href=\"{it['link']}\">Leia mais</a>")
            if it.get("img"):
                send_photo_url_telegram(it["img"], caption, chat_id)
            else:
                send_telegram(caption, chat_id)
            time.sleep(0.5)

    else:
        send_telegram("Comando nao reconhecido. /help", chat_id)

# ─── LOOP COMANDOS ───────────────────────────────────────────
def commands_loop():
    print("Ouvindo comandos...")
    while True:
        try:
            for upd in get_updates():
                msg  = upd.get("message") or upd.get("edited_message")
                if not msg: continue
                cid  = str(msg["chat"]["id"])
                text = msg.get("text", "")
                if text.startswith("/"):
                    print(f"[CMD] {text}")
                    handle_command(text, cid)
                elif msg.get("photo"):
                    photo   = msg["photo"][-1]
                    caption = msg.get("caption", "")
                    try:
                        img = download_photo(photo["file_id"])
                        threading.Thread(target=process_image,
                                         args=(img, cid, caption), daemon=True).start()
                    except Exception as e:
                        send_telegram(f"Erro foto: {e}", cid)
        except Exception as e:
            print(f"Erro cmd loop: {e}")
        time.sleep(2)

# ─── LOOP PRINCIPAL ──────────────────────────────────────────
_loop_n      = 0
STATUS_EVERY = max(1, int(4*3600/CHECK_INTERVAL))

def sym_performance():
    sinais = memory.get("signals", [])
    perf = {}
    for sym in SYMBOLS:
        ss     = [s for s in sinais if s.get("symbol") == sym]
        wins   = [s for s in ss if s["status"] == "win"]
        losses = [s for s in ss if s["status"] == "loss"]
        total  = len(wins) + len(losses)
        perf[sym] = {"wr": (len(wins)/total*100) if total > 0 else None,
                     "trades": total, "wins": len(wins), "losses": len(losses),
                     "rnet": sum(float(s["rr"]) for s in wins) - len(losses)}
    return perf

def sym_allowed(sym, perf):
    p = perf.get(sym, {})
    if p.get("trades", 0) < MIN_TRADES_RANK: return True, "novo"
    wr = p.get("wr")
    if wr is not None and wr >= MIN_WR_FILTER: return True, f"WR {wr:.0f}%"
    return False, f"WR {wr:.0f}% < {MIN_WR_FILTER:.0f}%"

def main_loop():
    global _loop_n
    while True:
        try:
            _loop_n += 1
            price_map = {}
            results = {}
            def _an(sym): results[sym] = analyze_symbol(sym)
            ths = [threading.Thread(target=_an, args=(s,), daemon=True) for s in SYMBOLS]
            for t in ths: t.start()
            for t in ths: t.join(timeout=30)

            if _loop_n % STATUS_EVERY == 0:
                msg = f"📡 {br_now('%d/%m %H:%M')} UTC\n"
                for sym, d in results.items():
                    if not d: msg += f"⚠️ {sym}\n"; continue
                    p    = d["price"]
                    m1t  = d["m1_trend"]; m5t = d["m5_trend"]
                    n_m1 = len(d.get("entries_m1",[])); n_m5 = len(d.get("entries_m5",[]))
                    both = "⚡" if (d.get("both_m1") or d.get("both_m5")) else ""
                    em   = "🟢" if m1t=="up" else ("🔴" if m1t=="down" else
                           "🟢" if m5t=="up" else ("🔴" if m5t=="down" else "⚪"))
                    setups = f"M1:{n_m1} M5:{n_m5}{both}" if (n_m1+n_m5) else f"{m1t[:1].upper()}/{m5t[:1].upper()}"
                    msg += f"{em} <b>{sym}</b> ${p:,.4f} {setups}\n"
                send_telegram(msg + ASSINATURA)
            _sym_perf = sym_performance()
            for sym, data in results.items():
                if not data: continue
                price_map[sym] = data["price"]
                m1t      = data.get("m1_trend","neutral")
                m5t      = data.get("m5_trend","neutral")
                entries_m1 = data.get("entries_m1", [])
                entries_m5 = data.get("entries_m5", [])
                has_both   = bool(data.get("both_m1") or data.get("both_m5"))
                n_m1 = len(entries_m1); n_m5 = len(entries_m5)
                print(f"[{br_now('%H:%M')}] {sym} ${data['price']:,.4f} "
                      f"M1:{m1t}({n_m1}) M5:{m5t}({n_m5})"
                      f"{' BOTH⚡' if has_both else ''}")
                now_ts = time.time()

                # ── Both (compressão): dispara, com cooldown curto p/ não repetir a mesma faixa ──
                if has_both:
                    l_, s_ = data["both_m1"] or data["both_m5"]
                    ref    = l_ or s_
                    key_b  = f"{sym}_BOTH_{round(ref['stop'],4)}_{round(ref['alvo'],4)}"
                    if now_ts - last_signal_time.get(key_b, 0) >= SHORT_COOLDOWN:
                        fire_signal(data)
                        last_signal_time[key_b] = now_ts
                    continue

                # ── M1: dispara CADA setup encontrado, com cooldown curto por entrada idêntica ──
                for e in entries_m1:
                    key = f"{sym}_M1_{e['tipo']}_{round(e['stop'],4)}_{round(e['alvo'],4)}"
                    if now_ts - last_signal_time.get(key, 0) >= SHORT_COOLDOWN:
                        data_copia = dict(data); data_copia["entry"] = e
                        fire_signal(data_copia)
                        last_signal_time[key] = now_ts

                # ── M5 Elliott: dispara melhor setup se tendência definida, com cooldown curto ──
                best_m5 = data.get("best_m5")
                eff_m5  = m5t if m5t != "neutral" else m1t
                if best_m5 and eff_m5 != "neutral":
                    key = f"{sym}_M5_{best_m5['tipo']}_{round(best_m5['stop'],4)}_{round(best_m5['alvo'],4)}"
                    if now_ts - last_signal_time.get(key, 0) >= SHORT_COOLDOWN:
                        data_copia = dict(data); data_copia["entry"] = best_m5
                        fire_signal(data_copia)
                        last_signal_time[key] = now_ts

                # ── HTF (H1/H4/Fib/OB/BOS): dispara com cooldown maior (10min) ──
                # Cooldown maior pois são setups de maior duração
                HTF_COOLDOWN = 600
                for e in data.get("entries_htf_all", []):
                    key = f"{sym}_HTF_{e['tipo']}_{round(e['stop'],4)}_{round(e['alvo'],2)}"
                    if now_ts - last_signal_time.get(key, 0) >= HTF_COOLDOWN:
                        data_copia = dict(data); data_copia["entry"] = e
                        fire_signal(data_copia)
                        last_signal_time[key] = now_ts
            check_signals(price_map)
        except Exception as e:
            print(f"[ERRO] {e}")
            import traceback; traceback.print_exc()
        time.sleep(CHECK_INTERVAL)

# ─── LOOP NOTÍCIAS URGENTES (automático a cada 10 min) ──────
def urgent_news_loop():
    time.sleep(60)  # aguarda inicialização
    while True:
        try:
            check_urgent_news()
        except Exception as e:
            print(f"[NEWS URGENT] {e}")
        time.sleep(NEWS_URGENT_CHECK)

# ─── START ───────────────────────────────────────────────────
modo_b   = "TESTNET 🟡" if BYBIT_MODE == "testnet" else "REAL 🔴"
broker_s  = "✅ OK" if BYBIT_API_KEY else "❌ Sem chaves"
print(f"TRON Bot v3 | Bybit [{modo_b}]: {broker_s}")
print(f"Simbolos: {', '.join(SYMBOLS.keys())}")
threading.Thread(target=run_server, daemon=True).start()
load_memory()
threading.Thread(target=commands_loop, daemon=True).start()
threading.Thread(target=urgent_news_loop, daemon=True).start()
send_telegram(
    f"🤖 <b>TRON Bot v3 iniciado!</b>\n"
    f"📊 {', '.join(SYMBOLS.keys())}\n"
    f"🏦 Bybit [{modo_b}]: {broker_s}\n"
    f"⚡ Alavancagem: {BYBIT_LEVERAGE}x\n"
    f"📐 Stop & Alvo TÉCNICOS (topos/fundos)\n"
    f"🔬 Topos/Fundos M1+M5 + Figuras Geométricas\n"
    f"/help para comandos\n"
    f"🧠 {memory['total_prints']} prints\n"
    f"⏰ {br_now('%d/%m/%Y %H:%M')} (UTC-3)" + ASSINATURA)
main_loop()
