#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenAlgo auto-seller:
- You buy manually.
- Script keeps one SELL LIMIT per symbol at (avg + ₹0.35) for total qty.
- Stops tracking when qty -> 0.
- Summary every 60s. Quiet otherwise.

Requires:
  pip install openalgo
"""

import os
import time
import threading
import signal
from typing import Dict, Tuple, List, Set
import dotenv
from openalgo import api

dotenv.load_dotenv()

API_KEY   = os.getenv("API_KEY", "").strip()
HOST      = os.getenv("OPENALGO_HOST", "https://openalgo.rpinj.shop").strip()
WS_URL    = os.getenv("OPENALGO_WS", "wss://openalgows.rpinj.shop").strip()
EXCHANGE  = os.getenv("OPENALGO_EXCHANGE", "NSE").strip().upper()
PRODUCT   = os.getenv("OPENALGO_PRODUCT", "CNC").strip().upper()
TICK_SIZE = float(os.getenv("OPENALGO_TICK", "0.05"))   # adjust if needed

client = api(api_key=API_KEY, host=HOST, ws_url=WS_URL)

# ---------------- Helpers ----------------
def round_to_tick(price: float, tick: float) -> float:
    # NSE common tick 0.05; change if your symbol differs
    return round(round(price / tick) * tick, 2)

def weighted_avg(pairs: List[Tuple[float, float]]) -> float:
    num = sum(q * a for q, a in pairs)
    den = sum(q for q, _ in pairs)
    return (num / den) if den > 0 else 0.0

# ---------------- State ----------------
class SymbolState:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.total_qty = 0.0
        self.avg_price = 0.0
        self.open_sell_id = None
        self.open_sell_px = None
        self.open_sell_qty = None
        self.tracking = False

    def __repr__(self):
        return (f"<{self.symbol} qty={self.total_qty} avg={self.avg_price} "
                f"sell_id={self.open_sell_id} sell_px={self.open_sell_px} sell_qty={self.open_sell_qty} "
                f"tracking={self.tracking}>")

SYMBOLS: Dict[str, SymbolState] = {}
STATE_LOCK = threading.Lock()
STOP = {"flag": False}

# ------------- REST snapshots -------------
def snapshot_positions_and_holdings() -> Dict[str, Tuple[float, float]]:
    """
    Returns dict: symbol -> (total_qty, combined_avg).
    Combines positionbook() + holdings() (if available).
    """
    totals: Dict[str, List[Tuple[float, float]]] = {}

    # Positions
    try:
        pos = client.positionbook()
        if pos.get("status") == "success":
            for p in pos.get("data", []):
                if p.get("exchange", "").upper() == EXCHANGE and p.get("product", "").upper() == PRODUCT:
                    sym = p.get("symbol", "").upper()
                    qty = float(p.get("quantity", 0) or 0)
                    if qty != 0:
                        avg = float(p.get("average_price", 0) or 0)
                        totals.setdefault(sym, []).append((qty, avg))
    except Exception as e:
        print("[WARN] positionbook error:", e)

    # Holdings (optional; field names can vary slightly)
    try:
        h = client.holdings()
        if h.get("status") == "success":
            for row in h.get("data", {}).get("holdings", []):
                if row.get("exchange", "").upper() == EXCHANGE:
                    sym = row.get("symbol", "").upper()
                    qty = float(row.get("quantity", 0) or 0)
                    if qty != 0:
                        avg = float(row.get("average_price", row.get("avg_price", 0)) or 0)
                        totals.setdefault(sym, []).append((qty, avg))
    except Exception:
        pass

    merged: Dict[str, Tuple[float, float]] = {}
    for sym, pairs in totals.items():
        total_qty = sum(q for q, _ in pairs)
        avg = weighted_avg(pairs)
        merged[sym] = (total_qty, avg)
    return merged

def snapshot_open_orders() -> Dict[str, List[dict]]:
    """
    Returns dict symbol -> list of OPEN orders for our EXCHANGE/PRODUCT.
    """
    out: Dict[str, List[dict]] = {}
    try:
        ob = client.orderbook()
        if ob.get("status") == "success":
            for o in ob.get("data", {}).get("orders", []):
                if (o.get("exchange", "").upper() == EXCHANGE and
                    o.get("product", "").upper() == PRODUCT and
                    o.get("order_status", "").lower() == "open"):
                    sym = o.get("symbol", "").upper()
                    out.setdefault(sym, []).append(o)
    except Exception as e:
        print("[WARN] orderbook error:", e)
    return out

def snapshot_latest_trade(symbol: str) -> dict:
    try:
        tb = client.tradebook()
        if tb.get("status") == "success":
            trades = [t for t in tb.get("data", [])
                      if t.get("symbol", "").upper() == symbol
                      and t.get("exchange", "").upper() == EXCHANGE
                      and t.get("product", "").upper() == PRODUCT]
            if trades:
                return trades[-1]
    except Exception as e:
        print("[WARN] tradebook error:", e)
    return {}

# ------------- Actions -------------
def cancel_existing_sells(symbol: str, open_orders_by_symbol: Dict[str, List[dict]]):
    """
    Cancels all OPEN SELL orders for symbol (uses cancelorder(order_id=...)).
    """
    for o in open_orders_by_symbol.get(symbol, []):
        if o.get("action", "").upper() == "SELL":
            # order id key is 'orderid' in orderbook payload
            oid = o.get("orderid") or o.get("order_id")
            if not oid:
                continue
            try:
                client.cancelorder(order_id=str(oid))  # official signature
                print(f"[ACTION] Cancelled SELL {symbol} (order_id={oid})")
            except Exception as e:
                print(f"[WARN] Failed to cancel SELL for {symbol} id={oid}: {e}")

def ensure_one_sell(symbol: str, qty: float, avg_price: float) -> Tuple[str, float, float]:
    """
    Places a single SELL LIMIT at avg+0.35 for the full qty.
    Uses placeorder(..., price_type='LIMIT').
    Returns (order_id, price, qty).
    """
    if qty <= 0:
        return (None, None, None)

    px = round_to_tick(avg_price + 0.35, TICK_SIZE)
    try:
        resp = client.placeorder(
            symbol=symbol,
            exchange=EXCHANGE,
            action="SELL",
            quantity=int(qty) if float(qty).is_integer() else float(qty),
            price=str(px),                 # many brokers expect string
            product=PRODUCT,
            price_type="LIMIT"            # official name per docs
        )
        # Extract order id from common locations
        oid = None
        if isinstance(resp, dict):
            oid = (resp.get("orderid") or
                   resp.get("order_id") or
                   resp.get("data", {}).get("orderid") or
                   resp.get("data", {}).get("order_id"))
        print(f"[ACTION] Placed SELL {symbol} qty={qty} @ {px} (order_id={oid})")
        return (str(oid) if oid else None, px, qty)
    except Exception as e:
        print(f"[ERROR] Failed to place SELL for {symbol} qty={qty}: {e}")
        return (None, None, None)

# ------------- WebSocket (quiet) -------------
def on_tick(_data: dict):
    # WS used mainly to keep the process "live" and future-ready for tick-driven logic.
    # We keep logs quiet as requested.
    pass

# ------------- Supervisor -------------
def supervisor():
    """
    - Reconciles positions+holdings, open orders.
    - Detects manual BUY executions (qty increase).
    - Maintains one SELL @ avg+0.35 for full qty.
    - Stops tracking when qty -> 0.
    - Summary log every 60s.
    """
    last_log_ts = 0
    last_qty: Dict[str, float] = {}

    while not STOP["flag"]:
        try:
            totals = snapshot_positions_and_holdings()   # sym -> (qty, avg)
            open_orders = snapshot_open_orders()         # sym -> [orders]

            with STATE_LOCK:
                # Discover active symbols
                for sym in totals.keys():
                    if sym not in SYMBOLS:
                        SYMBOLS[sym] = SymbolState(sym)

            # Per-symbol logic
            for sym in list(SYMBOLS.keys()):
                qty, avg = totals.get(sym, (0.0, 0.0))
                st = SYMBOLS[sym]
                prev_qty = last_qty.get(sym, 0.0)

                st.total_qty = qty
                st.avg_price = avg

                # Fully sold -> stop tracking + cancel any SELLs
                if qty <= 0:
                    if st.tracking:
                        cancel_existing_sells(sym, open_orders)
                        st.open_sell_id = st.open_sell_px = st.open_sell_qty = None
                        st.tracking = False
                        print(f"[INFO] Stopped tracking {sym} (qty=0).")
                    last_qty[sym] = 0.0
                    continue

                # Start tracking on first sight of qty>0
                if not st.tracking:
                    st.tracking = True
                    print(f"[INFO] Start tracking {sym} qty={qty} avg={avg}")

                # Manual BUY executed → qty increased
                if qty > prev_qty:
                    # Best-effort confirm via latest trade (optional)
                    _latest = snapshot_latest_trade(sym)
                    # Enforce: exactly one SELL at new avg/qty
                    cancel_existing_sells(sym, open_orders)
                    st.open_sell_id = st.open_sell_px = st.open_sell_qty = None
                    oid, px, oq = ensure_one_sell(sym, qty, avg)
                    st.open_sell_id, st.open_sell_px, st.open_sell_qty = oid, px, oq

                # Qty unchanged → ensure our SELL matches updated avg (if avg moved)
                elif qty == prev_qty:
                    target_px = round_to_tick(avg + 0.35, TICK_SIZE)
                    need_refresh = True
                    for o in open_orders.get(sym, []):
                        if (o.get("action", "").upper() == "SELL" and
                            float(o.get("quantity", 0) or 0) == qty and
                            float(o.get("price", 0) or 0) == target_px and
                            o.get("order_status", "").lower() == "open"):
                            st.open_sell_id = o.get("orderid") or o.get("order_id")
                            st.open_sell_px = target_px
                            st.open_sell_qty = qty
                            need_refresh = False
                            break
                    if need_refresh:
                        cancel_existing_sells(sym, open_orders)
                        st.open_sell_id = st.open_sell_px = st.open_sell_qty = None
                        oid, px, oq = ensure_one_sell(sym, qty, avg)
                        st.open_sell_id, st.open_sell_px, st.open_sell_qty = oid, px, oq

                last_qty[sym] = qty

            # Compact summary every 60s
            now = time.time()
            if now - last_log_ts >= 60:
                print("---- SUMMARY (60s) ----")
                for sym, st in list(SYMBOLS.items()):
                    print(f"{sym}: qty={st.total_qty} avg={st.avg_price} "
                          f"sell={'None' if not st.open_sell_id else f'id={st.open_sell_id}@{st.open_sell_px}x{st.open_sell_qty}'} "
                          f"tracking={st.tracking}")
                print("-----------------------")
                last_log_ts = now

            # Light cadence: internal loop can be a few seconds; logs stay minimal
            time.sleep(3)
        except Exception as e:
            print("[ERROR] supervisor loop:", e)
            time.sleep(3)

# ------------- Main -------------
def main():
    # Seed state
    _ = snapshot_positions_and_holdings()

    # Start supervisor
    t = threading.Thread(target=supervisor, daemon=True)
    t.start()

    # WS connect + dynamic subscriptions to whatever symbols we track
    client.connect()  # official signature: no kwargs

    subscribed: Set[Tuple[str, str]] = set()
    try:
        while not STOP["flag"]:
            # desired = all symbols we currently track
            with STATE_LOCK:
                desired = {(EXCHANGE, s) for s in SYMBOLS.keys() if SYMBOLS[s].tracking}

            # subscribe new
            to_add = desired - subscribed
            if to_add:
                instruments = [{"exchange": ex, "symbol": sym} for (ex, sym) in to_add]
                client.subscribe_ltp(instruments, on_data_received=on_tick)  # official signature
                subscribed |= to_add

            # unsubscribe removed
            to_remove = subscribed - desired
            if to_remove:
                instruments = [{"exchange": ex, "symbol": sym} for (ex, sym) in to_remove]
                try:
                    client.unsubscribe_ltp(instruments)
                except Exception:
                    pass
                subscribed -= to_remove

            time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        if subscribed:
            instruments = [{"exchange": ex, "symbol": sym} for (ex, sym) in subscribed]
            try:
                client.unsubscribe_ltp(instruments)
            except Exception:
                pass
        try:
            client.disconnect()
        except Exception:
            pass
        STOP["flag"] = True
        t.join(timeout=5)
        print("[INFO] Exited cleanly.")

if __name__ == "__main__":
    def _sigint(sig, frame):
        STOP["flag"] = True
    signal.signal(signal.SIGINT, _sigint)
    main()
