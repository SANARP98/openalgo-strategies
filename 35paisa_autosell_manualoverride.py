#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenAlgo auto-seller (SmartOrder-optimized):
- You buy manually.
- Script keeps exactly one SELL LIMIT per symbol at (avg + ₹0.35) for total qty (positions + holdings).
- Uses SmartOrder with position_size guardrail to avoid overselling during rapid manual trades.
- Prefers modifyorder over cancel+re-place when only price changes.
- Symbol-specific tick size & lot size cached from master.
- Smarter reconciliation cadence (separate, slower orderbook sweep; targeted orderstatus lookups).
- Stops tracking when qty -> 0.
- Summary every 60s. Quiet otherwise.

Requires:
  pip install openalgo python-dotenv

Notes:
- Equity & derivatives quantities are coerced to valid integers / lot multiples.
- No quotes/depth printing; WebSocket kept quiet by design.
"""

import os
import time
import threading
import signal
from typing import Dict, Tuple, List, Set, Optional
import dotenv
from openalgo import api

dotenv.load_dotenv()

API_KEY   = os.getenv("API_KEY", "").strip()
HOST      = os.getenv("OPENALGO_HOST", "https://openalgo.rpinj.shop").strip()
WS_URL    = os.getenv("OPENALGO_WS", "wss://openalgows.rpinj.shop").strip()
EXCHANGE  = os.getenv("OPENALGO_EXCHANGE", "NSE").strip().upper()
PRODUCT   = os.getenv("OPENALGO_PRODUCT", "CNC").strip().upper()
TICK_SIZE = float(os.getenv("OPENALGO_TICK", "0.05"))   # fallback only

client = api(api_key=API_KEY, host=HOST, ws_url=WS_URL)

# ---------------- Helpers ----------------
SYMBOL_META: Dict[str, Dict[str, float]] = {}  # sym -> {"tick": float, "lotsize": int}

def get_symbol_meta(symbol: str) -> Dict[str, float]:
    meta = SYMBOL_META.get(symbol)
    if meta is None:
        tick = TICK_SIZE
        lotsize = 1
        try:
            resp = client.symbol(symbol=symbol, exchange=EXCHANGE)
            if isinstance(resp, dict) and resp.get("status") == "success":
                data = resp.get("data", {})
                tick = float(data.get("tick_size", tick) or tick)
                lotsize = int(data.get("lotsize", lotsize) or lotsize)
        except Exception:
            pass
        meta = {"tick": float(tick), "lotsize": int(lotsize)}
        SYMBOL_META[symbol] = meta
    return meta

def round_to_tick_px(symbol: str, price: float) -> float:
    tick = get_symbol_meta(symbol)["tick"]
    # Round to exchange tick; keep 2 decimals for rupee quotes
    return round(round(price / tick) * tick, 2)

def normalize_quantity(symbol: str, qty: float) -> int:
    """Return a valid integer quantity in lot multiples (>=0)."""
    lotsize = max(1, int(get_symbol_meta(symbol)["lotsize"]))
    q = int(qty)
    # Snap down to nearest lot multiple (avoid accidental oversell)
    q = (q // lotsize) * lotsize
    return max(0, q)

def weighted_avg(pairs: List[Tuple[float, float]]) -> float:
    num = sum(q * a for q, a in pairs)
    den = sum(q for q, _ in pairs)
    return (num / den) if den > 0 else 0.0

# ---------------- State ----------------
class SymbolState:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.total_qty: float = 0.0
        self.avg_price: float = 0.0
        self.open_sell_id: Optional[str] = None
        self.open_sell_px: Optional[float] = None
        self.open_sell_qty: Optional[int] = None
        self.tracking: bool = False
        # If True, we will NOT auto-modify/cancel the current SELL price while qty is unchanged
        self.respect_manual_price: bool = False

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
        if isinstance(pos, dict) and pos.get("status") == "success":
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
        if isinstance(h, dict) and h.get("status") == "success":
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

# Cached orderbook (polled less often)
ORDERBOOK_CACHE: Dict[str, List[dict]] = {}
ORDERBOOK_TS = 0.0
ORDERBOOK_SWEEP_SECS = 20  # poll orderbook 2-3x/min


def snapshot_open_orders(force: bool = False) -> Dict[str, List[dict]]:
    """Returns dict symbol -> list of OPEN orders for our EXCHANGE/PRODUCT (cached)."""
    global ORDERBOOK_TS, ORDERBOOK_CACHE
    now = time.time()
    if (now - ORDERBOOK_TS < ORDERBOOK_SWEEP_SECS) and not force:
        return ORDERBOOK_CACHE
    out: Dict[str, List[dict]] = {}
    try:
        ob = client.orderbook()
        if isinstance(ob, dict) and ob.get("status") == "success":
            for o in ob.get("data", {}).get("orders", []):
                if (
                    o.get("exchange", "").upper() == EXCHANGE and
                    o.get("product", "").upper() == PRODUCT and
                    str(o.get("order_status", "")).lower() == "open"
                ):
                    sym = o.get("symbol", "").upper()
                    out.setdefault(sym, []).append(o)
    except Exception as e:
        print("[WARN] orderbook error:", e)
    ORDERBOOK_CACHE = out
    ORDERBOOK_TS = now
    return out


def get_order_status(order_id: str) -> Optional[dict]:
    try:
        resp = client.orderstatus(order_id=str(order_id))
        if isinstance(resp, dict) and resp.get("status") == "success":
            return resp.get("data")
    except Exception as e:
        print(f"[WARN] orderstatus error id={order_id}:", e)
    return None


# ------------- Actions -------------
def cancel_existing_sells(symbol: str, open_orders_by_symbol: Dict[str, List[dict]]):
    """Cancels all OPEN SELL orders for symbol (uses cancelorder(order_id=...))."""
    for o in open_orders_by_symbol.get(symbol, []):
        if str(o.get("action", "")).upper() == "SELL":
            oid = o.get("orderid") or o.get("order_id")
            if not oid:
                continue
            try:
                client.cancelorder(order_id=str(oid))
                print(f"[ACTION] Cancelled SELL {symbol} (order_id={oid})")
            except Exception as e:
                print(f"[WARN] Failed to cancel SELL for {symbol} id={oid}: {e}")


def ensure_one_sell_smart(symbol: str, qty: float, avg_price: float) -> Tuple[Optional[str], Optional[float], Optional[int]]:
    """
    Places a single SELL LIMIT @ avg+0.35 for the full qty using SmartOrder.
    Uses position_size=qty as an additional guardrail.
    Returns (order_id, price, qty_int).
    """
    qty_int = normalize_quantity(symbol, qty)
    if qty_int <= 0:
        return (None, None, None)

    px = round_to_tick_px(symbol, avg_price + 0.35)
    try:
        resp = client.placesmartorder(
            strategy="auto_seller",
            symbol=symbol,
            action="SELL",
            exchange=EXCHANGE,
            price_type="LIMIT",
            product=PRODUCT,
            quantity=qty_int,
            position_size=qty_int,   # respects current position target
            price=str(px),
        )
        oid = None
        if isinstance(resp, dict):
            oid = (
                resp.get("orderid")
                or resp.get("order_id")
                or resp.get("data", {}).get("orderid")
                or resp.get("data", {}).get("order_id")
            )
        print(f"[ACTION] Smart SELL {symbol} qty={qty_int} @ {px} (order_id={oid})")
        return (str(oid) if oid else None, px, qty_int)
    except Exception as e:
        print(f"[ERROR] Smart SELL failed for {symbol} qty={qty_int}: {e}")
        return (None, None, None)


def modify_sell_price_if_needed(symbol: str, order_id: str, new_px: float) -> bool:
    """Try to modify existing SELL LIMIT price instead of cancel+re-place."""
    try:
        resp = client.modifyorder(
            order_id=str(order_id),
            strategy="auto_seller",
            symbol=symbol,
            action="SELL",
            exchange=EXCHANGE,
            price_type="LIMIT",
            product=PRODUCT,
            price=str(new_px),
        )
        ok = isinstance(resp, dict) and resp.get("status") == "success"
        if ok:
            print(f"[ACTION] Modified SELL {symbol} to @{new_px} (order_id={order_id})")
        return ok
    except Exception as e:
        print(f"[WARN] modifyorder failed for {symbol} id={order_id}: {e}")
        return False


# ------------- WebSocket (quiet) -------------
def on_tick(_data: dict):
    # WS kept quiet as requested.
    pass


# ------------- Supervisor -------------
def supervisor():
    """
    - Reconciles positions+holdings (merged), open orders (cached).
    - Detects manual BUY executions (qty increase).
    - Maintains one SELL @ avg+0.35 for full qty using SmartOrder.
    - Prefers modifyorder when qty unchanged and only price target changes.
    - Respects manual price while quantity is unchanged (no auto-modify/cancel).
    - Stops tracking when qty -> 0.
    - Summary log every 60s.
    """
    last_log_ts = 0.0
    last_qty: Dict[str, int] = {}

    while not STOP["flag"]:
        try:
            # Fast path: positions+holdings snapshot each loop (cheap)
            totals = snapshot_positions_and_holdings()   # sym -> (qty, avg)

            # Orderbook cache refreshed at most every ORDERBOOK_SWEEP_SECS
            open_orders = snapshot_open_orders(force=False)

            with STATE_LOCK:
                # Discover active symbols
                for sym in totals.keys():
                    if sym not in SYMBOLS:
                        SYMBOLS[sym] = SymbolState(sym)

            # Per-symbol logic
            for sym in list(SYMBOLS.keys()):
                raw_qty, avg = totals.get(sym, (0.0, 0.0))
                st = SYMBOLS[sym]

                # Normalize qty to valid integer / lot multiple
                qty = normalize_quantity(sym, raw_qty)
                prev_qty = last_qty.get(sym, 0)

                st.total_qty = qty
                st.avg_price = float(avg)

                # Fully sold -> stop tracking + cancel any SELLs
                if qty <= 0:
                    if st.tracking:
                        cancel_existing_sells(sym, open_orders)
                        st.open_sell_id = st.open_sell_px = st.open_sell_qty = None
                        st.tracking = False
                        st.respect_manual_price = False
                        print(f"[INFO] Stopped tracking {sym} (qty=0).")
                    last_qty[sym] = 0
                    continue

                # Start tracking on first sight of qty>0
                if not st.tracking:
                    st.tracking = True
                    st.respect_manual_price = False
                    print(f"[INFO] Start tracking {sym} qty={qty} avg={avg}")

                # Manual BUY executed → qty increased
                if qty > prev_qty:
                    # Best-effort: cancel any stray sells, then place a fresh SmartOrder SELL
                    cancel_existing_sells(sym, open_orders)
                    st.open_sell_id = st.open_sell_px = st.open_sell_qty = None
                    oid, px, oq = ensure_one_sell_smart(sym, qty, avg)
                    st.open_sell_id, st.open_sell_px, st.open_sell_qty = oid, px, oq
                    st.respect_manual_price = False  # qty changed -> we manage price again

                # Qty unchanged → ensure our SELL exists, but respect manual price
                elif qty == prev_qty:
                    target_px = round_to_tick_px(sym, avg + 0.35)

                    # If we have an existing order id, check precise status (price may be stale in cached OB)
                    handled = False
                    if st.open_sell_id:
                        osd = get_order_status(st.open_sell_id)
                        if osd and str(osd.get("order_status", "")).lower() == "open":
                            current_px = float(osd.get("price", 0) or 0)
                            st.open_sell_px = current_px
                            st.open_sell_qty = qty
                            if current_px != target_px:
                                # Respect manual price until quantity changes
                                st.respect_manual_price = True
                                handled = True  # accept manual; do nothing else
                            else:
                                st.respect_manual_price = False
                                handled = True  # already at target; nothing to do
                        else:
                            # Our tracked order is no longer open; fall back to cache-based reconcile
                            st.open_sell_id = None

                    if not handled:
                        # Look for an OPEN SELL in cached orderbook and adopt it
                        adopted = False
                        for o in open_orders.get(sym, []):
                            if (
                                str(o.get("action", "")).upper() == "SELL"
                                and int(float(o.get("quantity", 0) or 0)) == qty
                                and str(o.get("order_status", "")).lower() == "open"
                            ):
                                cur_px = float(o.get("price", 0) or 0)
                                st.open_sell_id = o.get("orderid") or o.get("order_id")
                                st.open_sell_px = cur_px
                                st.open_sell_qty = qty
                                if cur_px != target_px:
                                    # Adopt as a manual override
                                    st.respect_manual_price = True
                                else:
                                    st.respect_manual_price = False
                                adopted = True
                                break

                        if not adopted:
                            # No open SELL found; only place if we're NOT respecting a manual price
                            if not st.respect_manual_price:
                                oid, px, oq = ensure_one_sell_smart(sym, qty, avg)
                                st.open_sell_id, st.open_sell_px, st.open_sell_qty = oid, px, oq

                last_qty[sym] = qty

            # Compact summary every 60s
            now = time.time()
            if now - last_log_ts >= 60:
                print("---- SUMMARY (60s) ----")
                for sym, st in list(SYMBOLS.items()):
                    print(
                        f"{sym}: qty={st.total_qty} avg={st.avg_price} "
                        f"sell={'None' if not st.open_sell_id else f'id={st.open_sell_id}@{st.open_sell_px}x{st.open_sell_qty}'} "
                        f"tracking={st.tracking}"
                    )
                print("-----------------------")
                last_log_ts = now

            time.sleep(3)  # light cadence
        except Exception as e:
            print("[ERROR] supervisor loop:", e)
            time.sleep(3)

# ------------- Main -------------

def main():
    print("🔁 OpenAlgo Python Bot is running.")

    # Seed state
    _ = snapshot_positions_and_holdings()
    # Prime an initial orderbook cache
    _ = snapshot_open_orders(force=True)

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
                client.subscribe_ltp(instruments, on_data_received=on_tick)
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
