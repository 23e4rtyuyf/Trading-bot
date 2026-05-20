import asyncio
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import httpx
import pandas as pd
import pandas_ta as ta
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware


DATA_URL = "https://data.alpaca.markets"
TRADING_URL = "https://paper-api.alpaca.markets"
TRACKED_SYMBOLS = ["AAPL", "BTC/USD"]
POLL_INTERVAL_SECONDS = 20
AAPL_ORDER_QTY = float(os.getenv("AAPL_ORDER_QTY", "1"))
BTC_ORDER_QTY = float(os.getenv("BTC_ORDER_QTY", "0.001"))
CORS_ORIGINS = [origin.strip() for origin in os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000").split(",") if origin.strip()]


class TradingState:
    def __init__(self) -> None:
        self.starting_balance = 100000.0
        self.starting_balance_initialized = False
        self.current_equity = 100000.0
        self.total_pnl = 0.0
        self.positions: list[dict[str, Any]] = []
        self.candles: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in TRACKED_SYMBOLS}
        self.last_signal: dict[str, str | None] = {symbol: None for symbol in TRACKED_SYMBOLS}
        self.last_price: dict[str, float | None] = {symbol: None for symbol in TRACKED_SYMBOLS}


class SQLiteStore:
    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path)
        self.lock = asyncio.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              qty REAL NOT NULL,
              price REAL,
              reason TEXT NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS equity_history (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              equity REAL NOT NULL
            )
            """
        )
        self.conn.commit()

    async def add_trade(self, symbol: str, side: str, qty: float, price: float | None, reason: str) -> None:
        async with self.lock:
            self.conn.execute(
                "INSERT INTO trades (ts, symbol, side, qty, price, reason) VALUES (?, ?, ?, ?, ?, ?)",
                (datetime.now(timezone.utc).isoformat(), symbol, side, qty, price, reason),
            )
            self.conn.commit()

    async def add_equity(self, equity: float) -> None:
        async with self.lock:
            self.conn.execute(
                "INSERT INTO equity_history (ts, equity) VALUES (?, ?)",
                (datetime.now(timezone.utc).isoformat(), equity),
            )
            self.conn.commit()

    async def recent_trades(self, limit: int = 100) -> list[dict[str, Any]]:
        async with self.lock:
            rows = self.conn.execute(
                "SELECT ts, symbol, side, qty, price, reason FROM trades ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            {"ts": ts, "symbol": symbol, "side": side, "qty": qty, "price": price, "reason": reason}
            for ts, symbol, side, qty, price, reason in rows
        ]


class AlpacaPaperClient:
    def __init__(self) -> None:
        self.key = os.getenv("ALPACA_API_KEY", "")
        self.secret = os.getenv("ALPACA_API_SECRET", "")
        self.headers = {
            "APCA-API-KEY-ID": self.key,
            "APCA-API-SECRET-KEY": self.secret,
        }
        self.http = httpx.AsyncClient(timeout=15)
        self.enabled = bool(self.key and self.secret)

    async def close(self) -> None:
        await self.http.aclose()

    async def get_latest_bar(self, symbol: str) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        try:
            if symbol == "AAPL":
                resp = await self.http.get(
                    f"{DATA_URL}/v2/stocks/{symbol}/bars/latest",
                    headers=self.headers,
                    params={"feed": "iex"},
                )
                resp.raise_for_status()
                bar = resp.json().get("bar")
            else:
                resp = await self.http.get(
                    f"{DATA_URL}/v1beta3/crypto/us/bars/latest",
                    headers=self.headers,
                    params={"symbols": symbol},
                )
                resp.raise_for_status()
                bar = resp.json().get("bars", {}).get(symbol)
            if not bar:
                return None
            return {
                "t": bar["t"],
                "o": float(bar["o"]),
                "h": float(bar["h"]),
                "l": float(bar["l"]),
                "c": float(bar["c"]),
                "v": float(bar.get("v", 0)),
            }
        except Exception:
            return None

    async def get_account(self) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        try:
            resp = await self.http.get(f"{TRADING_URL}/v2/account", headers=self.headers)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    async def get_positions(self) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        try:
            resp = await self.http.get(f"{TRADING_URL}/v2/positions", headers=self.headers)
            resp.raise_for_status()
            positions = []
            for item in resp.json():
                positions.append(
                    {
                        "symbol": item.get("symbol"),
                        "qty": float(item.get("qty", 0)),
                        "avg_entry_price": float(item.get("avg_entry_price", 0)),
                        "market_value": float(item.get("market_value", 0)),
                        "unrealized_pl": float(item.get("unrealized_pl", 0)),
                    }
                )
            return positions
        except Exception:
            return []

    async def place_market_order(self, symbol: str, side: str, qty: float) -> None:
        if not self.enabled:
            return
        time_in_force = "day" if symbol == "AAPL" else "gtc"
        payload = {
            "symbol": symbol,
            "side": side,
            "type": "market",
            "qty": qty,
            "time_in_force": time_in_force,
        }
        try:
            resp = await self.http.post(f"{TRADING_URL}/v2/orders", headers=self.headers, json=payload)
            resp.raise_for_status()
        except Exception:
            return


class ConnectionManager:
    def __init__(self) -> None:
        self.connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self.connections.discard(websocket)

    async def broadcast_json(self, payload: dict[str, Any]) -> None:
        for websocket in list(self.connections):
            try:
                await websocket.send_json(payload)
            except Exception:
                self.disconnect(websocket)


def crossover_signal(candles: list[dict[str, Any]]) -> str | None:
    if len(candles) < 22:
        return None
    frame = pd.DataFrame(candles)
    frame["ema_fast"] = ta.ema(frame["c"], length=8)
    frame["ema_slow"] = ta.ema(frame["c"], length=21)
    prev = frame.iloc[-2]
    curr = frame.iloc[-1]
    if prev["ema_fast"] <= prev["ema_slow"] and curr["ema_fast"] > curr["ema_slow"]:
        return "buy"
    if prev["ema_fast"] >= prev["ema_slow"] and curr["ema_fast"] < curr["ema_slow"]:
        return "sell"
    return None


state = TradingState()
store = SQLiteStore("trading.db")
alpaca = AlpacaPaperClient()
manager = ConnectionManager()
engine_task: asyncio.Task | None = None


@asynccontextmanager
async def app_lifespan(_: FastAPI):
    global engine_task
    if engine_task is None:
        engine_task = asyncio.create_task(engine_loop())
    try:
        yield
    finally:
        if engine_task:
            engine_task.cancel()
        await alpaca.close()


app = FastAPI(title="Alpaca Paper Trading Platform", lifespan=app_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def execute_strategy(symbol: str, signal: str, price: float) -> None:
    if state.last_signal[symbol] == signal:
        return
    qty = AAPL_ORDER_QTY if symbol == "AAPL" else BTC_ORDER_QTY
    await alpaca.place_market_order(symbol=symbol, side=signal, qty=qty)
    await store.add_trade(symbol=symbol, side=signal, qty=qty, price=price, reason="ema_8_21_crossover")
    state.last_signal[symbol] = signal


async def engine_loop() -> None:
    while True:
        for symbol in TRACKED_SYMBOLS:
            bar = await alpaca.get_latest_bar(symbol)
            if not bar:
                continue
            candles = state.candles[symbol]
            if candles and candles[-1]["t"] == bar["t"]:
                candles[-1] = bar
            else:
                candles.append(bar)
                state.candles[symbol] = candles[-500:]
            state.last_price[symbol] = bar["c"]

            signal = crossover_signal(state.candles[symbol])
            if signal:
                await execute_strategy(symbol, signal, bar["c"])

        account = await alpaca.get_account()
        if account:
            state.current_equity = float(account.get("equity", state.current_equity))
            if not state.starting_balance_initialized:
                state.starting_balance = state.current_equity
                state.starting_balance_initialized = True
            state.total_pnl = state.current_equity - state.starting_balance
        state.positions = await alpaca.get_positions()
        await store.add_equity(state.current_equity)

        await manager.broadcast_json(
            {
                "summary": {
                    "startingBalance": state.starting_balance,
                    "currentEquity": state.current_equity,
                    "totalPnL": state.total_pnl,
                },
                "positions": state.positions,
                "prices": state.last_price,
                "candles": state.candles,
            }
        )
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


@app.get("/api/summary")
async def get_summary() -> dict[str, float]:
    return {
        "startingBalance": state.starting_balance,
        "currentEquity": state.current_equity,
        "totalPnL": state.total_pnl,
    }


@app.get("/api/positions")
async def get_positions() -> list[dict[str, Any]]:
    return state.positions


@app.get("/api/history")
async def get_history(symbol: str = "AAPL", limit: int = 200) -> list[dict[str, Any]]:
    candles = state.candles.get(symbol, [])
    return candles[-limit:]


@app.get("/api/trades")
async def get_trades(limit: int = 100) -> list[dict[str, Any]]:
    return await store.recent_trades(limit=limit)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    await websocket.send_json(
        {
            "summary": {
                "startingBalance": state.starting_balance,
                "currentEquity": state.current_equity,
                "totalPnL": state.total_pnl,
            },
            "positions": state.positions,
            "prices": state.last_price,
            "candles": state.candles,
        }
    )
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
