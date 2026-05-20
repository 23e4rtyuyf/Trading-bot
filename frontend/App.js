import { useEffect, useMemo, useRef, useState } from "react";
import { createChart } from "lightweight-charts";

const apiBase = process.env.REACT_APP_API_BASE || "http://localhost:8000";

function StatCard({ label, value }) {
  return (
    <div className="rounded-xl bg-slate-900 p-4 shadow">
      <p className="text-sm text-slate-400">{label}</p>
      <p className="mt-2 text-xl font-semibold text-white">{value}</p>
    </div>
  );
}

function App() {
  const [summary, setSummary] = useState({
    startingBalance: 0,
    currentEquity: 0,
    totalPnL: 0,
  });
  const [positions, setPositions] = useState([]);
  const [candles, setCandles] = useState({ AAPL: [], "BTC/USD": [] });
  const [symbol, setSymbol] = useState("AAPL");
  const chartContainerRef = useRef(null);
  const seriesRef = useRef(null);

  useEffect(() => {
    fetch(`${apiBase}/api/summary`)
      .then((res) => res.json())
      .then(setSummary)
      .catch((error) => console.error("Failed to load summary", error));
    fetch(`${apiBase}/api/positions`)
      .then((res) => res.json())
      .then(setPositions)
      .catch((error) => console.error("Failed to load positions", error));
  }, []);

  useEffect(() => {
    const wsBase = apiBase.replace(/^http/, "ws");
    const ws = new WebSocket(`${wsBase}/ws`);
    ws.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (payload.summary) setSummary(payload.summary);
      if (payload.positions) setPositions(payload.positions);
      if (payload.candles) setCandles(payload.candles);
    };
    return () => ws.close();
  }, []);

  useEffect(() => {
    if (!chartContainerRef.current) return;
    const chart = createChart(chartContainerRef.current, {
      height: 320,
      layout: {
        background: { color: "#0f172a" },
        textColor: "#94a3b8",
      },
      grid: {
        vertLines: { color: "#1e293b" },
        horzLines: { color: "#1e293b" },
      },
      rightPriceScale: { borderColor: "#334155" },
      timeScale: { borderColor: "#334155" },
    });
    seriesRef.current = chart.addCandlestickSeries({
      upColor: "#22c55e",
      downColor: "#ef4444",
      borderVisible: false,
      wickUpColor: "#22c55e",
      wickDownColor: "#ef4444",
    });

    const observer = new ResizeObserver(() => {
      chart.applyOptions({ width: chartContainerRef.current.clientWidth });
    });
    observer.observe(chartContainerRef.current);
    chart.applyOptions({ width: chartContainerRef.current.clientWidth });

    return () => {
      observer.disconnect();
      chart.remove();
    };
  }, []);

  const chartData = useMemo(
    () =>
      (candles[symbol] || []).map((candle) => ({
        time: Math.floor(new Date(candle.t).getTime() / 1000),
        open: candle.o,
        high: candle.h,
        low: candle.l,
        close: candle.c,
      })),
    [candles, symbol]
  );

  useEffect(() => {
    if (seriesRef.current) {
      seriesRef.current.setData(chartData);
    }
  }, [chartData]);

  const formatUsd = (value) =>
    new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 2,
    }).format(value || 0);

  return (
    <div className="min-h-screen bg-slate-950 p-6 text-slate-100">
      <div className="mx-auto max-w-6xl space-y-6">
        <h1 className="text-2xl font-bold">Alpaca Paper Trading Dashboard</h1>

        <div className="grid gap-4 md:grid-cols-3">
          <StatCard label="Starting Balance" value={formatUsd(summary.startingBalance)} />
          <StatCard label="Current Equity" value={formatUsd(summary.currentEquity)} />
          <StatCard label="Total Profit / Loss" value={formatUsd(summary.totalPnL)} />
        </div>

        <div className="rounded-xl bg-slate-900 p-4 shadow">
          <div className="mb-4 flex items-center justify-between">
            <h2 className="text-lg font-semibold">Real-Time Price Chart</h2>
            <div className="flex gap-2">
              {["AAPL", "BTC/USD"].map((item) => (
                <button
                  key={item}
                  onClick={() => setSymbol(item)}
                  className={`rounded-md px-3 py-1 text-sm ${
                    symbol === item ? "bg-sky-500 text-white" : "bg-slate-800 text-slate-300"
                  }`}
                >
                  {item}
                </button>
              ))}
            </div>
          </div>
          <div ref={chartContainerRef} className="w-full" />
        </div>

        <div className="rounded-xl bg-slate-900 p-4 shadow">
          <h2 className="mb-3 text-lg font-semibold">Active Positions</h2>
          <div className="overflow-x-auto">
            <table className="min-w-full text-left text-sm">
              <thead className="text-slate-400">
                <tr>
                  <th className="py-2">Symbol</th>
                  <th className="py-2">Qty</th>
                  <th className="py-2">Avg Entry</th>
                  <th className="py-2">Market Value</th>
                  <th className="py-2">Unrealized P/L</th>
                </tr>
              </thead>
              <tbody>
                {positions.length === 0 && (
                  <tr>
                    <td className="py-3 text-slate-500" colSpan={5}>
                      No active positions
                    </td>
                  </tr>
                )}
                {positions.map((position) => (
                  <tr key={position.symbol} className="border-t border-slate-800">
                    <td className="py-2">{position.symbol}</td>
                    <td className="py-2">{position.qty}</td>
                    <td className="py-2">{formatUsd(position.avg_entry_price)}</td>
                    <td className="py-2">{formatUsd(position.market_value)}</td>
                    <td className="py-2">{formatUsd(position.unrealized_pl)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
