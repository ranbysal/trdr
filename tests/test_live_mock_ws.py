from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from futures_bot.config.loader import load_instruments
from futures_bot.core.enums import StrategyModule
from futures_bot.live.live_runner import run_live_paper
from futures_bot.live.ws_client import LiveWsClient

websockets = pytest.importorskip("websockets")

ET = ZoneInfo("America/New_York")


def test_ws_client_reconnects_with_mock_server() -> None:
    async def scenario() -> None:
        state = {"connections": 0}

        async def handler(conn):
            state["connections"] += 1
            now = datetime(2026, 1, 12, 9, 30, tzinfo=ET)
            await conn.send(
                json.dumps(
                    {
                        "type": "quote_1s",
                        "timestamp_et": now.isoformat(),
                        "symbol": "NQ",
                        "payload": {"bid": 100.0, "ask": 100.25, "bid_size": 1.0, "ask_size": 1.0},
                    }
                )
            )
            await conn.close()

        server = await websockets.serve(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        client = LiveWsClient(ws_url=f"ws://127.0.0.1:{port}", queue_maxsize=10)
        await client.start()

        received = []
        try:
            async for message in client.messages():
                received.append(message)
                if len(received) >= 2:
                    break
                await asyncio.sleep(0.05)
        finally:
            await client.stop()
            server.close()
            await server.wait_closed()

        assert len(received) >= 2
        assert state["connections"] >= 2

    asyncio.run(scenario())


def test_live_runner_mock_server_smoke(tmp_path: Path) -> None:
    async def scenario() -> None:
        async def handler(conn):
            start = datetime(2026, 1, 12, 9, 30, tzinfo=ET)
            await conn.send(
                json.dumps(
                    {
                        "type": "event",
                        "timestamp_et": start.isoformat(),
                        "symbol": "NQ",
                        "payload": {"code": "LOCKOUT_OFF"},
                    }
                )
            )
            for i in range(8):
                ts = start + timedelta(minutes=i)
                await conn.send(
                    json.dumps(
                        {
                            "type": "quote_1s",
                            "timestamp_et": ts.isoformat(),
                            "symbol": "NQ",
                            "payload": {"bid": 100.0 + i, "ask": 100.25 + i, "bid_size": 1.0, "ask_size": 1.0},
                        }
                    )
                )
                await conn.send(
                    json.dumps(
                        {
                            "type": "bar_1m",
                            "timestamp_et": ts.isoformat(),
                            "symbol": "NQ",
                            "payload": {
                                "open": 100.0 + i,
                                "high": 100.5 + i,
                                "low": 99.5 + i,
                                "close": 100.2 + i,
                                "volume": 1000.0,
                            },
                        }
                    )
                )
                await asyncio.sleep(0.01)
            await conn.close()

        server = await websockets.serve(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        out_dir = tmp_path / "live_out"

        try:
            await run_live_paper(
                ws_url=f"ws://127.0.0.1:{port}",
                out_dir=out_dir,
                instruments_by_symbol=load_instruments("configs"),
                enabled_strategies={StrategyModule.STRAT_A_ORB},
                paper=True,
                max_messages=17,
                max_runtime_s=5.0,
            )
        finally:
            server.close()
            await server.wait_closed()

        assert (out_dir / "live_events.ndjson").exists()
        assert (out_dir / "trade_logs.json").exists() or (out_dir / "live_events.ndjson").stat().st_size > 0

    asyncio.run(scenario())
