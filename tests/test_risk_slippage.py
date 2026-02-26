from __future__ import annotations

from futures_bot.risk.slippage import estimate_slippage_ticks


def test_slippage_coefficients_and_base_ticks() -> None:
    nq = estimate_slippage_ticks("NQ", 10.0)
    ym = estimate_slippage_ticks("YM", 10.0)
    mgc = estimate_slippage_ticks("MGC", 10.0)
    sil = estimate_slippage_ticks("SIL", 10.0)

    assert nq.slippage_est_ticks == 1.8
    assert ym.slippage_est_ticks == 1.6
    assert mgc.slippage_est_ticks == 2.0
    assert sil.slippage_est_ticks == 3.5
