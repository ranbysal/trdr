from __future__ import annotations

import numpy as np

from futures_bot.strategies.strategy_d_pair import ar1_half_life, evaluate_pair_signal, ewls_beta, fit_ar1_phi


def test_strategy_d_ewls_ar1_half_life_and_abs_z_proxy() -> None:
    rng = np.random.default_rng(7)
    hedge = np.linspace(100.0, 120.0, 120)
    noise = rng.normal(0.0, 0.2, size=120)
    lead = 1.5 * hedge + noise

    beta = ewls_beta(lead, hedge, lam=0.97)
    assert 1.3 < beta < 1.7

    # Mean-reverting spread proxy for AR(1) sanity.
    spread = np.zeros(120, dtype=float)
    phi_true = 0.85
    for i in range(1, spread.size):
        spread[i] = phi_true * spread[i - 1] + rng.normal(0.0, 0.3)
    phi_hat = fit_ar1_phi(spread)
    hl = ar1_half_life(phi_hat)
    assert 0.0 < abs(phi_hat) < 1.0
    assert 0.0 < hl < 80.0

    # Inject signal by dislocating last point.
    lead_sig = lead.copy()
    lead_sig[-1] = lead_sig[-1] + 4.0
    out = evaluate_pair_signal(
        lead_symbol="MGC",
        hedge_symbol="SIL",
        lead_prices=lead_sig,
        hedge_prices=hedge,
        entry_abs_z=1.0,
        max_abs_z=10.0,
        max_half_life_bars=150.0,
        data_ok=True,
    )
    assert out.approved
    assert out.stop_risk_proxy == abs(out.zscore)
    assert out.half_life_bars > 0.0
