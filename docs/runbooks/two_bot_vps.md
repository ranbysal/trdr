# Two-Bot VPS Layout

This repository now supports two independent bot entrypoints:

- `trader-v1` for the deterministic signal-only baseline.
- `prop-v2` for the isolated prop-fund scaffold.

Recommended directories:

- `configs/trader_v1`
- `configs/prop_v2`
- `state/trader_v1`
- `state/prop_v2`
- `out/trader_v1`
- `out/prop_v2`

Recommended commands:

```bash
trader-v1 signals --config-dir configs/trader_v1 --out out/trader_v1 --state-dir state/trader_v1
prop-v2 bootstrap --config-dir configs/prop_v2 --out out/prop_v2 --state-dir state/prop_v2
```

Systemd unit scaffolds are provided in `services/trader_v1.service` and `services/prop_v2.service`.

