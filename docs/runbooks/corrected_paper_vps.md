# Corrected Paper VPS Runbook

The corrected paper executor is isolated from Exec V3 and real-money execution. It consumes only CORR-V4 execution events from `out/corrected_v4/execution_events.ndjson` and writes corrected paper artifacts under `state/corrected_paper` and `out/corrected_paper`.

## Local Verification

```bash
corrected-paper validate-config --config-dir configs/corrected_paper
corrected-paper run-once --config-dir configs/corrected_paper
corrected-paper status --config-dir configs/corrected_paper
```

## VM Deployment

Runtime target: `/srv/trdr` on the Google Cloud VM.

After pushing changes from Codespaces and pulling them on the VM:

```bash
cd /srv/trdr
git pull
.venv/bin/python -m pip install -e .
TRDR_HOME=/srv/trdr scripts/deploy_corrected_paper_vps.sh
```

This does not require `sudo` or `systemd`. The deploy script starts the foreground launcher with `nohup`:

```bash
TRDR_HOME=/srv/trdr nohup scripts/run_corrected_paper.sh \
  >out/corrected_paper/logs/nohup_stdout.log \
  2>out/corrected_paper/logs/nohup_stderr.log &
```

From another machine with SSH access, you can run the same VM-side deployment command as:

```bash
VPS_TARGET=user@host scripts/deploy_corrected_paper_vps.sh
```

## VM Startup Script

Use the same launcher path from a Google Cloud VM startup-script:

```bash
cd /srv/trdr
TRDR_HOME=/srv/trdr scripts/deploy_corrected_paper_vps.sh
```

## VM Artifact Checks

```bash
cd /srv/trdr
TRDR_HOME=/srv/trdr scripts/verify_corrected_paper_vps.sh
```

Runtime files:

- `out/corrected_paper/corrected_paper.pid`
- `out/corrected_paper/logs/nohup_stdout.log`
- `out/corrected_paper/logs/nohup_stderr.log`
- `state/corrected_paper/corrected_paper_state.json`
- `out/corrected_paper/events.ndjson`
- `out/corrected_paper/corrected_paper_journal.ndjson`
- `out/corrected_paper/reports/summary.json`
- `out/corrected_paper/reports/closed_trades.csv`

The status and reports are factual paper accounting only. They do not indicate profitability.
