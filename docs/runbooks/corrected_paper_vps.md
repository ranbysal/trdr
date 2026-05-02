# Corrected Paper VPS Runbook

The corrected paper executor is isolated from Exec V3 and real-money execution. It consumes only CORR-V4 execution events from `out/corrected_v4/execution_events.ndjson` and writes corrected paper artifacts under `state/corrected_paper` and `out/corrected_paper`.

## Local Verification

```bash
corrected-paper validate-config --config-dir configs/corrected_paper
corrected-paper run-once --config-dir configs/corrected_paper
corrected-paper status --config-dir configs/corrected_paper
```

## systemd Deployment

```bash
sudo cp services/corrected_paper.service /etc/systemd/system/corrected-paper.service
sudo systemctl daemon-reload
sudo systemctl enable corrected-paper
sudo systemctl restart corrected-paper
sudo systemctl status corrected-paper --no-pager
```

From a workstation with SSH access, the same deployment and verification can be run as:

```bash
VPS_TARGET=user@host TRDR_REMOTE_DIR=/opt/trdr scripts/deploy_corrected_paper_vps.sh
```

## VPS Artifact Checks

```bash
scripts/verify_corrected_paper_vps.sh
```

The status and reports are factual paper accounting only. They do not indicate profitability.
