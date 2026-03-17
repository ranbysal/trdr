# systemd Service Example

Example unit file:

```ini
[Unit]
Description=Futures Bot Signal-Only Live Runner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/workspaces/trdr
EnvironmentFile=/workspaces/trdr/.env
ExecStart=/bin/bash -lc 'source /workspaces/trdr/.env && futures-bot signals --config-dir configs --out out/signals_live'
Restart=always
RestartSec=10
KillSignal=SIGTERM
TimeoutStopSec=30
StandardOutput=append:/workspaces/trdr/out/signals_live/systemd_stdout.log
StandardError=append:/workspaces/trdr/out/signals_live/systemd_stderr.log

[Install]
WantedBy=multi-user.target
```

Safe lifecycle commands:

```bash
sudo systemctl daemon-reload
sudo systemctl enable futures-bot-signals
sudo systemctl start futures-bot-signals
sudo systemctl status futures-bot-signals
sudo systemctl stop futures-bot-signals
sudo systemctl restart futures-bot-signals
```

Primary bot command:

```bash
futures-bot signals --config-dir configs --out out/signals_live
```
