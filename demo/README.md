# Watchdog demo

A one-command, throwaway demo that runs **airflow-plugin-watchdog** on a real
Airflow 3 instance with realistic history pre-seeded, so the dashboard is fully
populated the moment you open it — no need to stand up your own Airflow or wait
for DAGs to accumulate runs.

## Prerequisites

- Docker + Docker Compose (Docker Desktop, [OrbStack](https://orbstack.dev/), or
  any compatible engine).

## Run it

```bash
cd demo
docker compose up
```

The first boot pulls the Airflow image and installs the plugin from PyPI, so give
it a minute. Then open:

**<http://localhost:8080/watchdog/>**

No login required (the demo runs Airflow's SimpleAuthManager in all-admins mode).
The plugin's background scheduler runs its first detection cycle ~15 seconds after
the API server starts, so if the dashboard looks empty for a moment, give it a few
seconds and let it auto-refresh.

When you're done:

```bash
docker compose down -v        # -v also drops the seeded database
```

## What you'll see

The seed data (see [`seed.py`](seed.py)) is shaped so **every detector fires**,
across a mix of severities:

| DAG | Alert | Severity |
|---|---|---|
| `payment_reconciliation` | Failure spike (40% of recent runs failed) | critical |
| `transaction_settlement` | Stuck task (running far past its historical max) | critical |
| `nightly_warehouse_load` | Missed deadline (running well over 2× median) | critical |
| `inventory_sync` | Schedule anomaly (started hours later than usual) | critical |
| `daily_sales_etl` | Runtime anomaly (latest run slower than the IQR fence) | warning |
| `order_ingestion`, `customer_segmentation`, `product_recommendations` | — | healthy |
| `marketing_attribution` | — | healthy (paused) |

Click **Configuration** on the dashboard to explore the in-UI config editor
(detectors, thresholds, alert destinations) — it edits the same `watchdog_config`
Variable the demo seeds.

## Capturing screenshots and the GIF

This demo is the single source for the images in the main README — both the hero
GIF and the Configuration-page screenshots (there is no separate render script).
With the stack up:

- **Hero GIF** — record the browser at <http://localhost:8080/watchdog/> walking
  through the dashboard and the Configuration tabs (macOS `⇧⌘5`, or a GIF tool such
  as [Kap](https://getkap.co/)). To keep the first N seconds of a `.mov` and convert
  it to a GIF with `ffmpeg`:

  ```bash
  ffmpeg -t 36 -i recording.mov \
    -vf "fps=12,scale=1000:-1:flags=lanczos,split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse" \
    -loop 0 /tmp/watchdog-demo.gif
  ```

- **Configuration screenshots** — open the **Configuration** page and capture each
  of the three tabs (Detectors / Thresholds / Alerts).

None of these images are committed to the repo. Upload them to a GitHub issue or
release (drag-and-drop) and reference the resulting `user-attachments` URLs from the
README. Absolute URLs keep the repo lean, stay clear of the `check-added-large-files`
pre-commit hook, and render on the PyPI project page too (where relative paths do not).

## Notes

- This is a **demo, not a production reference**: it uses a single API-server
  process, installs the plugin via `_PIP_ADDITIONAL_REQUIREMENTS`, sets a fixed
  Fernet/JWT key, and disables the login wall. Don't copy these shortcuts into a
  real deployment — see the main [README](../README.md) for production install
  guidance.
- To re-seed from scratch, run `docker compose down -v` and `up` again. The seed
  is idempotent, so a plain restart won't duplicate data.
