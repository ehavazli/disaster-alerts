# disaster-alerts

Cron-friendly email alerts for significant **USGS earthquakes** and **NWS weather events**.
Runs on a schedule, pulls fresh events, filters by your thresholds/AOI, and emails concise HTML + plain-text digests.

---

## Features

- **Multi-provider**: USGS seismic events, NWS watches/warnings/advisories
- **Config-driven**: thresholds, recipients, providers, and optional AOI in `config/*.yaml`
- **Email out of the box**: HTML + text bodies via SMTP (yagmail or standard SMTP)
- **Cron-ready**: single entrypoint script for scheduled runs
- **Tested**: unit tests keep parsing/formatting stable

---

## Repository layout

```
disaster-alerts/
├─ config/                 # YAML configs (thresholds, recipients, providers, optional AOI)
├─ cron/                   # Example crontab snippets (optional)
├─ logs/                   # Runtime logs (gitignored)
├─ scripts/
│  └─ run.sh               # One-shot runner for local/cron
├─ src/
│  └─ disaster_alerts/     # Package source code
├─ tests/                  # pytest test suite
├─ .env.example            # Copy to .env and fill SMTP creds
├─ environment.yml         # Conda environment
├─ pyproject.toml          # Build/deps for pip/uv/poetry
└─ README.md
```

---

## Quick start

### 1. Create a virtual environment

Using Conda (recommended):

```bash
conda env create -f environment.yml
conda activate disaster-alerts
```

Then you can make an editable install using:

```bash
pip install -e .
```

### 2. Configure SMTP & secrets

Copy `.env.example` to `.env` and set your SMTP credentials:

```bash
cp .env.example .env
```

Common values:

```
YAGMAIL_USER=you@example.com
YAGMAIL_APP_PASSWORD=app_password_or_token
```

> For Gmail, create an **app password** and use that instead of your regular password.

### 3. Configure providers, thresholds, and recipients

Edit the YAML files under `config/`:

- `app.yaml` – enable/disable **nws**, **usgs**, set `AOI`, email routing
- `thresholds.yaml` – e.g., USGS minimum magnitude, NWS event types to include
- `recipients.yaml` – recipient groups and email addresses
- `nws_events_list.json` - list of all events available via NWS alerts

Keep configs minimal and explicit; the pipeline only reads what you define.

### 4. Run once

```bash
./scripts/run.sh
```

You should see a small log and (if events match your filters) receive an email.

---

## Schedule with cron

Open your crontab:

```bash
crontab -e
```

Run every 15 minutes, logging to `logs/cron.log`:

```cron
*/15 * * * * /bin/bash -lc 'cd /path/to/disaster-alerts && conda activate disaster-alerts && ./scripts/run.sh >> logs/cron.log 2>&1'
```

Tips:

- Use absolute paths in cron.
- The `-lc` ensures your conda initialization is sourced.
- Rotate `logs/` as needed.

---

## Configuration reference

### USGS
- Typical filters: `min_magnitude`, `time_window`, `aoi`
- Normalized event fields include: `id`, `title`, `magnitude`, `depth_km`, `coordinates`, `updated`, `link`

### NWS
- Typical filters: allowed product types (e.g., *Severe Thunderstorm Warning*, *Flash Flood Warning*, etc.), `aoi`
- Shapes may be polygons or county/zone references

### Recipients
- Define **groups** (keys) that map to one or more email addresses
- Use these groups in the pipeline/tests for clarity

---

## Email format

- **Subject**: Provider + concise event summary
- **HTML + Text body**:
  - Deduplicated, sorted list of new/updated events
  - Core attributes (time, severity, magnitude, location)
  - Links to official pages (USGS event, NWS product)
  - Optional geometry summary (centroid, bbox, polygon size)


---

## Development

Editable install:

```bash
pip install -e .[dev]
```

Where to add things:

- **New providers** → `src/disaster_alerts/providers/`
- **Filters** → `src/disaster_alerts/filters.py` (or a new module)
- **Email rendering** → message builder/templates in `src/disaster_alerts/`

---

## Extending the pipeline

### Add a provider

1. Create `src/disaster_alerts/providers/<name>.py`
2. Implement:
   - `fetch()` → returns raw items
   - `normalize()` → yields internal `Event` dicts with a consistent schema
3. Wire it in the provider registry and `providers.yaml`
4. Add unit tests in `tests/` to lock in parsing behavior

### Add a filter

- Write a boolean function that takes a normalized `Event` and returns `True/False`
- Register it in the pipeline before email formatting
- Add tests to cover edge cases (missing fields, boundary thresholds)

### Customize emails

- Tweak HTML/text templates and the message builder
- Keep both HTML and text in sync for deliverability

---

## Security & Ops

- Secrets live **only** in `.env` (never commit them)
- Avoid logging secrets; keep logs minimal
- Use app-specific passwords or tokens
- Prefer a low-privilege sender identity (e.g., `alerts@yourdomain`)

---

## Troubleshooting

- **No emails?** Check:
  - `logs/` for errors
  - SMTP credentials / network egress / SPF-DKIM
  - Thresholds too strict / AOI excludes all events
- **Cron runs but fails silently**:
  - Use absolute paths
  - Ensure the environment is activated in the cron command
  - Redirect stderr (`2>&1`) to a log
- **HTML shows weird characters**:
  - Ensure UTF-8 when composing and sending
- **Tests fail locally but pass in CI**:
  - Pin package versions via `environment.yml` or `pyproject.toml`

---

## License

Apache-2.0 (see `LICENSE`).

---
