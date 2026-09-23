Purpose
-------
Quarto QA/QC report for PeMS data used by the pipeline.
Public Link: https://sandag.github.io/PeMS-Pipeline/pems_quality/

Data flow
---------
| Layer  | Table                      |
| ------ | -------------------------- |
| Bronze | `bronze_raw_pems`          |
| ↓      |                            |
| Silver | `silver_pems_station_days` |
| ↓      |                            |
| Silver | `silver_pems_quality`      |
| ↓      |                            |
| Gold   | `gold_pems_weekday_counts` |


The report primarily summarizes `silver_pems_quality` on station-day level
and `bronze_raw_pems` for raw 5-min records.

The report queries `silver_pems_quality` directly from Databricks via a SQL
warehouse (see the first code cell in `index.qmd`), authenticating with the
`dev` Databricks CLI profile by default. Override with these env vars if
needed:

- `DATABRICKS_CONFIG_PROFILE` (default `dev`)
- `DATABRICKS_HTTP_PATH` (default: the workspace's serverless starter warehouse)
- `PEMS_CATALOG` (default `sandbox`)
- `PEMS_SCHEMA` (default: your own username, matching the `sandbox.<short_name>`
  dev bundle target; set to `pems` once deployed under `travel_data`)

Running end to end (new clone / new machine)
---------------------------------------------
Run from the repository root (`C:\dev\PeMS-Pipeline`).

**0. One-time Databricks auth** (skip if already authenticated):
```bash
databricks auth login --host <your workspace host> --profile dev
```
Use any profile name; set `DATABRICKS_CONFIG_PROFILE` (see below) if not `dev`.

**1. Deploy and run the pipeline** - populates `bronze_raw_pems` and
`silver_pems_quality` under `sandbox.<your Databricks username>` (the
schema this report assumes by default):
```bash
databricks bundle deploy
```
```bash
databricks bundle run pems_pipeline5
```
`deploy` alone does not populate any tables — you need `run` too.

**2. Set up the local environment + Quarto kernel** (one-time, or again any
time `.venv` is deleted/recreated):
```bash
uv sync
```
```bash
uv run python scripts/setup_quarto_kernel.py
```
This registers the `pems-quarto` Jupyter kernel this report uses (see the
comment at the top of that script for why it needs its own kernel, separate
from the default one).

**3. Render the report:**
```bash
uv run quarto render reports/pems_quality
```
This is the step where the SQL files under `sql/` actually get read and
executed against your warehouse — there's no separate command for them.

If `silver_pems_quality` lives somewhere other than
`sandbox.<your username>` (e.g. it's been deployed to `travel_data`/`pems`),
override with these env vars instead of step 1:

- `DATABRICKS_CONFIG_PROFILE` (default `dev`)
- `DATABRICKS_HTTP_PATH` (default: the workspace's serverless starter warehouse)
- `PEMS_CATALOG` (default `sandbox`)
- `PEMS_SCHEMA` (default: your own username, matching the `sandbox.<short_name>`
  dev bundle target; set to `pems` once deployed under `travel_data`)

Editing the SQL files (`sql/*.sql`)
------------------------------------
`execute: freeze: auto` in `_quarto.yml` means Quarto only re-runs the
Python code cell when `index.qmd`'s own text changes - it has no idea the
code reads external `.sql` files, so editing only a `.sql` file and
re-rendering normally will silently keep showing old cached results.
The reliable fix is to clear the freeze cache before rendering:
```powershell
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue reports/pems_quality/_freeze, reports/pems_quality/.quarto/_freeze
uv run quarto render reports/pems_quality
```
Do this any time you change a `.sql` file and want to confirm the report
actually reflects it.

Output
------
docs/pems_quality/