# PeMS Pipeline

Databricks Lakeflow pipeline for processing PeMS traffic count data and
producing analysis-ready weekday traffic counts.

## Pipeline

| Layer  | Table                      | Source file                                      |
| ------ | -------------------------- | ------------------------------------------------ |
| Bronze | `bronze_raw_pems`          | `transformations/01-bronze_raw_pems.py`          |
| Silver | `silver_pems_station_days` | `transformations/02-silver_pems_station_days.py` |
| Silver | `silver_pems_quality`      | `transformations/03-silver_pems_quality.py`      |
| Gold   | `gold_pems_weekday_counts` | `transformations/04-gold_pems_weekday_counts.py` |

```text
PeMS-Pipeline/
├── databricks.yml          # Databricks Asset Bundle and targets
├── resources/pipeline.yml  # Lakeflow Declarative Pipeline definition
├── transformations/        # pipeline source files (run on Databricks only)
├── reports/pems_quality/   # Quarto QA/QC report source
├── analysis/               # station volume trends map script
├── scripts/                # local helper scripts
└── docs/                   # rendered outputs published to GitHub Pages
```

## Outputs

- **QA/QC report** — <https://sandag.github.io/PeMS-Pipeline/pems_quality/>,
  built from `reports/pems_quality/`. Summarizes `silver_pems_quality` at
  the station-day level and `bronze_raw_pems` for raw 5-min records.
- **Station volume trends map** —
  <https://sandag.github.io/PeMS-Pipeline/station_volume_trends/>, built by
  `analysis/station_volume_trends.py`. Shows 2022–2025 weekday volume trends
  by station, with a switch to color stations by average weekday speed.

Both are rendered locally into `docs/` and published from there.

---

## Setup (new clone / new machine)

### 1. Prerequisites

Install on Windows:

- Git
- Visual Studio Code, with the **Python** and **Databricks** extensions
- [uv](https://docs.astral.sh/uv/) — manages Python and the virtual environment:
  ```powershell
  winget install astral-sh.uv
  ```
- Databricks CLI (not the legacy `pip install databricks-cli`):
  ```powershell
  winget install Databricks.DatabricksCLI
  ```
- [Quarto](https://quarto.org/docs/get-started/) — only needed to render the QA/QC report

Close and reopen VS Code / the terminal after installing, then check:

```powershell
uv --version
databricks -v
```

You do not need to install Python yourself; uv downloads a matching
version (`requires-python` in `pyproject.toml`) if one isn't found.

### 2. Clone the repository

```powershell
cd C:\dev
git clone https://github.com/SANDAG/PeMS-Pipeline.git
cd C:\dev\PeMS-Pipeline
code .
```

### 3. Create the Python environment

From the repository root:

```powershell
uv sync
```

This creates `.venv` and installs the exact versions pinned in `uv.lock`.
Re-run it whenever `pyproject.toml` or `uv.lock` changes. Use `uv run <cmd>`
to run anything inside the environment — no activation needed. To add a
dependency, use `uv add <package>` (it updates both `pyproject.toml` and
`uv.lock`; commit both).

In VS Code, press `Ctrl+Shift+P` → **Python: Select Interpreter** and pick
`.venv\Scripts\python.exe`.

Register the Jupyter kernel the Quarto report uses (one-time, and again any
time `.venv` is deleted or recreated):

```powershell
uv run python scripts/setup_quarto_kernel.py
```

The report needs its own `pems-quarto` kernel because the Databricks VS Code
extension installs a global IPython startup script that breaks Quarto's
plain code execution; see the comment at the top of the script.

`.venv` is git-ignored and should not be committed.

### 4. Authenticate to Databricks

Get the workspace URL from the team (not from someone's personal
credential file), then log in to the `dev` profile:

```powershell
databricks auth login --host https://<YOUR-DATABRICKS-WORKSPACE-HOST> --profile dev
databricks current-user me
```

`current-user me` should return your own account. If it returns
`Unauthorized network access to workspace`, connect to the corporate
network/VPN and retry — it's a network problem, not a pipeline error.

The bundle, the report, and the analysis script use the `dev` profile by
default. If a login expires, check and refresh that profile specifically:

```powershell
databricks auth profiles
databricks auth login --profile dev
```

Plain `databricks auth login` may refresh a different profile for the same
workspace (e.g. one named after your email) and leave `dev` expired. `Valid`
can also show `NO` for every profile while off VPN.

### 5. Create `.env`

The report and analysis script read Databricks settings from a `.env` file
in the repository root (git-ignored), passed in with `uv run --env-file .env`.
Copy `example.env` as a starting point:

```text
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse id>
DATABRICKS_CONFIG_PROFILE=dev
```

| Variable                    | Default                       | Notes                                                        |
| --------------------------- | ----------------------------- | ------------------------------------------------------------ |
| `DATABRICKS_HTTP_PATH`      | *(required)*                  | Databricks > SQL Warehouses > Connection details             |
| `DATABRICKS_CONFIG_PROFILE` | `dev`                         | Any valid profile from `databricks auth profiles`            |
| `PEMS_CATALOG`              | `sandbox`                     | Set to `travel_data` to read the deployed pipeline           |
| `PEMS_SCHEMA`               | your Databricks username      | Matches the `sandbox.<short_name>` dev target; `pems` for `travel_data` |

Always passing `--env-file .env` avoids logging in to the wrong account if
you have more than one Databricks profile.

---

## Running the pipeline

The `dev` target (the default) deploys to your own user folder and writes
tables to `sandbox.<your username>`. The pipeline resource key is
`pems_pipeline5` (see `resources/pipeline.yml`).

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run pems_pipeline5 -t dev
```

`deploy` only syncs files and creates/updates resources; `run` is what
populates the tables. Add `--debug` to `deploy` when troubleshooting.

Do not run individual files under `transformations/` with **Run File as
Workflow** — files that use `from pyspark import pipelines as dp` only work
as part of the Lakeflow pipeline. Also don't hand-edit the deployed copies
under `/Workspace/.../files`; the next deploy overwrites them.

### Typical workflow

```powershell
git checkout main
git pull origin main
# edit Python / YAML
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run pems_pipeline5 -t dev
# check the results in Databricks, then commit and push
```

GitHub is the source of truth for code; the bundle deploys it; Databricks
runs the pipeline; Unity Catalog holds the resulting tables.

### Other useful commands

```powershell
databricks bundle summary -t dev       # resources the bundle manages
databricks bundle sync -t dev          # sync files without deploying
```

---

## QA/QC report

With the pipeline run (so `bronze_raw_pems` and `silver_pems_quality` exist
under your schema) and `.env` set up:

```powershell
uv run --env-file .env quarto render reports/pems_quality
```

Output goes to `docs/pems_quality/`. Rendering is the step that reads and
executes the SQL files in `reports/pems_quality/sql/` against your warehouse.

To report on tables outside `sandbox.<your username>` (e.g. the deployed
`travel_data.pems`), set `PEMS_CATALOG` / `PEMS_SCHEMA` in `.env` instead of
running the pipeline yourself.

### After editing `sql/*.sql`

`_quarto.yml` uses `execute: freeze: auto`, which only re-runs the code
when `index.qmd` itself changes — Quarto doesn't know the code reads
external `.sql` files, so it will silently show cached results. Clear the
freeze cache before rendering:

```powershell
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue reports/pems_quality/_freeze, reports/pems_quality/.quarto/_freeze
uv run --env-file .env quarto render reports/pems_quality
```

## Station volume trends map

Builds the map (`docs/station_volume_trends/index.html`) and CSVs (in
`analysis/`, git-ignored):

```powershell
uv run --env-file .env python analysis/station_volume_trends.py
```

---

## Databricks bundle notes

### `bind` and `unbind`

A bundle tracks which real Databricks pipeline its resource key
(`pems_pipeline5`) manages.

- **Unbind** to stop the bundle managing a pipeline *without deleting it*:
  ```powershell
  databricks bundle deployment unbind pems_pipeline5 -t dev
  ```
  Don't unbind just because code changed — redeploy instead. Consider it
  when a deploy is unexpectedly trying to update, recreate, or delete a
  pipeline you want to keep.
- **Bind** to bring an existing pipeline (e.g. one created in the UI) under
  the bundle instead of creating a new one:
  ```powershell
  databricks bundle deployment bind pems_pipeline5 <PIPELINE-ID> -t dev
  ```

### Table ownership

Stopping or unbinding a pipeline does **not** release ownership of its
streaming tables or materialized views. A managed table can belong to only
one pipeline, so a new pipeline writing to the same table (e.g.
`travel_data.pems.gold_pems_weekday_counts`) will be rejected. When testing
a replacement pipeline, use a separate schema or different table names until
the migration is complete.

### Shared deployment paths

The `dev` target deploys to
`/Workspace/Users/<you>/.bundle/<bundle>/dev`, so developers don't
overwrite each other. If a target is ever pointed at a shared `root_path`
(e.g. `/Workspace/Shared/PeMS-Pipeline`), everyone deploying to it replaces
each other's code — coordinate deployments, and prefer per-developer `dev`
targets plus a controlled shared/staging/prod target. A
`databricks bundle validate` warning that `/Workspace/Shared` is writable by
all users is a permissions issue to review with the team.
