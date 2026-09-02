# PeMS Pipeline: New Developer Setup

This guide explains the exact steps a colleague should follow after
cloning the `PeMS-Pipeline` Git repository to a Windows computer and
opening it in VS Code.

## 1. Prerequisites

Install the following before starting:

-   Git
-   Visual Studio Code
-   Python 3.12
-   Databricks VS Code extension
-   Python VS Code extension
-   Databricks CLI
  

## 2. Clone the Git Repository

Choose a local development directory:

``` powershell
cd C:\dev
```

Clone the repository:

``` powershell
git clone https://github.com/SANDAG/PeMS-Pipeline.git
```

Enter the repository:

``` powershell
cd C:\dev\PeMS-Pipeline
```

Open it in VS Code:

``` powershell
code .
``` 

You should see `origin` pointing to the PeMS-Pipeline GitHub repository.

Check the current branch:

``` powershell
git branch --show-current
```

Pull the latest version:

``` powershell
git pull origin main
```

------------------------------------------------------------------------

## 3. Create a Local Python Environment

From the repository root:

``` powershell
cd C:\dev\PeMS-Pipeline
python3.12 -m venv .venv
```

If necessary, use:

``` powershell
py -3.12 -m venv .venv
```

Activate the environment:

``` powershell
.\.venv\Scripts\Activate.ps1
```

The terminal should now start with:

``` text
(.venv) PS C:\dev\PeMS-Pipeline>
```

Verify:

``` powershell
python --version
```

Select the same interpreter in VS Code:

1.  Press `Ctrl+Shift+P`.
2.  Run `Python: Select Interpreter`.
3.  Select:

``` text
C:\dev\PeMS-Pipeline\.venv\Scripts\python.exe
```

The `.venv` directory is local and should not be committed to Git.

------------------------------------------------------------------------

## 4. Install VS Code Extensions

In VS Code, install:

-   Databricks
-   Python

Open the repository root as the VS Code workspace.

The project should contain files similar to:

``` text
PeMS-Pipeline/
├── databricks.yml
├── resources/
│   └── pipeline.yml
└── transformations/
    ├── 01-bronze_raw_pems.py
    ├── 02-silver_pems_weekday_counts.py
    ├── 03-gold_pems_weekday_counts.py
    └── holidays_2025.py
```

`databricks.yml` defines the Databricks Asset Bundle and target.

`resources/pipeline.yml` defines the Lakeflow Declarative Pipeline.

`transformations/` contains the Python pipeline source files.

------------------------------------------------------------------------

### Install the Databricks CLI

If the `databricks` command is not available, install the current
Databricks CLI:

``` powershell
winget install Databricks.DatabricksCLI
```

Close and reopen VS Code after installation.

Verify:

``` powershell
databricks -v
```

Do not use the legacy Python package `pip install databricks-cli` for
this bundle workflow.

------------------------------------------------------------------------


## 5. Authenticate to Databricks

The workspace URL should be obtained from the team's approved
configuration/documentation rather than copied from a personal
credential file.

Authenticate:

``` powershell
databricks auth login --host https://<YOUR-DATABRICKS-WORKSPACE-HOST>
```

For example, replace `<YOUR-DATABRICKS-WORKSPACE-HOST>` with the team's
Azure Databricks workspace hostname.

Complete the browser authentication when prompted.

Verify that the CLI can identify you:

``` powershell
databricks current-user me
```

The returned `userName` should be your own Databricks account.

If this returns:

``` text
Unauthorized network access to workspace
```

connect to the required corporate network/VPN and retry. This is a
workspace/network-access problem, not a Python pipeline error.

------------------------------------------------------------------------

## 6. Validate the Bundle

Always validate before deploying:

``` powershell
cd C:\dev\PeMS-Pipeline
databricks bundle validate -t dev
```

A successful validation should identify the bundle, target, workspace,
and current user.

A warning about `/Workspace/Shared` being writable by workspace users is
a permissions warning; review it with the team rather than ignoring it
for production deployments.

------------------------------------------------------------------------

## 7. Inspect the Bundle Before Deployment

Optional but useful:

``` powershell
databricks bundle summary -t dev
```

This shows the resources the bundle intends to manage.

The pipeline resource key is the YAML key under:

``` yaml
resources:
  pipelines:
    pems_pipeline:
```

In this example, the resource key used by CLI commands is:

``` text
pems_pipeline
```

Use the actual key in the repository if it differs.

------------------------------------------------------------------------

## 8. Deploy the Bundle

Deploy the current local bundle:

``` powershell
databricks bundle deploy -t dev
```

This synchronizes the bundle files and creates or updates the Databricks
resources defined by the bundle.

Do not manually edit the deployed Python files under
`/Workspace/.../files`. They are deployment artifacts and may be
overwritten by the next bundle deployment.

If deployment troubleshooting is needed:

``` powershell
databricks bundle deploy -t dev --debug
```

------------------------------------------------------------------------

## 9. Run the Pipeline

After deployment succeeds, run the pipeline resource:

``` powershell
databricks bundle run pems_pipeline -t dev
```

If the repository uses a different resource key, replace `pems_pipeline`
accordingly.

Do **not** run individual Lakeflow pipeline source files using
`Run File as Workflow` when they contain code such as:

``` python
from pyspark import pipelines as dp
```

Those files are intended to run as part of the Lakeflow Declarative
Pipeline.

------------------------------------------------------------------------

## 10. Typical Development Workflow

Before starting work:

``` powershell
cd C:\dev\PeMS-Pipeline
git checkout main
git pull origin main
```

Edit the Python/YAML files in VS Code.

Validate:

``` powershell
databricks bundle validate -t dev
```

Deploy:

``` powershell
databricks bundle deploy -t dev
```

Run:

``` powershell
databricks bundle run pems_pipeline -t dev
```

Check the pipeline result in Databricks. 

------------------------------------------------------------------------

## 11. Useful Databricks Bundle Commands

Check CLI version:

``` powershell
databricks -v
```

Check authentication:

``` powershell
databricks current-user me
```

Validate the bundle:

``` powershell
databricks bundle validate -t dev
```

Summarize bundle resources:

``` powershell
databricks bundle summary -t dev
```

Synchronize files:

``` powershell
databricks bundle sync -t dev
```

Deploy:

``` powershell
databricks bundle deploy -t dev
```

Deploy with debugging output:

``` powershell
databricks bundle deploy -t dev --debug
```

Run the pipeline:

``` powershell
databricks bundle run pems_pipeline -t dev
```

List deployed workspace files, adjusting the path to match `root_path`:

``` powershell
databricks workspace list /Workspace/Shared/PeMS-Pipeline/files
```

------------------------------------------------------------------------

## 12. `bind` and `unbind`

A bundle maintains a relationship between its resource key and an actual
Databricks pipeline.

Conceptually:

``` text
Bundle resource "pems_pipeline"
        |
        v
Existing Databricks Pipeline
```

### Unbind

Use `unbind` when the bundle should stop managing an existing pipeline
**without deleting the pipeline itself**:

``` powershell
databricks bundle deployment unbind pems_pipeline -t dev
```

After unbinding, the old pipeline remains in Databricks. It can still
exist, run, and retain ownership of its managed tables.

Do **not** unbind just because Python code changed. For normal code
changes, keep the binding and redeploy:

``` powershell
databricks bundle deploy -t dev
databricks bundle run pems_pipeline -t dev
```

Consider `unbind` when deployment is unexpectedly trying to update,
recreate, or delete an old pipeline that you intentionally want to
preserve.

### Bind

If an existing Databricks pipeline should become managed by the bundle,
bind the bundle resource to its pipeline ID:

``` powershell
databricks bundle deployment bind pems_pipeline <PIPELINE-ID> -t dev
```

Use the real Databricks pipeline ID in place of `<PIPELINE-ID>`.

Binding is useful when migrating a pipeline that was originally created
in the Databricks UI and you want to preserve that remote pipeline
rather than create another one.

------------------------------------------------------------------------

## 13. Pipeline Table Ownership

Stopping or unbinding a pipeline does **not** automatically release
ownership of its streaming tables or materialized views.

For example:

``` text
Old Pipeline
     |
     v
travel_data.pems.gold_pems_weekday_counts
```

If a new pipeline tries to manage the same table, Databricks can reject
the run because a managed table can only belong to one pipeline.

For testing a replacement pipeline, use a separate schema or different
output table names until the migration is intentionally completed.

------------------------------------------------------------------------

## 14. Git and Databricks Responsibilities

The intended workflow is:

``` text
GitHub Repository
      |
      | git pull
      v
Local Git Repository in VS Code
      |
      | edit Python / YAML files
      v
Validate Bundle Configuration
      |
      | databricks bundle validate
      | databricks bundle deploy
      | databricks bundle run
      v
Pipeline Executes on Databricks
      |
      v
Unity Catalog Tables
      |
      | verify successful results
      v
Commit Changes in VS Code
      |
      | git push
      v
GitHub Repository
```

GitHub is the source of truth for project code.

VS Code is the local development environment.

The Databricks Bundle deploys the code and resource configuration.

Databricks executes the Lakeflow pipeline.

Unity Catalog stores/manages the resulting tables.

------------------------------------------------------------------------

## 15. Important Team Note About a Shared `root_path`

If all developers use the same configuration, for example:

``` yaml
workspace:
  root_path: /Workspace/Shared/PeMS-Pipeline
```

then everyone is deploying to the **same bundle location and pipeline
resources**.

This means one developer's deployment can replace another developer's
deployed source code.

Coordinate deployments when sharing a development target.

For a larger team, prefer separate developer-specific `dev` deployments
and a controlled shared/staging/production target.

------------------------------------------------------------------------
