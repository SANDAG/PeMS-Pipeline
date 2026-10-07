# %% [markdown]
# Year-over-year daily volume trend for stations common to 2022-2025.
# Run cell by cell (VS Code: "Run Cell" above each `# %%`), or end to end:
#   uv run python analysis/station_volume_trends.py
#   uv run python analysis/station_volume_trends.py --lane-types ML,HV
# Read-only against Databricks; writes a CSV and an HTML map under analysis/.
# --lane-types limits both outputs to those PeMS lane types (ML mainline,
# HV HOV, OR on-ramp, FR off-ramp, FF freeway-to-freeway, ...) and suffixes
# the file names, e.g. station_volume_trends_ML_HV.csv. Default: all types.
# The map also has lane-type checkboxes to filter what is shown.
# station_volume_totals*.csv (and a table on the map) sums the common
# stations' average daily volumes per year by lane type, plus a total row.
#
# Station-day filter: qc_fully_observed (average_percent_observed = 100, no
# imputed 5-min values) AND qc_pass (all 288 five-minute intervals present,
# every period complete) -- the same station-days gold uses.
# Volume per station-year = average of count_day over its passing station-days,
# so years with more/fewer passing days stay comparable.
# Common stations = stations with >= 1 passing station-day in every year.
#
# Trend: compare 2025 with 2022. A station is "decreasing" (red) when its 2025
# volume is more than DECLINE_PCT % below 2022, else "not decreasing" (blue).
# Year-over-year changes are kept in the CSV and map popups for context.

# %%
import argparse
import io
import json
import logging
import os
from pathlib import Path

import pandas as pd
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

logging.getLogger("databricks.sql").setLevel(logging.ERROR)

YEARS = [2022, 2023, 2024, 2025]
DECLINE_PCT = 10.0
METADATA_VOLUME_PATH = (
    "/Volumes/travel_data/pems/raw_pems/station_metadata/d11_text_meta_2022_03_16.txt"
)

LANE_TYPE_NAMES = {
    "ML": "Mainline", "HV": "HOV", "OR": "On-ramp", "FR": "Off-ramp",
    "FF": "Fwy-to-fwy", "CD": "Collector/distributor", "CH": "Conventional hwy",
}

parser = argparse.ArgumentParser()
parser.add_argument(
    "--lane-types",
    default="",
    help="Comma-separated PeMS lane types to keep, e.g. ML,HV (default: all)",
)
# parse_known_args so the script also runs cell by cell in a Jupyter kernel.
args, _ = parser.parse_known_args()
LANE_TYPES = [t.strip().upper() for t in args.lane_types.split(",") if t.strip()]

suffix = "_" + "_".join(LANE_TYPES) if LANE_TYPES else ""
OUT_DIR = Path(__file__).parent
CSV_PATH = OUT_DIR / f"station_volume_trends{suffix}.csv"
TOTALS_PATH = OUT_DIR / f"station_volume_totals{suffix}.csv"
MAP_PATH = OUT_DIR / "plots" / f"station_volume_trend_map{suffix}.html"

cfg = Config(profile=os.environ.get("DATABRICKS_CONFIG_PROFILE", "dev"))
catalog = os.environ.get("PEMS_CATALOG", "sandbox")
schema = os.environ.get("PEMS_SCHEMA")
if schema is None:
    schema = WorkspaceClient(config=cfg).current_user.me().user_name.split("@")[0]
http_path = os.environ.get(
    "DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/bebb2aea2c14f69c"
)

# %% Average daily volume by station and year (passing station-days only)
query = """
SELECT
  station,
  year,
  FIRST(freeway) AS freeway,
  FIRST(direction_of_travel) AS direction,
  FIRST(lane_type) AS lane_type,
  COUNT(*) AS n_days,
  AVG(count_day) AS avg_daily_volume
FROM silver_pems_quality
WHERE qc_pass AND qc_fully_observed
GROUP BY station, year
"""

with sql.connect(
    server_hostname=cfg.host.removeprefix("https://"),
    http_path=http_path,
    credentials_provider=lambda: cfg.authenticate,
) as conn:
    with conn.cursor() as cur:
        cur.execute(f"USE CATALOG {catalog}")
        cur.execute(f"USE SCHEMA {schema}")
        cur.execute(query)
        station_years = pd.DataFrame(
            cur.fetchall(), columns=[d[0] for d in cur.description]
        )

station_years["avg_daily_volume"] = station_years["avg_daily_volume"].astype(float)

if LANE_TYPES:
    station_years = station_years[station_years["lane_type"].isin(LANE_TYPES)]
    print(f"Lane types: {', '.join(LANE_TYPES)}")

# %% Keep stations present in all four years; pivot years to columns
years_per_station = station_years.groupby("station")["year"].nunique()
common = years_per_station[years_per_station == len(YEARS)].index
common_years = station_years[station_years["station"].isin(common)]

volume = common_years.pivot(index="station", columns="year", values="avg_daily_volume")
n_days = common_years.pivot(index="station", columns="year", values="n_days")
attrs = common_years.groupby("station")[["freeway", "direction", "lane_type"]].first()

print(f"Stations with >=1 passing day in any year: {years_per_station.size:,}")
print(f"Stations common to {YEARS[0]}-{YEARS[-1]}: {len(common):,}")

# %% Classify trend
yoy_pct = volume[YEARS].pct_change(axis=1).iloc[:, 1:] * 100
total_pct = (volume[YEARS[-1]] / volume[YEARS[0]] - 1) * 100
trend = (total_pct < -DECLINE_PCT).map({True: "decreasing", False: "not decreasing"})

out = pd.DataFrame(index=volume.index)
out[[str(y) for y in YEARS]] = volume[YEARS].round(0).astype(int)
out[[f"n_days_{y}" for y in YEARS]] = n_days[YEARS].astype(int)
out[[f"pct_change_{a}_{b}" for a, b in zip(YEARS, YEARS[1:])]] = yoy_pct.round(1).values
out[f"pct_change_{YEARS[0]}_{YEARS[-1]}"] = total_pct.round(1)
out["trend"] = trend
out = attrs.join(out)

# %% Station coordinates from PeMS station metadata
w = WorkspaceClient(config=cfg)
with w.files.download(METADATA_VOLUME_PATH).contents as f:
    meta = pd.read_csv(
        io.BytesIO(f.read()), sep="\t", usecols=["ID", "Latitude", "Longitude", "Name"]
    )
meta = meta.rename(
    columns={"ID": "station", "Latitude": "lat", "Longitude": "lon", "Name": "name"}
).set_index("station")

out = out.join(meta, how="left")
missing_xy = out["lat"].isna().sum()
print(f"Common stations missing coordinates: {missing_xy}")

out.index.name = "station"
out.to_csv(CSV_PATH)
print(f"Wrote {CSV_PATH}")
print(out["trend"].value_counts().to_string())

# %% Totals per year by lane type (sum of station average daily volumes)
year_cols = [str(y) for y in YEARS]
by_lane = out.groupby("lane_type").agg(
    n_stations=("trend", "size"),
    n_decreasing=("trend", lambda t: int((t == "decreasing").sum())),
    **{y: (y, "sum") for y in year_cols},
)
totals = pd.concat([by_lane, by_lane.sum().to_frame("Total").T])
totals.index.name = "lane_type"
totals[f"pct_change_{YEARS[0]}_{YEARS[-1]}"] = (
    (totals[year_cols[-1]] / totals[year_cols[0]] - 1) * 100
).round(1)
totals = totals.sort_values("n_stations", ascending=False)
totals = pd.concat([totals.drop(index="Total"), totals.loc[["Total"]]])
totals.to_csv(TOTALS_PATH)
print(f"Wrote {TOTALS_PATH}")
print(totals.to_string())

# %% Map: dot color = trend, dot area = 2025 average daily volume
points = (
    out.dropna(subset=["lat", "lon"])
    .reset_index()
    .assign(vol2025=lambda d: d["2025"])
    [["station", "name", "freeway", "direction", "lane_type", "lat", "lon",
      "2022", "2023", "2024", "2025", "pct_change_2022_2023",
      "pct_change_2023_2024", "pct_change_2024_2025", "pct_change_2022_2025",
      "trend"]]
)
points.columns = [
    "station", "name", "fwy", "dir", "lane", "lat", "lon",
    "v2022", "v2023", "v2024", "v2025", "c1", "c2", "c3", "ctotal", "trend",
]

MAP_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PeMS Volume Trends</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<style>
  :root {
    --surface: #fcfcfb; --ink: #1f1f1d; --ink-2: #5c5c58; --border: #e4e3df;
    --up: #2a6fdb; --down: #d64535;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --surface: #1a1a19; --ink: #ecebe8; --ink-2: #a6a5a0; --border: #383835;
      --up: #5b9bff; --down: #ff6b5c;
    }
  }
  html, body { margin: 0; height: 100%; background: var(--surface); color: var(--ink);
    font: 14px/1.4 system-ui, -apple-system, "Segoe UI", sans-serif; }
  body { display: flex; flex-direction: column; }
  body > :not(#map) { flex-shrink: 0; }   /* the map takes what is left */
  header { padding: 12px 16px 8px; }
  h1 { font-size: 18px; margin: 0 0 4px; }
  p.sub { margin: 0; color: var(--ink-2); font-size: 13px; }
  .legend { display: flex; flex-wrap: wrap; gap: 16px; padding: 6px 16px 10px;
    font-size: 13px; color: var(--ink-2); align-items: center; }
  .sw { display: inline-block; width: 12px; height: 12px; border-radius: 50%;
    vertical-align: -1px; margin-right: 6px; }
  .toggle { display: flex; gap: 12px; margin-left: auto; }
  .toggle label, #laneFilter label { cursor: pointer; }
  #laneFilter { padding-top: 0; gap: 12px; }
  .search { display: flex; gap: 8px; align-items: center; }
  .search input { font: inherit; color: var(--ink); background: var(--surface);
    border: 1px solid var(--border); border-radius: 6px; padding: 4px 8px; width: 9em; }
  .search button { font: inherit; color: var(--ink); background: var(--surface);
    border: 1px solid var(--border); border-radius: 6px; padding: 4px 10px; cursor: pointer; }
  #searchMsg { color: var(--ink-2); }
  #map { flex: 1; min-height: 300px; }
  details.totals { padding: 0 16px 10px; font-size: 13px; color: var(--ink-2); }
  details.totals summary { cursor: pointer; }
  .totals table { border-collapse: collapse; margin-top: 6px; color: var(--ink);
    font-variant-numeric: tabular-nums; }
  .totals th, .totals td { padding: 3px 12px 3px 0; text-align: right; }
  .totals th:first-child, .totals td:first-child { text-align: left; }
  .totals th { color: var(--ink-2); font-weight: 600; border-bottom: 1px solid var(--border); }
  .totals tr.total td { font-weight: 600; border-top: 1px solid var(--border); }
  .totals .note { margin: 4px 0 0; }
  #totalsTable { overflow-x: auto; }
  @media (max-width: 600px) { .totals th, .totals td { padding-right: 8px; } }
  .leaflet-popup-content { font-size: 13px; }
  .leaflet-popup-content table { border-collapse: collapse; margin-top: 4px; }
  .leaflet-popup-content td { padding: 1px 8px 1px 0; font-variant-numeric: tabular-nums; }
</style>
</head>
<body>
<header>
  <h1>Where weekday traffic volumes are dropping, 2022&ndash;2025</h1>
  <p class="sub">__N__ PeMS District 11 stations (__LANES__) with fully observed,
  complete Sep&ndash;Oct weekdays in all four years. Dot area = 2025 average daily
  volume. Red = 2025 volume more than __DECLINE__% below 2022.</p>
</header>
<div class="legend">
  <span><span class="sw" style="background:var(--up)"></span>Within &minus;__DECLINE__% of 2022, or higher (<span id="nUp"></span>)</span>
  <span><span class="sw" style="background:var(--down)"></span>Down more than __DECLINE__% vs. 2022 (<span id="nDown"></span>)</span>
  <span class="toggle">
    <label><input type="checkbox" id="showUp" checked> blue</label>
    <label><input type="checkbox" id="showDown" checked> red</label>
  </span>
</div>
<div class="legend" id="laneFilter"><span>Lane type:</span></div>
<form class="legend search" id="search" autocomplete="off">
  <label for="searchInput">Station ID</label>
  <input id="searchInput" list="stationIds" inputmode="numeric" placeholder="e.g. 1100313">
  <datalist id="stationIds"></datalist>
  <button type="submit">Find</button>
  <span id="searchMsg" role="status"></span>
</form>
<details class="totals" id="totals">
  <summary>Totals per year by lane type (checked lane types)</summary>
  <div id="totalsTable"></div>
  <p class="note">Sum of the stations' average weekday daily volumes. It's an
  index for comparing years, not a count of trips or vehicles.__TOTALS_NOTE__</p>
</details>
<div id="map"></div>
<script>
const DATA = __DATA__;
const css = getComputedStyle(document.documentElement);
const UP = css.getPropertyValue('--up').trim();
const DOWN = css.getPropertyValue('--down').trim();
const dark = matchMedia('(prefers-color-scheme: dark)').matches;

const map = L.map('map', { preferCanvas: true });
// Esri gray canvas basemaps need no API key.
const esri = 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas';
L.tileLayer(
  `${esri}/${dark ? 'World_Dark_Gray_Base' : 'World_Light_Gray_Base'}/MapServer/tile/{z}/{y}/{x}`,
  { attribution: 'Tiles &copy; Esri &mdash; Esri, HERE, Garmin, &copy; OpenStreetMap contributors', maxZoom: 16 }
).addTo(map);

const maxV = Math.max(...DATA.map(d => d.v2025));
const radius = v => 2 + 12 * Math.sqrt(v / maxV);   // area ~ volume
const fmt = v => Math.round(v).toLocaleString();
const pct = c => (c > 0 ? '+' : '') + c.toFixed(1) + '%';

const LANE_NAMES = __LANE_NAMES__;
const TOTALS = __TOTALS__;   // by lane type, from every common station
const YEARS = __YEARS__;
const shown = L.layerGroup().addTo(map);
// Draw big dots first so small ones stay clickable on top.
const markers = [...DATA].sort((a, b) => b.v2025 - a.v2025).map(d => {
  const color = d.trend === 'decreasing' ? DOWN : UP;
  const m = L.circleMarker([d.lat, d.lon], {
    radius: radius(d.v2025), color: dark ? '#1a1a19' : '#fcfcfb', weight: 1,
    fillColor: color, fillOpacity: 0.75,
  }).bindPopup(
    `<b>${d.station}</b> &middot; ${d.fwy}${d.dir} ${d.lane}<br>${d.name ?? ''}
     <table>
       <tr><td>2022</td><td>${fmt(d.v2022)}</td><td></td></tr>
       <tr><td>2023</td><td>${fmt(d.v2023)}</td><td>${pct(d.c1)}</td></tr>
       <tr><td>2024</td><td>${fmt(d.v2024)}</td><td>${pct(d.c2)}</td></tr>
       <tr><td>2025</td><td>${fmt(d.v2025)}</td><td>${pct(d.c3)}</td></tr>
       <tr><td colspan="2"><b>2022&rarr;2025</b></td><td><b>${pct(d.ctotal)}</b></td></tr>
     </table>`
  ).bindTooltip(`${d.station}: ${fmt(d.v2025)} veh/day (2025)`);
  return { d, m };
});

// Lane-type checkboxes, most common type first.
const laneCounts = {};
DATA.forEach(d => { laneCounts[d.lane] = (laneCounts[d.lane] || 0) + 1; });
const laneBox = document.getElementById('laneFilter');
Object.entries(laneCounts).sort((a, b) => b[1] - a[1]).forEach(([lane, n]) => {
  const label = document.createElement('label');
  label.innerHTML = `<input type="checkbox" data-lane="${lane}" checked> `
    + `${LANE_NAMES[lane] ?? lane} (${lane}, ${n})`;
  laneBox.appendChild(label);
});

function redraw() {
  const lanes = new Set(
    [...laneBox.querySelectorAll('input:checked')].map(i => i.dataset.lane));
  const showUp = document.getElementById('showUp').checked;
  const showDown = document.getElementById('showDown').checked;
  let nUp = 0, nDown = 0;
  shown.clearLayers();
  markers.forEach(({ d, m }) => {
    if (!lanes.has(d.lane)) return;
    const isDown = d.trend === 'decreasing';
    isDown ? nDown++ : nUp++;   // counts reflect the lane filter, not the color toggles
    if (isDown ? showDown : showUp) shown.addLayer(m);
  });
  document.getElementById('nUp').textContent = nUp.toLocaleString();
  document.getElementById('nDown').textContent = nDown.toLocaleString();
  renderTotals(lanes);
}

function renderTotals(lanes) {
  const rows = TOTALS.filter(t => lanes.has(t.lane_type))
    .sort((a, b) => b.n_stations - a.n_stations);
  const sum = { lane_type: 'Total', n_stations: 0, n_decreasing: 0 };
  YEARS.forEach(y => { sum[y] = 0; });
  rows.forEach(t => {
    sum.n_stations += t.n_stations; sum.n_decreasing += t.n_decreasing;
    YEARS.forEach(y => { sum[y] += t[y]; });
  });
  const first = YEARS[0], last = YEARS[YEARS.length - 1];
  const change = t => t[first] ? pct((t[last] / t[first] - 1) * 100) : '';
  const row = (t, cls = '') => `<tr class="${cls}">
    <td>${t.lane_type === 'Total' ? 'Total' : `${LANE_NAMES[t.lane_type] ?? t.lane_type} (${t.lane_type})`}</td>
    <td>${t.n_stations.toLocaleString()}</td><td>${t.n_decreasing.toLocaleString()}</td>
    ${YEARS.map(y => `<td>${fmt(t[y])}</td>`).join('')}
    <td>${change(t)}</td></tr>`;
  document.getElementById('totalsTable').innerHTML = rows.length ? `<table>
    <tr><th>Lane type</th><th>Stations</th><th>Red</th>
      ${YEARS.map(y => `<th>${y}</th>`).join('')}<th>${first}&rarr;${last}</th></tr>
    ${rows.map(t => row(t)).join('')}
    ${row(sum, 'total')}
  </table>` : '<p>No lane types checked.</p>';
}
document.querySelectorAll('.legend input[type=checkbox]').forEach(i => i.onchange = redraw);
redraw();

// Station search: zoom to the station and open its popup. A station hidden
// by the current filters is still shown (and the message says so).
const byId = new Map(markers.map(x => [String(x.d.station), x]));
document.getElementById('stationIds').innerHTML =
  [...byId.keys()].sort().map(id => `<option value="${id}">`).join('');
const msg = document.getElementById('searchMsg');
document.getElementById('search').onsubmit = e => {
  e.preventDefault();
  const id = document.getElementById('searchInput').value.trim();
  const hit = byId.get(id);
  if (!hit) {
    msg.textContent = id ? `${id} is not among the mapped stations.` : '';
    return;
  }
  const hidden = !shown.hasLayer(hit.m);
  if (hidden) shown.addLayer(hit.m);
  map.setView(hit.m.getLatLng(), Math.max(map.getZoom(), 14));
  hit.m.openPopup();
  msg.textContent = hidden ? 'Shown although hidden by the current filters.' : '';
};
// Fit once the container has a real size; fitting a zero-size (e.g. hidden)
// container jumps to max zoom.
map.setView([32.9, -117.1], 9);
let fitted = false;
new ResizeObserver(() => {
  const el = map.getContainer();
  if (fitted || !el.clientWidth || !el.clientHeight) return;
  fitted = true;
  map.invalidateSize();
  map.fitBounds(DATA.map(d => [d.lat, d.lon]), { padding: [20, 20] });
}).observe(map.getContainer());

</script>
</body>
</html>
"""

html = (
    MAP_HTML.replace("__DATA__", json.dumps(points.to_dict(orient="records")))
    .replace("__N__", f"{len(points):,}")
    .replace("__LANES__", ", ".join(LANE_TYPES) if LANE_TYPES else "all lane types")
    .replace("__LANE_NAMES__", json.dumps(LANE_TYPE_NAMES))
    .replace("__DECLINE__", f"{DECLINE_PCT:g}")
    .replace("__TOTALS__", by_lane.reset_index().to_json(orient="records"))
    .replace("__YEARS__", json.dumps(year_cols))
    .replace(
        "__TOTALS_NOTE__",
        f" Includes {missing_xy} station(s) with no coordinates, not drawn on the map."
        if missing_xy else "",
    )
)
MAP_PATH.parent.mkdir(exist_ok=True)
MAP_PATH.write_text(html, encoding="utf-8")
print(f"Wrote {MAP_PATH}")
