"""
generate_dashboard.py
Run this script once to generate a complete HR Analytics HTML dashboard.
It reads your 6 CSV files from the exports/ folder and produces
a single self-contained hr_dashboard.html file you can open in any browser.

Usage:
    python generate_dashboard.py
"""

import pandas as pd
import os, json
from pathlib import Path
from datetime import datetime

EXPORT_DIR = "./exports"
OUTPUT_FILE = "./hr_dashboard.html"

# ── Load CSVs ──────────────────────────────────────────────────────────────
def load(name):
    path = Path(EXPORT_DIR) / name
    if path.exists():
        return pd.read_csv(path)
    print(f"  [WARN] {name} not found, skipping.")
    return pd.DataFrame()

print("Loading data...")
hr   = load("hr_live_export.csv")
risk = load("attrition_risk.csv")
sal  = load("salary_comparison.csv")
pt   = load("performance_trends.csv")
ps   = load("performance_summary.csv")

# Normalise column names
def norm(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ","_")
    return df

hr   = norm(hr)
risk = norm(risk)
sal  = norm(sal)
pt   = norm(pt)
ps   = norm(ps)

print(f"  HR employees   : {len(hr):,}")
print(f"  Attrition risk : {len(risk):,}")
print(f"  Salary compare : {len(sal):,}")
print(f"  Perf trends    : {len(pt):,}")
print(f"  Perf summary   : {len(ps):,}")

# ── KPI helpers ────────────────────────────────────────────────────────────
def kpi(label, value, color="#1D9E75"):
    return f"""
    <div class="kpi">
      <div class="kpi-val" style="color:{color}">{value}</div>
      <div class="kpi-lbl">{label}</div>
    </div>"""

# ── Chart data helpers ─────────────────────────────────────────────────────
def bar_data(categories, values, color="#378ADD"):
    cats  = json.dumps(list(categories))
    vals  = json.dumps([round(float(v),1) for v in values])
    col   = json.dumps(color if isinstance(color,str) else list(color))
    return cats, vals, col

# ── Compute stats ──────────────────────────────────────────────────────────
total     = len(hr)
active    = int(hr["status"].str.lower().eq("active").sum()) if "status" in hr.columns else total
terminated= total - active
pct_active= round(active/total*100,1) if total else 0

# Dept distribution
dept_counts = hr["department"].value_counts().head(8) if "department" in hr.columns else pd.Series()

# Gender
gender_counts = hr["gender"].value_counts() if "gender" in hr.columns else pd.Series()

# Attrition risk breakdown
if not risk.empty and "risk_label" in risk.columns:
    risk_counts = risk["risk_label"].value_counts()
    high_risk   = int(risk_counts.get("High",0))
    med_risk    = int(risk_counts.get("Medium",0))
    low_risk    = int(risk_counts.get("Low",0))
    risk_dept   = risk.merge(hr[["employee_id","department"]], on="employee_id", how="left") if "employee_id" in risk.columns and "employee_id" in hr.columns else pd.DataFrame()
    if not risk_dept.empty and "department" in risk_dept.columns and "risk_score" in risk_dept.columns:
        risk_by_dept = risk_dept.groupby("department")["risk_score"].mean().sort_values(ascending=False).head(8)
    else:
        risk_by_dept = pd.Series()
else:
    high_risk=med_risk=low_risk=0; risk_by_dept=pd.Series()

# Salary benchmark
if not sal.empty and "salary_band" in sal.columns:
    sal_band = sal["salary_band"].value_counts()
    below_mkt= int(sal_band.get("Below Market",0))
    at_mkt   = int(sal_band.get("At Market",0))
    above_mkt= int(sal_band.get("Above Market",0))
    if "job_title" in sal.columns and "salary_vs_market_pct" in sal.columns:
        sal_by_role = sal.groupby("job_title")["salary_vs_market_pct"].mean().sort_values().head(10)
    else:
        sal_by_role = pd.Series()
else:
    below_mkt=at_mkt=above_mkt=0; sal_by_role=pd.Series()

# Performance trends
if not pt.empty and "review_year" in pt.columns and "score" in pt.columns:
    trend_yr = pt.groupby("review_year")["score"].mean().reset_index()
    trend_years  = list(trend_yr["review_year"].astype(int))
    trend_scores = [round(float(s),2) for s in trend_yr["score"]]
else:
    trend_years=[]; trend_scores=[]

if not ps.empty and "trend_direction" in ps.columns:
    td = ps["trend_direction"].value_counts()
    improving= int(td.get("Improving",0))
    declining= int(td.get("Declining",0))
    stable   = int(td.get("Stable",0))
    at_risk_perf = int(ps["at_risk_flag"].sum()) if "at_risk_flag" in ps.columns else 0
else:
    improving=declining=stable=at_risk_perf=0

now = datetime.now().strftime("%d %b %Y, %H:%M")

# ── Build HTML ─────────────────────────────────────────────────────────────
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>HR Analytics Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}}
header{{background:linear-gradient(135deg,#1a1f2e,#0f1117);padding:24px 40px;border-bottom:1px solid #2d3748;display:flex;align-items:center;justify-content:space-between}}
header h1{{font-size:22px;font-weight:600;color:#63b3ed}}
header .sub{{font-size:12px;color:#718096;margin-top:4px}}
.badge{{background:#1D9E75;color:#fff;padding:4px 12px;border-radius:20px;font-size:11px}}
.tabs{{display:flex;gap:0;background:#1a1f2e;border-bottom:1px solid #2d3748;padding:0 40px}}
.tab{{padding:14px 24px;font-size:13px;cursor:pointer;border-bottom:2px solid transparent;color:#718096;transition:all .2s}}
.tab.active{{color:#63b3ed;border-bottom-color:#63b3ed}}
.tab:hover{{color:#e2e8f0}}
.page{{display:none;padding:32px 40px}}
.page.active{{display:block}}
.section-title{{font-size:16px;font-weight:600;color:#e2e8f0;margin-bottom:20px;padding-bottom:8px;border-bottom:1px solid #2d3748}}
.kpi-row{{display:flex;gap:16px;margin-bottom:32px;flex-wrap:wrap}}
.kpi{{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:20px 28px;flex:1;min-width:160px}}
.kpi-val{{font-size:32px;font-weight:700;margin-bottom:4px}}
.kpi-lbl{{font-size:12px;color:#718096;text-transform:uppercase;letter-spacing:.5px}}
.charts-grid{{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-bottom:32px}}
.chart-box{{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:24px}}
.chart-box.full{{grid-column:1/-1}}
.chart-box h3{{font-size:14px;font-weight:600;color:#a0aec0;margin-bottom:20px}}
.chart-wrap{{position:relative;height:260px}}
.risk-pills{{display:flex;gap:12px;margin-bottom:24px;flex-wrap:wrap}}
.pill{{padding:8px 20px;border-radius:24px;font-size:13px;font-weight:600}}
.pill.high{{background:#7f1d1d;color:#fca5a5}}
.pill.med{{background:#78350f;color:#fcd34d}}
.pill.low{{background:#064e3b;color:#6ee7b7}}
.table-wrap{{overflow-x:auto;border-radius:8px;border:1px solid #2d3748}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{background:#0f1117;color:#718096;padding:10px 14px;text-align:left;font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.5px}}
td{{padding:10px 14px;border-top:1px solid #2d3748;color:#e2e8f0}}
tr:hover td{{background:#2d3748}}
.tag{{display:inline-block;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:600}}
.tag.high{{background:#7f1d1d;color:#fca5a5}}
.tag.medium{{background:#78350f;color:#fcd34d}}
.tag.low{{background:#064e3b;color:#6ee7b7}}
.tag.improving{{background:#064e3b;color:#6ee7b7}}
.tag.declining{{background:#7f1d1d;color:#fca5a5}}
.tag.stable{{background:#1e3a5f;color:#93c5fd}}
.tag.below{{background:#7f1d1d;color:#fca5a5}}
.tag.at{{background:#064e3b;color:#6ee7b7}}
.tag.above{{background:#1e3a5f;color:#93c5fd}}
footer{{text-align:center;padding:24px;color:#4a5568;font-size:12px;border-top:1px solid #2d3748;margin-top:32px}}
</style>
</head>
<body>

<header>
  <div>
    <h1>&#9650; HR Analytics Dashboard</h1>
    <div class="sub">Auto-generated &nbsp;|&nbsp; {now} &nbsp;|&nbsp; {total:,} employees</div>
  </div>
  <div class="badge">LIVE DATA</div>
</header>

<div class="tabs">
  <div class="tab active" onclick="showPage('overview')">Overview</div>
  <div class="tab" onclick="showPage('attrition')">Attrition Risk</div>
  <div class="tab" onclick="showPage('salary')">Salary Benchmark</div>
  <div class="tab" onclick="showPage('performance')">Performance Trends</div>
</div>

<!-- ═══ PAGE 1: OVERVIEW ═══ -->
<div class="page active" id="overview">
  <div class="kpi-row">
    {kpi("Total Employees", f"{total:,}", "#63b3ed")}
    {kpi("Active Employees", f"{active:,}", "#1D9E75")}
    {kpi("Terminated", f"{terminated:,}", "#FC8181")}
    {kpi("Active Rate", f"{pct_active}%", "#F6AD55")}
    {kpi("High Attrition Risk", f"{high_risk:,}", "#FC8181")}
    {kpi("Below Market Salary", f"{below_mkt:,}", "#F6AD55")}
  </div>

  <div class="charts-grid">
    <div class="chart-box">
      <h3>Employees by Department</h3>
      <div class="chart-wrap"><canvas id="deptChart"></canvas></div>
    </div>
    <div class="chart-box">
      <h3>Gender Distribution</h3>
      <div class="chart-wrap"><canvas id="genderChart"></canvas></div>
    </div>
  </div>
</div>

<!-- ═══ PAGE 2: ATTRITION RISK ═══ -->
<div class="page" id="attrition">
  <div class="kpi-row">
    {kpi("High Risk", f"{high_risk:,}", "#FC8181")}
    {kpi("Medium Risk", f"{med_risk:,}", "#F6AD55")}
    {kpi("Low Risk", f"{low_risk:,}", "#1D9E75")}
    {kpi("Total Scored", f"{high_risk+med_risk+low_risk:,}", "#63b3ed")}
  </div>

  <div class="charts-grid">
    <div class="chart-box">
      <h3>Risk Distribution</h3>
      <div class="chart-wrap"><canvas id="riskPieChart"></canvas></div>
    </div>
    <div class="chart-box">
      <h3>Average Risk Score by Department</h3>
      <div class="chart-wrap"><canvas id="riskDeptChart"></canvas></div>
    </div>
  </div>

  {"<div class='chart-box full'><h3>Top 20 High-Risk Employees</h3>" + _risk_table(risk, hr) + "</div>" if not risk.empty else ""}
</div>

<!-- ═══ PAGE 3: SALARY BENCHMARK ═══ -->
<div class="page" id="salary">
  <div class="kpi-row">
    {kpi("Below Market", f"{below_mkt:,}", "#FC8181")}
    {kpi("At Market", f"{at_mkt:,}", "#1D9E75")}
    {kpi("Above Market", f"{above_mkt:,}", "#63b3ed")}
  </div>

  <div class="charts-grid">
    <div class="chart-box">
      <h3>Salary Band Distribution</h3>
      <div class="chart-wrap"><canvas id="salBandChart"></canvas></div>
    </div>
    <div class="chart-box">
      <h3>Salary vs Market by Role (% diff)</h3>
      <div class="chart-wrap"><canvas id="salRoleChart"></canvas></div>
    </div>
  </div>

  {"<div class='chart-box full'><h3>Employees Most Below Market</h3>" + _sal_table(sal) + "</div>" if not sal.empty else ""}
</div>

<!-- ═══ PAGE 4: PERFORMANCE TRENDS ═══ -->
<div class="page" id="performance">
  <div class="kpi-row">
    {kpi("Improving", f"{improving:,}", "#1D9E75")}
    {kpi("Declining", f"{declining:,}", "#FC8181")}
    {kpi("Stable", f"{stable:,}", "#63b3ed")}
    {kpi("At-Risk (2+ low)", f"{at_risk_perf:,}", "#F6AD55")}
  </div>

  <div class="charts-grid">
    <div class="chart-box">
      <h3>Average Score Trend Over Years</h3>
      <div class="chart-wrap"><canvas id="trendChart"></canvas></div>
    </div>
    <div class="chart-box">
      <h3>Trend Direction Breakdown</h3>
      <div class="chart-wrap"><canvas id="trendPieChart"></canvas></div>
    </div>
  </div>

  {"<div class='chart-box full'><h3>Declining Performers — Needs Attention</h3>" + _perf_table(ps) + "</div>" if not ps.empty else ""}
</div>

<footer>HR Analytics Dashboard &nbsp;|&nbsp; Generated by Python pipeline &nbsp;|&nbsp; {now}</footer>

<script>
function showPage(id){{
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  event.target.classList.add('active');
}}

const gridColor='rgba(255,255,255,0.06)';
const textColor='#718096';
const baseOpts={{responsive:true,maintainAspectRatio:false,
  plugins:{{legend:{{labels:{{color:textColor,font:{{size:12}}}}}}}},
  scales:{{x:{{ticks:{{color:textColor}},grid:{{color:gridColor}}}},
           y:{{ticks:{{color:textColor}},grid:{{color:gridColor}}}}}}}};

// Dept bar
new Chart('deptChart',{{type:'bar',
  data:{{labels:{json.dumps([str(x) for x in dept_counts.index.tolist()])},
         datasets:[{{data:{json.dumps([int(x) for x in dept_counts.values.tolist()])},
           backgroundColor:'#378ADD',borderRadius:4,label:'Employees'}}]}},
  options:{{...baseOpts,plugins:{{legend:{{display:false}}}}}}}});

// Gender pie
new Chart('genderChart',{{type:'doughnut',
  data:{{labels:{json.dumps([str(x) for x in gender_counts.index.tolist()])},
         datasets:[{{data:{json.dumps([int(x) for x in gender_counts.values.tolist()])},
           backgroundColor:['#63b3ed','#F687B3','#F6AD55'],borderWidth:0}}]}},
  options:{{responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{position:'right',labels:{{color:textColor}}}}}}}}}});

// Risk pie
new Chart('riskPieChart',{{type:'doughnut',
  data:{{labels:['High','Medium','Low'],
         datasets:[{{data:[{high_risk},{med_risk},{low_risk}],
           backgroundColor:['#FC8181','#F6AD55','#1D9E75'],borderWidth:0}}]}},
  options:{{responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{position:'right',labels:{{color:textColor}}}}}}}}}});

// Risk by dept
new Chart('riskDeptChart',{{type:'bar',
  data:{{labels:{json.dumps([str(x) for x in risk_by_dept.index.tolist()])},
         datasets:[{{data:{json.dumps([round(float(x),4) for x in risk_by_dept.values.tolist()])},
           backgroundColor:'#FC8181',borderRadius:4,label:'Avg Risk Score'}}]}},
  options:{{...baseOpts,indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});

// Salary band
new Chart('salBandChart',{{type:'doughnut',
  data:{{labels:['Below Market','At Market','Above Market'],
         datasets:[{{data:[{below_mkt},{at_mkt},{above_mkt}],
           backgroundColor:['#FC8181','#1D9E75','#63b3ed'],borderWidth:0}}]}},
  options:{{responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{position:'right',labels:{{color:textColor}}}}}}}}}});

// Salary by role
new Chart('salRoleChart',{{type:'bar',
  data:{{labels:{json.dumps([str(x) for x in sal_by_role.index.tolist()])},
         datasets:[{{data:{json.dumps([round(float(x),1) for x in sal_by_role.values.tolist()])},
           backgroundColor:{json.dumps(['#FC8181' if v<0 else '#1D9E75' for v in sal_by_role.values.tolist()])},
           borderRadius:4,label:'% vs Market'}}]}},
  options:{{...baseOpts,indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});

// Trend line
new Chart('trendChart',{{type:'line',
  data:{{labels:{json.dumps(trend_years)},
         datasets:[{{data:{json.dumps(trend_scores)},
           borderColor:'#1D9E75',backgroundColor:'rgba(29,158,117,.15)',
           fill:true,tension:0.4,pointBackgroundColor:'#1D9E75',label:'Avg Score'}}]}},
  options:{{...baseOpts}}}});

// Trend pie
new Chart('trendPieChart',{{type:'doughnut',
  data:{{labels:['Improving','Declining','Stable'],
         datasets:[{{data:[{improving},{declining},{stable}],
           backgroundColor:['#1D9E75','#FC8181','#63b3ed'],borderWidth:0}}]}},
  options:{{responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{position:'right',labels:{{color:textColor}}}}}}}}}});
</script>
</body></html>
"""

# ── Table helpers (defined before html string so they can be used) ──────────
def _risk_table(risk_df, hr_df):
    if risk_df.empty: return ""
    cols = [c for c in ["employee_id","first_name","last_name","department","job_title","risk_score","risk_label","top_factor_1"] if c in risk_df.columns]
    top = risk_df.sort_values("risk_score", ascending=False).head(20)[cols]
    rows = ""
    for _, r in top.iterrows():
        lbl  = str(r.get("risk_label","")).lower()
        tag  = f'<span class="tag {lbl}">{r.get("risk_label","")}</span>'
        rows += f"<tr><td>{r.get('employee_id','')}</td><td>{r.get('first_name','')} {r.get('last_name','')}</td><td>{r.get('department','')}</td><td>{r.get('job_title','')}</td><td>{round(float(r.get('risk_score',0)),4)}</td><td>{tag}</td><td>{r.get('top_factor_1','')}</td></tr>"
    return f"<div class='table-wrap'><table><tr><th>ID</th><th>Name</th><th>Dept</th><th>Job Title</th><th>Risk Score</th><th>Label</th><th>Top Factor</th></tr>{rows}</table></div>"

def _sal_table(sal_df):
    if sal_df.empty: return ""
    cols = [c for c in ["employee_id","first_name","last_name","job_title","salary","benchmark_mid","salary_vs_market_pct","salary_band"] if c in sal_df.columns]
    if "salary_vs_market_pct" in sal_df.columns:
        top = sal_df.sort_values("salary_vs_market_pct").head(20)[cols]
    else:
        top = sal_df.head(20)[cols]
    rows = ""
    for _, r in top.iterrows():
        band = str(r.get("salary_band","")).lower().replace(" ","")
        tag  = f'<span class="tag {band}">{r.get("salary_band","")}</span>'
        pct  = r.get("salary_vs_market_pct",0)
        pct_str = f"{float(pct):+.1f}%" if pd.notna(pct) else "N/A"
        rows += f"<tr><td>{r.get('first_name','')} {r.get('last_name','')}</td><td>{r.get('job_title','')}</td><td>${float(r.get('salary',0)):,.0f}</td><td>${float(r.get('benchmark_mid',0)):,.0f}</td><td>{pct_str}</td><td>{tag}</td></tr>"
    return f"<div class='table-wrap'><table><tr><th>Name</th><th>Job Title</th><th>Salary</th><th>Market Mid</th><th>vs Market</th><th>Band</th></tr>{rows}</table></div>"

def _perf_table(ps_df):
    if ps_df.empty: return ""
    cols = [c for c in ["employee_id","first_name","last_name","department","latest_score","avg_score","trend_direction","yoy_delta_latest","at_risk_flag"] if c in ps_df.columns]
    if "trend_direction" in ps_df.columns:
        top = ps_df[ps_df["trend_direction"]=="Declining"].head(20)[cols]
    else:
        top = ps_df.head(20)[cols]
    rows = ""
    for _, r in top.iterrows():
        td   = str(r.get("trend_direction","")).lower()
        tag  = f'<span class="tag {td}">{r.get("trend_direction","")}</span>'
        flag = "⚠️ Yes" if r.get("at_risk_flag") else "No"
        rows += f"<tr><td>{r.get('first_name','')} {r.get('last_name','')}</td><td>{r.get('department','')}</td><td>{round(float(r.get('latest_score',0)),2)}</td><td>{round(float(r.get('avg_score',0)),2)}</td><td>{tag}</td><td>{round(float(r.get('yoy_delta_latest',0)),2)}</td><td>{flag}</td></tr>"
    return f"<div class='table-wrap'><table><tr><th>Name</th><th>Dept</th><th>Latest Score</th><th>Avg Score</th><th>Trend</th><th>YoY Delta</th><th>At Risk</th></tr>{rows}</table></div>"

# Inject table helpers into html (replace placeholders)
html = html.replace(
    "_risk_table(risk, hr)", "''").replace(
    "_sal_table(sal)", "''").replace(
    "_perf_table(ps)", "''")

# Re-build with proper table calls
html2 = html.replace(
    "\" + _risk_table(risk, hr) + \"",
    _risk_table(risk, hr)).replace(
    "\" + _sal_table(sal) + \"",
    _sal_table(sal)).replace(
    "\" + _perf_table(ps) + \"",
    _perf_table(ps))

# Write file
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(html)

print(f"\n✅ Dashboard generated: {OUTPUT_FILE}")
print("   Open this file in Chrome/Edge/Firefox to view it.")
