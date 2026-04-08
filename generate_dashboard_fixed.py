"""
generate_dashboard.py  (fixed version)
Run:  python generate_dashboard.py
Opens: hr_dashboard.html  in your browser
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

EXPORT_DIR = "./exports"
OUTPUT_FILE = "./hr_dashboard.html"

def load(name):
    path = Path(EXPORT_DIR) / name
    if path.exists():
        return pd.read_csv(path)
    print(f"  [WARN] {name} not found, skipping.")
    return pd.DataFrame()

def norm(df):
    if df.empty:
        return df
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

print("Loading data...")
hr   = norm(load("hr_live_export.csv"))
risk = norm(load("attrition_risk.csv"))
sal  = norm(load("salary_comparison.csv"))
pt   = norm(load("performance_trends.csv"))
ps   = norm(load("performance_summary.csv"))

print(f"  HR employees   : {len(hr):,}")
print(f"  Attrition risk : {len(risk):,}")
print(f"  Salary compare : {len(sal):,}")
print(f"  Perf trends    : {len(pt):,}")
print(f"  Perf summary   : {len(ps):,}")

def risk_table(risk_df, hr_df):
    if risk_df.empty:
        return ""
    cols = [c for c in ["employee_id","first_name","last_name","department","job_title","risk_score","risk_label","top_factor_1"] if c in risk_df.columns]
    top = risk_df.sort_values("risk_score", ascending=False).head(20)[cols]
    rows = ""
    for _, r in top.iterrows():
        lbl = str(r.get("risk_label","")).lower()
        tag = f'<span class="tag {lbl}">{r.get("risk_label","")}</span>'
        rows += f"<tr><td>{r.get('employee_id','')}</td><td>{r.get('first_name','')} {r.get('last_name','')}</td><td>{r.get('department','')}</td><td>{r.get('job_title','')}</td><td>{round(float(r.get('risk_score',0)),4)}</td><td>{tag}</td><td>{r.get('top_factor_1','')}</td></tr>"
    return f"<div class='table-wrap'><table><tr><th>ID</th><th>Name</th><th>Dept</th><th>Job Title</th><th>Risk Score</th><th>Label</th><th>Top Factor</th></tr>{rows}</table></div>"

def sal_table(sal_df):
    if sal_df.empty:
        return ""
    cols = [c for c in ["first_name","last_name","job_title","salary","benchmark_mid","salary_vs_market_pct","salary_band"] if c in sal_df.columns]
    top = sal_df.sort_values("salary_vs_market_pct").head(20)[cols] if "salary_vs_market_pct" in sal_df.columns else sal_df.head(20)[cols]
    rows = ""
    for _, r in top.iterrows():
        band = str(r.get("salary_band","")).lower().replace(" ","")
        tag  = f'<span class="tag {band}">{r.get("salary_band","")}</span>'
        pct  = r.get("salary_vs_market_pct",0)
        pct_str = f"{float(pct):+.1f}%" if pd.notna(pct) else "N/A"
        rows += f"<tr><td>{r.get('first_name','')} {r.get('last_name','')}</td><td>{r.get('job_title','')}</td><td>${float(r.get('salary',0)):,.0f}</td><td>${float(r.get('benchmark_mid',0)):,.0f}</td><td>{pct_str}</td><td>{tag}</td></tr>"
    return f"<div class='table-wrap'><table><tr><th>Name</th><th>Job Title</th><th>Salary</th><th>Market Mid</th><th>vs Market</th><th>Band</th></tr>{rows}</table></div>"

def perf_table(ps_df):
    if ps_df.empty:
        return ""
    cols = [c for c in ["first_name","last_name","department","latest_score","avg_score","trend_direction","yoy_delta_latest","at_risk_flag"] if c in ps_df.columns]
    top = ps_df[ps_df["trend_direction"]=="Declining"].head(20)[cols] if "trend_direction" in ps_df.columns else ps_df.head(20)[cols]
    rows = ""
    for _, r in top.iterrows():
        td  = str(r.get("trend_direction","")).lower()
        tag = f'<span class="tag {td}">{r.get("trend_direction","")}</span>'
        flag = "Yes" if r.get("at_risk_flag") else "No"
        rows += f"<tr><td>{r.get('first_name','')} {r.get('last_name','')}</td><td>{r.get('department','')}</td><td>{round(float(r.get('latest_score',0)),2)}</td><td>{round(float(r.get('avg_score',0)),2)}</td><td>{tag}</td><td>{round(float(r.get('yoy_delta_latest',0)),2)}</td><td>{flag}</td></tr>"
    return f"<div class='table-wrap'><table><tr><th>Name</th><th>Dept</th><th>Latest Score</th><th>Avg Score</th><th>Trend</th><th>YoY Delta</th><th>At Risk</th></tr>{rows}</table></div>"

total      = len(hr)
active     = int(hr["status"].str.lower().eq("active").sum()) if "status" in hr.columns else total
terminated = total - active
pct_active = round(active/total*100,1) if total else 0
dept_counts   = hr["department"].value_counts().head(8)   if "department" in hr.columns else pd.Series()
gender_counts = hr["gender"].value_counts()               if "gender"     in hr.columns else pd.Series()

if not risk.empty and "risk_label" in risk.columns:
    rc = risk["risk_label"].value_counts()
    high_risk,med_risk,low_risk = int(rc.get("High",0)),int(rc.get("Medium",0)),int(rc.get("Low",0))
    rd = risk.merge(hr[["employee_id","department"]],on="employee_id",how="left") if "employee_id" in risk.columns and "employee_id" in hr.columns else pd.DataFrame()
    risk_by_dept = rd.groupby("department")["risk_score"].mean().sort_values(ascending=False).head(8) if not rd.empty and "department" in rd.columns else pd.Series()
else:
    high_risk=med_risk=low_risk=0; risk_by_dept=pd.Series()

if not sal.empty and "salary_band" in sal.columns:
    sb = sal["salary_band"].value_counts()
    below_mkt,at_mkt,above_mkt = int(sb.get("Below Market",0)),int(sb.get("At Market",0)),int(sb.get("Above Market",0))
    sal_by_role = sal.groupby("job_title")["salary_vs_market_pct"].mean().sort_values().head(10) if "job_title" in sal.columns and "salary_vs_market_pct" in sal.columns else pd.Series()
else:
    below_mkt=at_mkt=above_mkt=0; sal_by_role=pd.Series()

if not pt.empty and "review_year" in pt.columns and "score" in pt.columns:
    ty = pt.groupby("review_year")["score"].mean().reset_index()
    trend_years,trend_scores = list(ty["review_year"].astype(int)),[round(float(s),2) for s in ty["score"]]
else:
    trend_years,trend_scores = [],[]

if not ps.empty and "trend_direction" in ps.columns:
    td_c = ps["trend_direction"].value_counts()
    improving,declining,stable = int(td_c.get("Improving",0)),int(td_c.get("Declining",0)),int(td_c.get("Stable",0))
    at_risk_perf = int(ps["at_risk_flag"].sum()) if "at_risk_flag" in ps.columns else 0
else:
    improving=declining=stable=at_risk_perf=0

now = datetime.now().strftime("%d %b %Y, %H:%M")

def kpi(label,value,color="#1D9E75"):
    return f'<div class="kpi"><div class="kpi-val" style="color:{color}">{value}</div><div class="kpi-lbl">{label}</div></div>'

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>HR Analytics Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0}}
header{{background:#1a1f2e;padding:24px 40px;border-bottom:1px solid #2d3748;display:flex;align-items:center;justify-content:space-between}}
header h1{{font-size:22px;font-weight:600;color:#63b3ed}}
header .sub{{font-size:12px;color:#718096;margin-top:4px}}
.badge{{background:#1D9E75;color:#fff;padding:4px 12px;border-radius:20px;font-size:11px}}
.tabs{{display:flex;background:#1a1f2e;border-bottom:1px solid #2d3748;padding:0 40px}}
.tab{{padding:14px 24px;font-size:13px;cursor:pointer;border-bottom:2px solid transparent;color:#718096}}
.tab.active{{color:#63b3ed;border-bottom-color:#63b3ed}}
.page{{display:none;padding:32px 40px}}
.page.active{{display:block}}
.kpi-row{{display:flex;gap:16px;margin-bottom:32px;flex-wrap:wrap}}
.kpi{{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:20px 28px;flex:1;min-width:150px}}
.kpi-val{{font-size:30px;font-weight:700;margin-bottom:4px}}
.kpi-lbl{{font-size:11px;color:#718096;text-transform:uppercase;letter-spacing:.5px}}
.charts-grid{{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-bottom:32px}}
.chart-box{{background:#1a1f2e;border:1px solid #2d3748;border-radius:12px;padding:24px}}
.chart-box.full{{grid-column:1/-1}}
.chart-box h3{{font-size:12px;font-weight:600;color:#a0aec0;margin-bottom:20px;text-transform:uppercase;letter-spacing:.5px}}
.chart-wrap{{position:relative;height:260px}}
.table-wrap{{overflow-x:auto;border-radius:8px;border:1px solid #2d3748}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{background:#0f1117;color:#718096;padding:10px 14px;text-align:left;font-weight:500;font-size:11px;text-transform:uppercase}}
td{{padding:10px 14px;border-top:1px solid #2d3748;color:#e2e8f0}}
tr:hover td{{background:#2d3748}}
.tag{{display:inline-block;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:600}}
.tag.high,.tag.declining,.tag.belowmarket{{background:#7f1d1d;color:#fca5a5}}
.tag.medium{{background:#78350f;color:#fcd34d}}
.tag.low,.tag.improving,.tag.atmarket{{background:#064e3b;color:#6ee7b7}}
.tag.stable,.tag.abovemarket{{background:#1e3a5f;color:#93c5fd}}
footer{{text-align:center;padding:24px;color:#4a5568;font-size:12px;border-top:1px solid #2d3748;margin-top:32px}}
</style>
</head>
<body>
<header>
  <div><h1>HR Analytics Dashboard</h1><div class="sub">Auto-generated | {now} | {total:,} employees</div></div>
  <div class="badge">LIVE DATA</div>
</header>
<div class="tabs">
  <div class="tab active" onclick="showPage('overview',this)">Overview</div>
  <div class="tab" onclick="showPage('attrition',this)">Attrition Risk</div>
  <div class="tab" onclick="showPage('salary',this)">Salary Benchmark</div>
  <div class="tab" onclick="showPage('performance',this)">Performance Trends</div>
</div>

<div class="page active" id="overview">
  <div class="kpi-row">{kpi("Total Employees",f"{total:,}","#63b3ed")}{kpi("Active",f"{active:,}","#1D9E75")}{kpi("Terminated",f"{terminated:,}","#FC8181")}{kpi("Active Rate",f"{pct_active}%","#F6AD55")}{kpi("High Risk",f"{high_risk:,}","#FC8181")}{kpi("Below Market",f"{below_mkt:,}","#F6AD55")}</div>
  <div class="charts-grid">
    <div class="chart-box"><h3>Employees by Department</h3><div class="chart-wrap"><canvas id="deptChart"></canvas></div></div>
    <div class="chart-box"><h3>Gender Distribution</h3><div class="chart-wrap"><canvas id="genderChart"></canvas></div></div>
  </div>
</div>

<div class="page" id="attrition">
  <div class="kpi-row">{kpi("High Risk",f"{high_risk:,}","#FC8181")}{kpi("Medium Risk",f"{med_risk:,}","#F6AD55")}{kpi("Low Risk",f"{low_risk:,}","#1D9E75")}{kpi("Total Scored",f"{high_risk+med_risk+low_risk:,}","#63b3ed")}</div>
  <div class="charts-grid">
    <div class="chart-box"><h3>Risk Distribution</h3><div class="chart-wrap"><canvas id="riskPieChart"></canvas></div></div>
    <div class="chart-box"><h3>Avg Risk Score by Department</h3><div class="chart-wrap"><canvas id="riskDeptChart"></canvas></div></div>
    <div class="chart-box full"><h3>Top 20 High-Risk Employees</h3>{risk_table(risk,hr)}</div>
  </div>
</div>

<div class="page" id="salary">
  <div class="kpi-row">{kpi("Below Market",f"{below_mkt:,}","#FC8181")}{kpi("At Market",f"{at_mkt:,}","#1D9E75")}{kpi("Above Market",f"{above_mkt:,}","#63b3ed")}</div>
  <div class="charts-grid">
    <div class="chart-box"><h3>Salary Band Distribution</h3><div class="chart-wrap"><canvas id="salBandChart"></canvas></div></div>
    <div class="chart-box"><h3>Salary vs Market by Role (%)</h3><div class="chart-wrap"><canvas id="salRoleChart"></canvas></div></div>
    <div class="chart-box full"><h3>Employees Most Below Market</h3>{sal_table(sal)}</div>
  </div>
</div>

<div class="page" id="performance">
  <div class="kpi-row">{kpi("Improving",f"{improving:,}","#1D9E75")}{kpi("Declining",f"{declining:,}","#FC8181")}{kpi("Stable",f"{stable:,}","#63b3ed")}{kpi("At-Risk",f"{at_risk_perf:,}","#F6AD55")}</div>
  <div class="charts-grid">
    <div class="chart-box"><h3>Average Score Trend Over Years</h3><div class="chart-wrap"><canvas id="trendChart"></canvas></div></div>
    <div class="chart-box"><h3>Trend Direction Breakdown</h3><div class="chart-wrap"><canvas id="trendPieChart"></canvas></div></div>
    <div class="chart-box full"><h3>Declining Performers</h3>{perf_table(ps)}</div>
  </div>
</div>

<footer>HR Analytics Dashboard | {now}</footer>
<script>
function showPage(id,el){{document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));document.getElementById(id).classList.add('active');el.classList.add('active');}}
const gc='rgba(255,255,255,0.06)',tc='#718096';
const base={{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{labels:{{color:tc}}}}}},scales:{{x:{{ticks:{{color:tc}},grid:{{color:gc}}}},y:{{ticks:{{color:tc}},grid:{{color:gc}}}}}}}};
new Chart('deptChart',{{type:'bar',data:{{labels:{json.dumps([str(x) for x in dept_counts.index.tolist()])},datasets:[{{data:{json.dumps([int(x) for x in dept_counts.values.tolist()])},backgroundColor:'#378ADD',borderRadius:4,label:'Employees'}}]}},options:{{...base,plugins:{{legend:{{display:false}}}}}}}});
new Chart('genderChart',{{type:'doughnut',data:{{labels:{json.dumps([str(x) for x in gender_counts.index.tolist()])},datasets:[{{data:{json.dumps([int(x) for x in gender_counts.values.tolist()])},backgroundColor:['#63b3ed','#F687B3','#F6AD55'],borderWidth:0}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'right',labels:{{color:tc}}}}}}}}}});
new Chart('riskPieChart',{{type:'doughnut',data:{{labels:['High','Medium','Low'],datasets:[{{data:[{high_risk},{med_risk},{low_risk}],backgroundColor:['#FC8181','#F6AD55','#1D9E75'],borderWidth:0}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'right',labels:{{color:tc}}}}}}}}}});
new Chart('riskDeptChart',{{type:'bar',data:{{labels:{json.dumps([str(x) for x in risk_by_dept.index.tolist()])},datasets:[{{data:{json.dumps([round(float(x),4) for x in risk_by_dept.values.tolist()])},backgroundColor:'#FC8181',borderRadius:4,label:'Avg Risk Score'}}]}},options:{{...base,indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});
new Chart('salBandChart',{{type:'doughnut',data:{{labels:['Below Market','At Market','Above Market'],datasets:[{{data:[{below_mkt},{at_mkt},{above_mkt}],backgroundColor:['#FC8181','#1D9E75','#63b3ed'],borderWidth:0}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'right',labels:{{color:tc}}}}}}}}}});
new Chart('salRoleChart',{{type:'bar',data:{{labels:{json.dumps([str(x) for x in sal_by_role.index.tolist()])},datasets:[{{data:{json.dumps([round(float(x),1) for x in sal_by_role.values.tolist()])},backgroundColor:{json.dumps(['#FC8181' if v<0 else '#1D9E75' for v in sal_by_role.values.tolist()])},borderRadius:4,label:'% vs Market'}}]}},options:{{...base,indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});
new Chart('trendChart',{{type:'line',data:{{labels:{json.dumps(trend_years)},datasets:[{{data:{json.dumps(trend_scores)},borderColor:'#1D9E75',backgroundColor:'rgba(29,158,117,.15)',fill:true,tension:0.4,pointBackgroundColor:'#1D9E75',label:'Avg Score'}}]}},options:{{...base}}}});
new Chart('trendPieChart',{{type:'doughnut',data:{{labels:['Improving','Declining','Stable'],datasets:[{{data:[{improving},{declining},{stable}],backgroundColor:['#1D9E75','#FC8181','#63b3ed'],borderWidth:0}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'right',labels:{{color:tc}}}}}}}}}});
</script>
</body></html>"""

with open(OUTPUT_FILE,"w",encoding="utf-8") as f:
    f.write(html)
print(f"\nDashboard generated: {OUTPUT_FILE}")
print("Double-click hr_dashboard.html to open in your browser!")
