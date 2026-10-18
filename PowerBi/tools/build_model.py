"""DataSentinel Power BI semantic model: report measures, Attention table, receivables ageing and theme.

Usage:  python PowerBi/tools/build_model.py [path/to/PowerBi]
Run it with Power BI Desktop closed. It is idempotent: helper measures are tagged with
'/// UI:', '/// UI5:' or '/// UI6:' comments and replaced on every run, including the
lineageTag lines Power BI Desktop adds when it saves.
"""
import json
import os
import re
import sys

PB = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ===== Part 1: helper measures and light theme =====
MEAS = os.path.join(PB, "model", "DataSentinel.SemanticModel", "definition", "tables", "_Measures.tmdl")

BLUE, GREEN, PURPLE, ORANGE, RED, AMBER, GREY = "#2F6FDB", "#1FAE7A", "#6B5BD6", "#F06A35", "#D9363E", "#F2B33D", "#9CA3AF"
TRACK = "#EDF0F4"


def m(name, body, fmt=None):
    lines = body.strip("\n").split("\n")
    if len(lines) == 1:
        out = f"\n\t/// UI: report helper\n\tmeasure '{name}' = {lines[0].strip()}\n"
    else:
        out = f"\n\t/// UI: report helper\n\tmeasure '{name}' =\n" + "".join(f"\t\t\t{l}\n" for l in lines)
    if fmt:
        out += f"\t\tformatString: {fmt}\n"
    return out + "\t\tisHidden\n"


def delta_pct(name, base, table, datecol, days=7):
    return m(name, f"""VAR maxD = CALCULATE ( MAX ( {datecol} ), ALLSELECTED ( {table} ) )
VAR cur = CALCULATE ( {base}, {datecol} > maxD - {days}, {datecol} <= maxD )
VAR prev = CALCULATE ( {base}, {datecol} > maxD - {2 * days}, {datecol} <= maxD - {days} )
VAR d = DIVIDE ( cur - prev, prev )
RETURN IF ( ISBLANK ( d ), "no prior week", IF ( d >= 0, "▲ ", "▼ " ) & FORMAT ( ABS ( d ), "0%" ) & " vs last week" )""")


def delta_pt(name, base, table, datecol, days=7):
    return m(name, f"""VAR maxD = CALCULATE ( MAX ( {datecol} ), ALLSELECTED ( {table} ) )
VAR cur = CALCULATE ( {base}, {datecol} > maxD - {days}, {datecol} <= maxD )
VAR prev = CALCULATE ( {base}, {datecol} > maxD - {2 * days}, {datecol} <= maxD - {days} )
RETURN IF ( ISBLANK ( prev ), "no prior week", IF ( cur >= prev, "▲ ", "▼ " ) & FORMAT ( ABS ( cur - prev ) * 100, "0.0" ) & " pt vs last week" )""")


def track(name, value, column):
    return m(name, f"""VAR mx = MAXX ( ALLSELECTED ( {column} ), {value} )
RETURN IF ( NOT ISBLANK ( {value} ), mx * 1.08 - {value} )""")


def top_red(name, column, value, order="DESC"):
    return m(name, f'IF ( RANKX ( ALLSELECTED ( {column} ), {value},, {order}, DENSE ) = 1, "{RED}", "{BLUE}" )')


def status_bg(name, column, bad):
    vals = ", ".join(f'"{v}"' for v in bad)
    return m(name, f'IF ( SELECTEDVALUE ( {column} ) IN {{ {vals} }}, "#FDE2E2" )')


def status_font(name, column, bad):
    vals = ", ".join(f'"{v}"' for v in bad)
    return m(name, f'IF ( SELECTEDVALUE ( {column} ) IN {{ {vals} }}, "#B42318", "#111827" )')


UI = "".join([
    # ---- Network & App
    delta_pct("UI Records Delta", "[Network Records]", "mart_network_daily", "mart_network_daily[usage_date]"),
    delta_pct("UI Voice Delta", "[Voice Minutes]", "mart_network_daily", "mart_network_daily[usage_date]"),
    delta_pct("UI Data Delta", "[Data GB]", "mart_network_daily", "mart_network_daily[usage_date]"),
    delta_pt("UI Drop Delta", "[Drop Rate]", "mart_network_daily", "mart_network_daily[usage_date]"),
    m("UI Partner Status", 'IF ( [Unknown Partner Cost EUR] > 0, "needs matching", "all matched" )'),
    m("UI Days Network", 'FORMAT ( DISTINCTCOUNT ( mart_network_daily[usage_date] ), "0" ) & " days"'),
    m("UI Days App", 'FORMAT ( DISTINCTCOUNT ( mart_app_usage[event_date] ), "0" ) & " days"'),
    m("UI Versions App", 'FORMAT ( DISTINCTCOUNT ( mart_app_usage[app_version] ), "0" ) & " versions"'),
    m("UI Months Partner", 'FORMAT ( DISTINCTCOUNT ( mart_partner_usage[period_start] ), "0" ) & " months"'),
    track("Crash Share Track", "[Crash Share]", "mart_app_usage[app_version]"),
    top_red("Color Crash Share", "mart_app_usage[app_version]", "[Crash Share]"),
    track("Cell Drop Track", "[Cell Drop Rate]", "mart_cell_quality[cell_id]"),
    m("App Events K", "DIVIDE ( [App Events], 1000 )", "#,0"),
    m("Network Records M", "DIVIDE ( [Network Records], 1000000 )", "0.0"),
    m("Partner Cost K", "DIVIDE ( [Partner Cost EUR], 1000 )", "€ #,0"),
    # ---- Overview
    m("UI Open 7d", """VAR maxD = CALCULATE ( MAX ( mart_incidents[opened_at] ), ALLSELECTED ( mart_incidents ) )
VAR n = CALCULATE ( [Incident Count], mart_incidents[opened_at] > maxD - 7 )
RETURN FORMAT ( n, "0" ) & " opened in last 7 days" """),
    m("UI SLA Policy", '"P1 1h · P2 4h · P3 24h · P4 72h"'),
    m("UI Quarantine Datasets", 'FORMAT ( DISTINCTCOUNT ( mart_quarantine[dataset] ), "0" ) & " datasets affected"'),
    m("UI Active Share", 'FORMAT ( DIVIDE ( [Active Customers], [Customer Count] ), "0%" ) & " of customers"'),
    m("UI Unpaid Owe", """VAR n = CALCULATE ( [Customer Count], mart_customer_360[unpaid_eur] > 0 )
RETURN FORMAT ( n, "#,0" ) & " customers owe" """),
    # ---- Data Quality
    m("UI DQ Delta", """VAR maxD = CALCULATE ( MAX ( mart_dq_trend[run_date] ), ALLSELECTED ( mart_dq_trend ) )
VAR prevD = CALCULATE ( MAX ( mart_dq_trend[run_date] ), ALLSELECTED ( mart_dq_trend ), mart_dq_trend[run_date] < maxD )
VAR cur = CALCULATE ( [DQ Trend Pass Rate], mart_dq_trend[run_date] = maxD )
VAR prev = CALCULATE ( [DQ Trend Pass Rate], mart_dq_trend[run_date] = prevD )
RETURN IF ( ISBLANK ( prev ), "latest run", IF ( cur >= prev, "▲ ", "▼ " ) & FORMAT ( ABS ( cur - prev ) * 100, "0.0" ) & " pt vs previous run" )"""),
    m("Failing Rules", 'CALCULATE ( COUNTROWS ( mart_dq_health ), mart_dq_health[status] <> "PASS" )', "#,0"),
    m("UI Rules Total", '"of " & FORMAT ( COUNTROWS ( mart_dq_health ), "#,0" ) & " rules checked"'),
    m("UI Oldest Note", '"waiting in quarantine"'),
    m("UI Runs DQ", 'FORMAT ( DISTINCTCOUNT ( mart_dq_trend[run_date] ), "0" ) & " days"'),
    m("UI Datasets DQ", 'FORMAT ( DISTINCTCOUNT ( mart_dq_health[dataset] ), "0" ) & " datasets"'),
    m("Pass Rate Track", "IF ( NOT ISBLANK ( [DQ Pass Rate] ), 1 - [DQ Pass Rate] )", "0.0%"),
    m("Color Pass Rate", f'IF ( [DQ Pass Rate] < 0.95, "{RED}", "{BLUE}" )'),
    m("Color Matrix", """VAR v = [DQ Trend Pass Rate]
RETURN SWITCH ( TRUE (), ISBLANK ( v ), BLANK (), v < 0.9, "#FBD5D5", v < 0.98, "#FDECC8", "#D7F2E3" )"""),
    status_bg("Color DQ Status", "mart_dq_health[status]", ["FAIL", "FAILED"]),
    status_font("Font DQ Status", "mart_dq_health[status]", ["FAIL", "FAILED"]),
    # ---- Revenue Assurance
    m("UI Risk Delta", """VAR lastT = CALCULATE ( MAX ( mart_revenue_assurance_trend[run_ts] ), ALLSELECTED ( mart_revenue_assurance_trend ) )
VAR prevT = CALCULATE ( MAX ( mart_revenue_assurance_trend[run_ts] ), ALLSELECTED ( mart_revenue_assurance_trend ), mart_revenue_assurance_trend[run_ts] < lastT )
VAR cur = CALCULATE ( SUM ( mart_revenue_assurance_trend[amount_at_risk_eur] ), mart_revenue_assurance_trend[run_ts] = lastT )
VAR prev = CALCULATE ( SUM ( mart_revenue_assurance_trend[amount_at_risk_eur] ), mart_revenue_assurance_trend[run_ts] = prevT )
VAR d = DIVIDE ( cur - prev, prev )
RETURN IF ( ISBLANK ( d ), "latest run", IF ( d >= 0, "▲ ", "▼ " ) & FORMAT ( ABS ( d ), "0%" ) & " vs previous run" )"""),
    m("UI Gross Note", '"gross: R3 and R6 overlap"'),
    m("UI Defect Note", '"excludes R10 unpaid invoices"'),
    m("Checks Failing", 'CALCULATE ( COUNTROWS ( mart_revenue_assurance ), mart_revenue_assurance[status] = "FAIL" )', "#,0"),
    m("UI Checks Total", '"of " & FORMAT ( COUNTROWS ( mart_revenue_assurance ), "0" ) & " checks"'),
    m("Cases To Investigate", "COUNTROWS ( mart_recon_mismatches )", "#,0"),
    m("UI Latest Run", '"latest reconciliation run"'),
    m("UI Runs Recon", 'FORMAT ( DISTINCTCOUNT ( mart_revenue_assurance_trend[run_id] ), "0" ) & " runs"'),
    m("UI Mismatch Checks", """VAR n = CALCULATE ( COUNTROWS ( mart_revenue_assurance ), mart_revenue_assurance[rows_mismatched] > 0 )
RETURN "across " & FORMAT ( n, "0" ) & " checks" """),
    track("Leak Risk Track", "[Gross EUR At Risk]", "mart_revenue_assurance[leak_type]"),
    m("Recon Risk Run", "SUM ( mart_revenue_assurance_trend[amount_at_risk_eur] )", "€ #,0"),
    status_bg("Color Recon Status", "mart_revenue_assurance[status]", ["FAIL", "FAILED"]),
    status_font("Font Recon Status", "mart_revenue_assurance[status]", ["FAIL", "FAILED"]),
    # ---- Incidents
    m("UI Resolved Share", """VAR resolved = CALCULATE ( [Incident Count], mart_incidents[status] = "RESOLVED" )
RETURN FORMAT ( DIVIDE ( [Resolved In SLA], resolved ), "0%" ) & " of resolved" """),
    m("UI Ack Note", '"minutes to acknowledge"'),
    m("UI Incidents Total", 'FORMAT ( [Incident Count], "#,0" ) & " incidents"'),
    m("UI Teams", 'FORMAT ( DISTINCTCOUNT ( mart_incidents[team] ), "0" ) & " teams"'),
    track("Team Workload Track", "[Open Incidents]", "mart_incidents[team]"),
    status_bg("Color SLA Status", "mart_incidents[sla_status]", ["BREACHED"]),
    status_font("Font SLA Status", "mart_incidents[sla_status]", ["BREACHED"]),
    # ---- Customer 360
    m("UI Invoices", 'FORMAT ( SUM ( mart_customer_360[invoices] ), "#,0" ) & " invoices"'),
    m("UI Collected", 'FORMAT ( [Collection Rate], "0%" ) & " collected"'),
    m("UI Plans", 'FORMAT ( DISTINCTCOUNT ( mart_customer_360[plan_name] ), "0" ) & " plans"'),
    m("UI Customers Shown", 'FORMAT ( [Customer Count], "#,0" ) & " customers"'),
    track("Unpaid Plan Track", "[Unpaid EUR]", "mart_customer_360[plan_name]"),
])

src = open(MEAS, encoding="utf-8").read()
src = re.sub(r"\n\t/// [^\n]*\n\tmeasure 'Color [^']+'.*?\t\tisHidden\n(?:\t\t[^\n]*\n)*", "\n", src, flags=re.S)
src = re.sub(r"\n\t/// UI:[^\n]*\n\tmeasure '[^']+'.*?\t\tisHidden\n(?:\t\t[^\n]*\n)*", "\n", src, flags=re.S)
src = re.sub(r"\n{3,}", "\n\n", src)
src = src.replace("\n\tcolumn MeasureHost", UI + "\n\tcolumn MeasureHost", 1)
for name in ["Billed EUR", "Paid EUR", "Unpaid EUR", "EUR At Risk", "Gross EUR At Risk", "Partner Cost EUR",
             "Unknown Partner Cost EUR"]:
    src = re.sub(rf"(\tmeasure '{re.escape(name)}' = [^\n]*\n\t\tformatString: )[^\n]*", r"\g<1>€ #,0", src)
open(MEAS, "w", encoding="utf-8").write(src)

TEXT, SUB, BORDER, BG = "#111827", "#6B7280", "#E6E8EC", "#F5F7FA"
sol = lambda c: {"solid": {"color": c}}
theme = {
    "name": "DataSentinel light",
    "dataColors": [BLUE, GREEN, PURPLE, ORANGE, AMBER, "#0EA5E9", RED, "#64748B", "#A78BFA", "#14B8A6"],
    "foreground": TEXT, "foregroundNeutralSecondary": SUB, "foregroundNeutralTertiary": GREY,
    "background": "#FFFFFF", "backgroundLight": BG, "backgroundNeutral": BORDER,
    "tableAccent": BLUE, "good": GREEN, "neutral": AMBER, "bad": RED,
    "maximum": GREEN, "center": AMBER, "minimum": RED,
    "textClasses": {
        "callout": {"fontSize": 28, "fontFace": "Segoe UI Bold", "color": TEXT},
        "title": {"fontSize": 12, "fontFace": "Segoe UI Semibold", "color": TEXT},
        "header": {"fontSize": 10, "fontFace": "Segoe UI Semibold", "color": TEXT},
        "label": {"fontSize": 9, "fontFace": "Segoe UI", "color": SUB},
    },
    "visualStyles": {
        "*": {"*": {
            "background": [{"show": True, "color": sol("#FFFFFF"), "transparency": 0}],
            "border": [{"show": True, "color": sol(BORDER), "radius": 12}],
            "dropShadow": [{"show": False}],
            "title": [{"alignment": "left", "fontColor": sol(TEXT), "bold": True, "fontSize": 12}],
            "subTitle": [{"fontColor": sol(SUB), "fontSize": 9}],
            "padding": [{"top": 14, "bottom": 12, "left": 18, "right": 18}],
            "categoryAxis": [{"showAxisTitle": False, "labelColor": sol(SUB), "fontSize": 9, "gridlineShow": False}],
            "valueAxis": [{"showAxisTitle": False, "labelColor": sol(SUB), "fontSize": 9,
                           "gridlineShow": True, "gridlineColor": sol("#EEF0F3"), "gridlineStyle": "dotted"}],
            "legend": [{"show": True, "position": "Top", "labelColor": sol("#374151"), "fontSize": 9,
                        "showTitle": False, "defaultToCircle": True}],
            "labels": [{"color": sol(TEXT), "fontSize": 9}],
        }},
        "page": {"*": {
            "background": [{"color": sol(BG), "transparency": 0}],
            "outspace": [{"color": sol(BG), "transparency": 0}],
        }},
        "tableEx": {"*": {
            "columnHeaders": [{"backColor": sol("#FFFFFF"), "fontColor": sol(SUB), "bold": True, "fontSize": 9,
                               "outlineStyle": 2}],
            "values": [{"backColorPrimary": sol("#FFFFFF"), "backColorSecondary": sol("#F9FAFB"),
                        "fontColorPrimary": sol(TEXT), "fontColorSecondary": sol(TEXT), "fontSize": 9}],
            "grid": [{"gridHorizontal": True, "gridHorizontalColor": sol("#EEF0F3"), "gridVertical": False,
                      "outlineColor": sol(BORDER), "rowPadding": 6}],
        }},
        "pivotTable": {"*": {
            "columnHeaders": [{"backColor": sol("#FFFFFF"), "fontColor": sol(SUB), "bold": True}],
            "rowHeaders": [{"backColor": sol("#FFFFFF"), "fontColor": sol(TEXT)}],
            "values": [{"fontColorPrimary": sol(TEXT), "fontSize": 9}],
            "grid": [{"gridHorizontal": True, "gridHorizontalColor": sol("#FFFFFF"), "gridVertical": True,
                      "gridVerticalColor": sol("#FFFFFF"), "rowPadding": 8}],
        }},
        "slicer": {"*": {"items": [{"fontColor": sol(TEXT), "fontSize": 9, "bold": True}]}},
        "areaChart": {"*": {"lineStyles": [{"strokeWidth": 2}]}},
        "lineChart": {"*": {"lineStyles": [{"strokeWidth": 2}]}},
        "textbox": {"*": {"background": [{"show": False}], "border": [{"show": False}], "padding": [{"top": 0, "bottom": 0, "left": 0, "right": 0}]}},
        "shape": {"*": {"background": [{"show": False}], "border": [{"show": False}]}},
        "actionButton": {"*": {"background": [{"show": False}], "border": [{"show": False}],
                               "padding": [{"top": 0, "bottom": 0, "left": 0, "right": 0}]}},
    },
}
with open(os.path.join(PB, "theme", "sentinel_theme.json"), "w", encoding="utf-8") as fh:
    json.dump(theme, fh, indent=2, ensure_ascii=False)
print("part 1: helper measures + theme")


# ===== Part 2: targets, status pills, Attention table, receivables ageing =====
DEF = os.path.join(PB, "model", "DataSentinel.SemanticModel", "definition")
MEAS = os.path.join(DEF, "tables", "_Measures.tmdl")
STATUS = {0: ("#E3F6EE", "#0F7A53"), 1: ("#FDF1D6", "#92600A"), 2: ("#FDE2E2", "#B42318")}
NEUTRAL = ("#F1F3F6", "#4B5563")


def block(name, body, fmt=None):
    lines = body.strip("\n").split("\n")
    if len(lines) == 1:
        out = f"\n\t/// UI5: BI review\n\tmeasure '{name}' = {lines[0].strip()}\n"
    else:
        out = f"\n\t/// UI5: BI review\n\tmeasure '{name}' =\n" + "".join(f"\t\t\t{l}\n" for l in lines)
    if fmt:
        out += f"\t\tformatString: {fmt}\n"
    return out + "\t\tisHidden\n"


def status_pill(name, level_expr, text_expr):
    """Three measures: <name> (text with icon), <name> Bg, <name> Fg. Level 0 good, 1 warning, 2 critical."""
    bg = ", ".join(f'{k}, "{v[0]}"' for k, v in STATUS.items())
    fg = ", ".join(f'{k}, "{v[1]}"' for k, v in STATUS.items())
    return (block(f"{name} Level", level_expr)
            + block(name, text_expr)
            + block(f"{name} Bg", f'SWITCH ( [{name} Level], {bg}, "{NEUTRAL[0]}" )')
            + block(f"{name} Fg", f'SWITCH ( [{name} Level], {fg}, "{NEUTRAL[1]}" )'))


UI5 = "".join([
    # Targets (business thresholds, change here)
    block("Target DQ Pass", "0.98", "0%"),
    block("Target Drop Rate", "0.02", "0.0%"),
    block("Target SLA Compliance", "0.95", "0%"),
    block("Target Collection", "0.95", "0%"),
    block("Limit Crash Share", "0.02", "0.0%"),
    block("SLA Compliance", 'DIVIDE ( [Resolved In SLA], CALCULATE ( [Incident Count], mart_incidents[status] = "RESOLVED" ) )',
          "0%"),
    # Status pills: icon + label, never colour alone
    status_pill("UI DQ Pill",
                "VAR v = [DQ Pass Rate] RETURN SWITCH ( TRUE (), ISBLANK ( v ), -1, v >= [Target DQ Pass], 0, v >= 0.95, 1, 2 )",
                'SWITCH ( [UI DQ Pill Level], 0, "✓ On target (98%)", 1, "! Below 98% target", 2, "✕ Under 95%, critical", "no data" )'),
    status_pill("UI SLA Pill", "IF ( [SLA Breached] > 0, 2, 0 )",
                'IF ( [SLA Breached] > 0, "✕ Deadline missed, act now", "✓ No deadline missed" )'),
    status_pill("UI Drop Pill",
                "VAR v = [Drop Rate] RETURN SWITCH ( TRUE (), ISBLANK ( v ), -1, v <= [Target Drop Rate], 0, v <= 0.03, 1, 2 )",
                'SWITCH ( [UI Drop Pill Level], 0, "✓ Within 2% limit", 1, "! Above 2% limit", 2, "✕ Above 3%, critical", "no data" )'),
    status_pill("UI Collection Pill",
                "VAR c = [Collection Rate] RETURN SWITCH ( TRUE (), ISBLANK ( c ), -1, c >= [Target Collection], 0, c >= 0.85, 1, 2 )",
                'FORMAT ( [Collection Rate], "0%" ) & " collected, target 95%"'),
    status_pill("UI Compliance Pill",
                "VAR c = [SLA Compliance] RETURN SWITCH ( TRUE (), ISBLANK ( c ), -1, c >= [Target SLA Compliance], 0, c >= 0.85, 1, 2 )",
                'IF ( ISBLANK ( [SLA Compliance] ), "none resolved yet", FORMAT ( [SLA Compliance], "0%" ) & " in SLA, target 95%" )'),
    status_pill("UI Quarantine Pill", "IF ( [Quarantined Rows] > 0, 1, 0 )",
                'IF ( [Quarantined Rows] > 0, "! " & FORMAT ( DISTINCTCOUNT ( mart_quarantine[dataset] ), "0" ) & " datasets affected", "✓ Quarantine empty" )'),
    status_pill("UI Failing Pill", "IF ( [Failing Rules] > 0, 2, 0 )",
                'IF ( [Failing Rules] > 0, "✕ " & [UI Rules Total], "✓ All rules pass" )'),
    status_pill("UI Defect Pill", "IF ( [EUR At Risk] > 0, 2, 0 )",
                'IF ( [EUR At Risk] > 0, "✕ Leakage to investigate", "✓ No data-defect leakage" )'),
    status_pill("UI Partner Pill", "IF ( [Unknown Partner Cost EUR] > 0, 1, 0 )",
                'IF ( [Unknown Partner Cost EUR] > 0, "! Needs matching", "✓ All matched" )'),
    # Chart-rule fixes
    block("Color Crash Share", 'IF ( [Crash Share] > [Limit Crash Share], "#D9363E", "#2F6FDB" )'),
    block("Color Aging", """SWITCH ( MAX ( mart_customer_360[payment_age_order] ),
	1, "#F9C4A8", 2, "#F4996F", 3, "#F06A35", 4, "#C2461A", "#8C2F10" )"""),
    # Attention list formatting
    block("Color Attention", 'IF ( SELECTEDVALUE ( Attention[Severity] ) <= 1, "#FDE2E2", IF ( SELECTEDVALUE ( Attention[Severity] ) = 2, "#FDF1D6" ) )'),
    block("Font Attention", 'SWITCH ( SELECTEDVALUE ( Attention[Severity] ), 1, "#B42318", 2, "#92600A", "#111827" )'),
    block("UI Attention Count", 'FORMAT ( COUNTROWS ( Attention ), "0" ) & " open items"'),
])

src = open(MEAS, encoding="utf-8").read()
# v4 defines 'Color Crash Share' by rank; v5 replaces it with a threshold rule
src = re.sub(r"\n\t/// UI:[^\n]*\n\tmeasure 'Color Crash Share'.*?\t\tisHidden\n(?:\t\t[^\n]*\n)*", "\n", src, flags=re.S)
src = re.sub(r"\n\t/// UI5:[^\n]*\n\tmeasure '[^']+'.*?\t\tisHidden\n(?:\t\t[^\n]*\n)*", "\n", src, flags=re.S)
src = src.replace("\n\tcolumn MeasureHost", UI5 + "\n\tcolumn MeasureHost", 1)
open(MEAS, "w", encoding="utf-8").write(src)

# ---- Receivables ageing: calculated columns on mart_customer_360
cust = os.path.join(DEF, "tables", "mart_customer_360.tmdl")
c = open(cust, encoding="utf-8").read()
if "payment_age_order" not in c:
    calc = """	column payment_age_order = VAR d = DATEDIFF ( mart_customer_360[last_payment_at], NOW (), DAY ) RETURN SWITCH ( TRUE (), mart_customer_360[unpaid_eur] <= 0, 0, ISBLANK ( mart_customer_360[last_payment_at] ), 5, d <= 30, 1, d <= 60, 2, d <= 90, 3, 4 )
		dataType: int64
		isHidden
		summarizeBy: none

	column payment_age_band = SWITCH ( mart_customer_360[payment_age_order], 0, "Paid up", 1, "0-30 days", 2, "31-60 days", 3, "61-90 days", 4, "90+ days", "Never paid" )
		dataType: string
		summarizeBy: none
		sortByColumn: payment_age_order

"""
    c = c.replace("\tpartition ", calc + "\tpartition ", 1)
    open(cust, "w", encoding="utf-8").write(c)

# ---- Attention list: one calculated table across DQ, incidents and revenue assurance
att = """table Attention

	column Area
		dataType: string
		summarizeBy: none
		sourceColumn: [Area]

	column Item
		dataType: string
		summarizeBy: none
		sourceColumn: [Item]

	column Issue
		dataType: string
		summarizeBy: none
		sourceColumn: [Issue]

	column Severity
		dataType: int64
		summarizeBy: none
		sourceColumn: [Severity]

	column Priority
		dataType: string
		summarizeBy: none
		sourceColumn: [Priority]
		sortByColumn: Severity

	partition Attention = calculated
		mode: import
		source = ```
				VAR sev = { ( "critical", 1 ), ( "high", 2 ), ( "medium", 3 ) }
				VAR dq =
				    SELECTCOLUMNS (
				        FILTER ( mart_dq_health, mart_dq_health[status] <> "PASS" ),
				        "Area", "Data quality",
				        "Item", mart_dq_health[dataset] & " · " & mart_dq_health[rule_id],
				        "Issue", mart_dq_health[check_name] & ": " & FORMAT ( mart_dq_health[rows_failed], "#,0" ) & " rows failed",
				        "Severity", SWITCH ( LOWER ( mart_dq_health[severity] ), "critical", 1, "high", 2, "medium", 3, 4 )
				    )
				VAR inc =
				    SELECTCOLUMNS (
				        FILTER ( mart_incidents, mart_incidents[status] <> "RESOLVED" && mart_incidents[sla_status] IN { "BREACHED", "UNASSIGNED" } ),
				        "Area", "Incidents",
				        "Item", mart_incidents[incident_id] & " · " & mart_incidents[priority],
				        "Issue", mart_incidents[sla_status] & ": " & mart_incidents[description],
				        "Severity", SWITCH ( mart_incidents[priority], "P1", 1, "P2", 2, "P3", 3, 4 )
				    )
				VAR rec =
				    SELECTCOLUMNS (
				        FILTER ( mart_revenue_assurance, mart_revenue_assurance[status] = "FAIL" ),
				        "Area", "Revenue",
				        "Item", mart_revenue_assurance[check_id] & " · " & mart_revenue_assurance[leak_type],
				        "Issue", "EUR " & FORMAT ( mart_revenue_assurance[amount_at_risk_eur], "#,0" ) & " at risk, " & FORMAT ( mart_revenue_assurance[rows_mismatched], "#,0" ) & " rows",
				        "Severity", SWITCH ( LOWER ( mart_revenue_assurance[severity] ), "critical", 1, "high", 2, "medium", 3, 4 )
				    )
				VAR all_items = UNION ( dq, inc, rec )
				RETURN
				    ADDCOLUMNS ( all_items, "Priority", SWITCH ( [Severity], 1, "Critical", 2, "High", 3, "Medium", "Low" ) )
				```
"""
att = att.replace('				VAR sev = { ( "critical", 1 ), ( "high", 2 ), ( "medium", 3 ) }\n', "")
open(os.path.join(DEF, "tables", "Attention.tmdl"), "w", encoding="utf-8").write(att)
model = os.path.join(DEF, "model.tmdl")
mt = open(model, encoding="utf-8").read()
if "ref table Attention" not in mt:
    mt = mt.replace("ref table _Measures", "ref table _Measures\nref table Attention", 1)
    open(model, "w", encoding="utf-8").write(mt)

# ---- Theme fixes: solid hairline grid, status colours out of the categorical order
tp = os.path.join(PB, "theme", "sentinel_theme.json")
t = json.load(open(tp, encoding="utf-8"))
t["dataColors"] = ["#2F6FDB", "#1FAE7A", "#6B5BD6", "#F06A35", "#0891B2", "#8A5A00"]  # validated: dataviz validate_palette.js, light
t["visualStyles"]["*"]["*"]["valueAxis"][0]["gridlineStyle"] = "solid"
json.dump(t, open(tp, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
print("v5 model: targets, status pills, attention list, ageing, theme fixes")


# ===== Part 3: failure rate, singular/plural labels, theme type sizes =====
MEAS = os.path.join(PB, "model", "DataSentinel.SemanticModel", "definition", "tables", "_Measures.tmdl")
src = open(MEAS, encoding="utf-8").read()

# "1 days" -> "1 day"
def plural(m):
    expr, word = m.group(1), m.group(2)
    return f'FORMAT ( {expr}, "0" ) & IF ( {expr} = 1, " {word[:-1]}", " {word}" )'


src = re.sub(r'FORMAT \( (DISTINCTCOUNT \( [^)]+\] \)), "0" \) & " (days|runs|months|versions|teams|plans|datasets)"',
             plural, src)

V6 = '''
	/// UI6: BI review
	measure 'DQ Failure Rate' = DIVIDE ( SUM ( mart_dq_health[rows_failed] ), SUM ( mart_dq_health[rows_checked] ) )
		formatString: 0.00%
		isHidden

	/// UI6: BI review
	measure 'Color Failure' = IF ( [DQ Failure Rate] > 1 - [Target DQ Pass], "#D9363E", "#2F6FDB" )
		isHidden
'''
src = re.sub(r"\n\t/// UI6:[^\n]*\n\tmeasure '[^']+'.*?\t\tisHidden\n(?:\t\t[^\n]*\n)*", "\n", src, flags=re.S)
src = src.replace("\n\tcolumn MeasureHost", V6 + "\n\tcolumn MeasureHost", 1)
open(MEAS, "w", encoding="utf-8").write(src)

tp = os.path.join(PB, "theme", "sentinel_theme.json")
th = json.load(open(tp, encoding="utf-8"))
star = th["visualStyles"]["*"]["*"]
star["categoryAxis"][0]["fontSize"] = 10
star["valueAxis"][0]["fontSize"] = 10
star["legend"][0]["fontSize"] = 10
star["labels"][0]["fontSize"] = 10
star["title"][0]["fontSize"] = 14
star["padding"] = [{"top": 10, "bottom": 8, "left": 14, "right": 14}]
for v in ("tableEx", "pivotTable"):
    th["visualStyles"][v]["*"]["values"][0]["fontSize"] = 10
th["visualStyles"]["tableEx"]["*"]["columnHeaders"][0]["fontSize"] = 10
th["visualStyles"]["tableEx"]["*"]["total"] = [{"totals": False}]
th["textClasses"]["label"]["fontSize"] = 10
json.dump(th, open(tp, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
print("v6 model: failure rate, plural labels, theme type sizes")

# ===== Part 4: refined SaaS styling (grey headers, zebra rows, grey-blue canvas) =====
th = json.load(open(tp, encoding="utf-8"))
vs = th["visualStyles"]
vs["page"]["*"]["background"] = [{"color": {"solid": {"color": "#EEF2F7"}}, "transparency": 0}]
vs["page"]["*"]["outspace"] = [{"color": {"solid": {"color": "#EEF2F7"}}, "transparency": 0}]
for v in ("tableEx", "pivotTable"):
    vs[v]["*"]["columnHeaders"][0].update({"backColor": {"solid": {"color": "#F1F3F6"}},
                                           "fontColor": {"solid": {"color": "#374151"}}, "bold": True})
    vs[v]["*"]["values"][0].update({"backColorPrimary": {"solid": {"color": "#FFFFFF"}},
                                    "backColorSecondary": {"solid": {"color": "#F7F9FC"}}})
th["backgroundLight"] = "#EEF2F7"
json.dump(th, open(tp, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
print("part 4: refined styling")

# ===== Part 5: bold Arial typography =====
th = json.load(open(tp, encoding="utf-8"))
for k, face in (("callout", "Arial Black"), ("title", "Arial"), ("header", "Arial"), ("label", "Arial")):
    th["textClasses"][k]["fontFace"] = face
star = th["visualStyles"]["*"]["*"]
star["title"][0].update({"fontFamily": "Arial", "bold": True})
star["subTitle"][0]["fontFamily"] = "Arial"
for k in ("categoryAxis", "valueAxis", "legend", "labels"):
    star[k][0]["fontFamily"] = "Arial"
star["labels"][0]["bold"] = True
for v in ("tableEx", "pivotTable"):
    vs = th["visualStyles"][v]["*"]
    vs["columnHeaders"][0].update({"fontFamily": "Arial", "bold": True})
    vs["values"][0]["fontFamily"] = "Arial"
    if "rowHeaders" in vs:
        vs["rowHeaders"][0].update({"fontFamily": "Arial", "bold": True})
json.dump(th, open(tp, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
print("part 5: bold Arial typography")


# ===== Part 7: aggregate measures that replace the list tables =====
V7 = [
    ("Attention Items", "COUNTROWS ( Attention )", "#,0"),
    ("Rows Failed", 'CALCULATE ( SUM ( mart_dq_health[rows_failed] ), mart_dq_health[status] <> "PASS" )', "#,0"),
    ("Case EUR", "SUM ( mart_recon_mismatches[amount_at_risk_eur] )", "€ #,0"),
    ("Avg Open Hours", 'CALCULATE ( AVERAGE ( mart_incidents[open_hours] ), mart_incidents[status] <> "RESOLVED" )', "#,0.0"),
    ("Customers With Dropped Calls", "CALCULATE ( [Customer Count], mart_customer_360[dropped_calls] > 0 )", "#,0"),
    ("Customers With App Crashes", "CALCULATE ( [Customer Count], mart_customer_360[app_crashes] > 0 )", "#,0"),
    ("Customers With Plan Changes", "CALCULATE ( [Customer Count], mart_customer_360[plan_changes] > 0 )", "#,0"),
]
src = open(MEAS, encoding="utf-8").read()
src = re.sub(r"\n\t/// UI7:[^\n]*\n\tmeasure '[^']+'.*?\t\tisHidden\n(?:\t\t[^\n]*\n)*", "\n", src, flags=re.S)
block7 = "".join(f"\n\t/// UI7: aggregate view\n\tmeasure '{n}' = {e}\n\t\tformatString: {f}\n\t\tisHidden\n" for n, e, f in V7)
src = src.replace("\n\tcolumn MeasureHost", block7 + "\n\tcolumn MeasureHost", 1)
open(MEAS, "w", encoding="utf-8").write(src)
print("part 7: aggregate measures")


# ===== Part 8: larger axis text (category and value axes) on every chart =====
AXIS_FONT = 13
th = json.load(open(tp, encoding="utf-8"))
star = th["visualStyles"]["*"]["*"]
for k in ("categoryAxis", "valueAxis"):
    star[k][0].update({"fontSize": AXIS_FONT, "titleFontSize": AXIS_FONT})
json.dump(th, open(tp, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
print("part 8: axis text", AXIS_FONT)
