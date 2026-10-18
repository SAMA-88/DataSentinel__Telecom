"""DataSentinel Power BI report pages (PBIR): sidebar navigation, KPI cards with status pills and
sparklines, chart cards, and the hidden Customer profile drill-through page.

Usage:  python PowerBi/tools/build_report.py [path/to/PowerBi/model]
Run it with Power BI Desktop closed, after build_model.py. It rewrites every page folder.
Canvas 1920 x 1080 (reference image 1600 x 900 scaled 1.2).
"""
import json
import os
import shutil
import sys

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "model")
REPORT = os.path.join(ROOT, "DataSentinel.Report")
DEF = os.path.join(REPORT, "definition")
PAGES = os.path.join(DEF, "pages")
M = "_Measures"

# Reference image palette
BLUE, GREEN, PURPLE, ORANGE, RED, AMBER = "#2F6FDB", "#1FAE7A", "#6B5BD6", "#F06A35", "#D9363E", "#F2B33D"
GREY, TRACK = "#9CA3AF", "#EDF0F4"
TEXT, SUB, BORDER, BG, WHITE = "#111827", "#6B7280", "#E3E8EF", "#EEF2F7", "#FFFFFF"
NAV_BG, NAV_ACTIVE_BG, NAV_TEXT, NAV_ICON = "#0B1B3D", "#1E4FD8", "#C3CCDA", "#8C99B0"
PILLS = {  # background, text
    "blue": ("#E8F0FD", "#1D4ED8"), "green": ("#E3F6EE", "#0F7A53"), "amber": ("#FDF1D6", "#92600A"),
    "red": ("#FDE2E2", "#B42318"), "yellow": ("#FCEFC7", "#7A5A00"), "grey": ("#F1F3F6", "#4B5563"),
}

VIS_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/2.7.0/schema.json"
PAGE_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.1.0/schema.json"

STRIP_W = 64      # always-visible menu strip with the ☰ button
SIDEBAR_W = 264   # slide-out menu panel, hidden until ☰ is clicked (bookmarks)
X0, X1 = STRIP_W + 30, 1886
W = X1 - X0
GAP = 20
KPI_Y, KPI_H = 120, 118
R2_Y, R2_H = 258, 326
R3_Y, R3_H = 604, 420

NAV = [("overview", "Overview", "⌂"), ("data-quality", "Data Quality", "◇"),
       ("revenue-assurance", "Revenue Assurance", "€"), ("incidents", "Incidents", "△"),
       ("customer-360", "Customer 360", "○"), ("network-app", "Network & App", "≋")]


# ---------- expression helpers ----------
def lit(v):
    return {"expr": {"Literal": {"Value": v}}}


def s(text):
    return "'" + text.replace("'", "''") + "'"


def color(c):
    return {"solid": {"color": lit(s(c))}}


def col(table, name):
    return {"Column": {"Expression": {"SourceRef": {"Entity": table}}, "Property": name}}


def meas(name):
    return {"Measure": {"Expression": {"SourceRef": {"Entity": M}}, "Property": name}}


def agg(table, name, fn=0):
    return {"Aggregation": {"Function": fn, "Expression": col(table, name)}}


AGG_NAMES = {0: "Sum", 1: "Average", 3: "Min", 4: "Max"}


def ref(field):
    if "Measure" in field:
        return f"{M}.{field['Measure']['Property']}"
    if "Column" in field:
        return f"{field['Column']['Expression']['SourceRef']['Entity']}.{field['Column']['Property']}"
    a = field["Aggregation"]
    c = a["Expression"]["Column"]
    return f"{AGG_NAMES[a['Function']]}({c['Expression']['SourceRef']['Entity']}.{c['Property']})"


def proj(field, display=None):
    if "Measure" in field:
        p = {"field": field, "queryRef": ref(field), "nativeQueryRef": field["Measure"]["Property"]}
    elif "Column" in field:
        p = {"field": field, "queryRef": ref(field), "nativeQueryRef": field["Column"]["Property"], "active": True}
    else:
        a = field["Aggregation"]
        p = {"field": field, "queryRef": ref(field),
             "nativeQueryRef": f"{AGG_NAMES[a['Function']]} of {a['Expression']['Column']['Property']}"}
    if display:
        p["displayName"] = display
    return p


def sourced(field, alias):
    out = json.loads(json.dumps(field))

    def walk(o):
        if isinstance(o, dict):
            if "SourceRef" in o and "Entity" in o["SourceRef"]:
                o["SourceRef"] = {"Source": alias}
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(out)
    return out


# ---------- filters ----------
def filter_not_in(table, column, values):
    f = col(table, column)
    return {"name": f"f_{column}_not_in", "field": f, "type": "Categorical", "filter": {
        "Version": 2, "From": [{"Name": "t", "Entity": table, "Type": 0}],
        "Where": [{"Condition": {"Not": {"Expression": {"In": {
            "Expressions": [sourced(f, "t")], "Values": [[{"Literal": {"Value": s(v)}}] for v in values]}}}}}]}}


def filter_top_n(table, column, n, by_measure):
    f = col(table, column)
    return {"name": f"f_top_{column}", "field": f, "type": "TopN", "filter": {
        "Version": 2,
        "From": [
            {"Name": "subquery", "Expression": {"Subquery": {"Query": {
                "Version": 2,
                "From": [{"Name": "t", "Entity": table, "Type": 0}, {"Name": "m", "Entity": M, "Type": 0}],
                "Select": [{**sourced(f, "t"), "Name": "field"}],
                "OrderBy": [{"Direction": 2, "Expression": sourced(meas(by_measure), "m")}],
                "Top": n}}}, "Type": 2},
            {"Name": "t", "Entity": table, "Type": 0}],
        "Where": [{"Condition": {"In": {"Expressions": [sourced(f, "t")],
                                        "Table": {"SourceRef": {"Source": "subquery"}}}}}]}}


# ---------- formatting ----------
ALL_POINTS = {"data": [{"dataViewWildcard": {"matchingOption": 1}}]}
OFF = lambda: [{"properties": {"show": lit("false")}}]


def series_color(field, c):
    return {"properties": {"fill": color(c)}, "selector": {"metadata": ref(field)}}


def series_color_by_measure(field, color_measure):
    return {"properties": {"fill": {"solid": {"color": {"expr": meas(color_measure)}}}},
            "selector": {**ALL_POINTS, "metadata": ref(field)}}


def value_color(table, column, value, c):
    return {"properties": {"fill": color(c)}, "selector": {"data": [{"scopeId": {"Comparison": {
        "ComparisonKind": 0, "Left": col(table, column), "Right": {"Literal": {"Value": s(value)}}}}}]}}


def cell_rule(bg_measure, font_measure, target):
    return [{"properties": {"backColor": {"solid": {"color": {"expr": meas(bg_measure)}}},
                            "fontColor": {"solid": {"color": {"expr": meas(font_measure)}}}},
             "selector": {**ALL_POINTS, "metadata": ref(target)}}]


def shadow():
    return [{"properties": {"show": lit("true"), "color": color("#0B1B3D"), "transparency": lit("90D"),
                            "position": lit("'Outer'"), "preset": lit("'Custom'"), "shadowBlur": lit("14D"),
                            "shadowDistance": lit("3D"), "shadowSpread": lit("0D"), "angle": lit("90D")}}]


def frame(title=None, subtitle=None, bg=WHITE, border=True, radius="12D", border_color=BORDER, pad=True):
    o = {"background": [{"properties": {"show": lit("true" if bg else "false"),
                                        **({"color": color(bg), "transparency": lit("0D")} if bg else {})}}],
         "border": [{"properties": {"show": lit("true" if border else "false"), "color": color(border_color),
                                    "radius": lit(radius), "width": lit("1D")}}],
         "dropShadow": shadow() if (bg and border) else OFF(),
         "visualHeader": OFF()}
    if title:
        o["title"] = [{"properties": {"show": lit("true"), "text": lit(s(title)), "alignment": lit("'left'"),
                                      "fontColor": color(TEXT), "fontSize": lit("14D"), "bold": lit("true"),
                                      "fontFamily": lit("'Arial'")}}]
    else:
        o["title"] = OFF()
    if subtitle:
        o["subTitle"] = [{"properties": {"show": lit("true"), "text": lit(s(subtitle)), "fontColor": color(SUB),
                                         "fontSize": lit("10D"), "fontFamily": lit("'Arial'")}}]
    if pad:
        o["padding"] = [{"properties": {"top": lit("10D"), "bottom": lit("8D"), "left": lit("14D"),
                                        "right": lit("14D")}}]
    else:
        o["padding"] = [{"properties": {"top": lit("0D"), "bottom": lit("0D"), "left": lit("0D"),
                                        "right": lit("0D")}}]
    return o


# ---------- visual builders ----------
VISUALS = []


CODE = {"revenue-assurance": "ra", "customer-profile": "cp", "customer-360": "cu", "data-quality": "dq",
        "network-app": "nw", "incidents": "in", "overview": "ov"}


def put(page, name, pos, visual, filters=None):
    for k, v in CODE.items():
        name = name.replace(k, v)
    x, y, w, h = (round(v) for v in pos)
    z = len(VISUALS)
    doc = {"$schema": VIS_SCHEMA, "name": name,
           "position": {"x": x, "y": y, "z": z, "width": w, "height": h, "tabOrder": z},
           "visual": visual}
    if filters:
        doc["filterConfig"] = {"filters": filters}
    if PANEL_MODE:  # menu panel: hidden by default, drawn above the page, toggled by bookmarks
        doc["isHidden"] = True
        doc["position"]["z"] = 50000 + z
        PANEL.setdefault(page, []).append((name, visual["visualType"]))
    VISUALS.append((page, name, doc))


PANEL_MODE = False
PANEL = {}


def bookmark_name(page, state):
    return f"{CODE[page]}-menu-{state}"


def click_layer(page, name, pos, link):
    """Invisible clickable area; link = ('PageNavigation', page) or ('Bookmark', bookmark name)."""
    kind, target = link
    key = "navigationSection" if kind == "PageNavigation" else "bookmark"
    put(page, name, pos, {
        "visualType": "shape",
        "objects": {"shape": [{"properties": {"tileShape": lit("'rectangle'")}}],
                    "fill": [{"properties": {"show": lit("false")}}],
                    "outline": OFF()},
        "visualContainerObjects": {
            "visualLink": [{"properties": {"show": lit("true"), "type": lit(s(kind)), key: lit(s(target))}}],
            "background": OFF(), "border": OFF(), "dropShadow": OFF(), "title": OFF(), "visualHeader": OFF()}})


def query(buckets, sort=None, displays=None):
    displays = displays or {}
    q = {"queryState": {k: {"projections": [proj(f, displays.get(ref(f))) for f in v]} for k, v in buckets.items()}}
    if sort:
        q["sortDefinition"] = {"sort": [{"field": sort[0], "direction": sort[1]}], "isDefaultSort": True}
    return q


def chart(page, name, vtype, pos, buckets, title, subtitle=None, objects=None, sort=None, filters=None,
          displays=None, meta=None):
    put(page, name, pos, {"visualType": vtype, "query": query(buckets, sort, displays),
                          "visualContainerObjects": frame(title, subtitle),
                          "objects": objects or {}, "drillFilterOtherVisuals": True}, filters)
    if meta:
        x, y, w, h = pos
        chip = (x + w - 156, y + 12, 140, 30)
        shape(page, f"{name}-meta-bg", chip, WHITE, radius="8D", border="#D9DEE7")
        if meta.startswith("="):  # measure-driven text
            text_card(page, f"{name}-meta", (chip[0] + 4, chip[1] + 2, chip[2] - 8, 26), meta[1:], "#374151", 9)
        else:
            textbox(page, f"{name}-meta", (chip[0], chip[1] + 6, chip[2], 22),
                    [(meta, {"fontSize": "9pt", "color": "#374151"})], align="center")


def shape(page, name, pos, fill, radius="12D", border=None, nav=None, lifted=False):
    vco = {"background": [{"properties": {"show": lit("true"), "color": color(fill), "transparency": lit("0D")}}],
           "border": [{"properties": {"show": lit("true" if border else "false"),
                                      **({"color": color(border), "width": lit("1D")} if border else {}),
                                      "radius": lit(radius)}}],
           "dropShadow": shadow() if lifted else OFF(), "title": OFF(), "visualHeader": OFF()}
    if nav:
        vco["visualLink"] = [{"properties": {"show": lit("true"), "type": lit("'PageNavigation'"),
                                             "navigationSection": lit(s(nav))}}]
    put(page, name, pos, {"visualType": "shape",
                          "objects": {"shape": [{"properties": {"tileShape": lit("'rectangle'")}}],
                                      "fill": [{"properties": {"show": lit("false")}}],
                                      "outline": OFF()},
                          "visualContainerObjects": vco})


def textbox(page, name, pos, runs, align="left", paragraphs=None):
    paras = paragraphs or [runs]
    put(page, name, pos, {
        "visualType": "textbox",
        "objects": {"general": [{"properties": {"paragraphs": [
            {"textRuns": [{"value": t, "textStyle": {"fontFamily": "Arial", **st}} for t, st in p],
             "horizontalTextAlignment": align}
            for p in paras]}}]},
        "visualContainerObjects": {"background": OFF(), "border": OFF(), "dropShadow": OFF(),
                                   "visualHeader": OFF(),
                                   "padding": [{"properties": {"top": lit("0D"), "bottom": lit("0D"),
                                                               "left": lit("0D"), "right": lit("0D")}}]}})


def text_card(page, name, pos, measure, fg, size, bg=None, bold=False, align="left", radius="14D", nav=None,
              dynamic=False):
    """A classic card used to show a text measure (pill, meta label). dynamic: colours from <measure> Bg/Fg."""
    vco = frame(bg=bg, border=False, radius=radius, pad=False)
    vco["border"] = [{"properties": {"show": lit("true" if bg else "false"),
                                     "color": color(bg or WHITE), "radius": lit(radius)}}]
    if dynamic:
        dyn = lambda suffix: {"solid": {"color": {"expr": meas(f"{measure} {suffix}")}}}
        vco["background"] = [{"properties": {"show": lit("true"), "color": dyn("Bg"), "transparency": lit("0D")}}]
        vco["border"] = [{"properties": {"show": lit("true"), "color": dyn("Bg"), "radius": lit(radius)}}]
    if nav:
        vco["visualLink"] = [{"properties": {"show": lit("true"), "type": lit("'PageNavigation'"),
                                             "navigationSection": lit(s(nav))}}]
    put(page, name, pos, {
        "visualType": "card",
        "query": query({"Values": [meas(measure)]}),
        "visualContainerObjects": vco,
        "objects": {"categoryLabels": OFF(),
                    "labels": [{"properties": {"color": ({"solid": {"color": {"expr": meas(f"{measure} Fg")}}}
                                                         if dynamic else color(fg)), "fontSize": lit(f"{size}D"),
                                               "bold": lit("true" if bold else "false"),
                                               "fontFamily": lit("'Arial'")}}]},
        "drillFilterOtherVisuals": True})


def kpi(page, name, pos, label, dot, measure, pill_measure=None, pill="blue", spark=None, nav=None,
        units="0D", decimals=None):
    """White KPI card: dot + label, big left-aligned number, status pill, optional sparkline on the right."""
    x, y, w, h = pos
    shape(page, f"{name}-bg", pos, WHITE, border=BORDER, nav=nav, lifted=True)
    textbox(page, f"{name}-label", (x + 18, y + 14, w - 36, 22),
            [("● ", {"fontSize": "10pt", "color": dot}),
             (label, {"fontSize": "11pt", "color": "#374151", "fontWeight": "bold"})])
    region = w * 0.58 if spark else w - 24
    vco = frame(bg=None, border=False, pad=False)
    if nav:
        vco["visualLink"] = [{"properties": {"show": lit("true"), "type": lit("'PageNavigation'"),
                                             "navigationSection": lit(s(nav))}}]
    put(page, f"{name}-value", (x + 10, y + 36, region, 50), {
        "visualType": "multiRowCard", "query": query({"Values": [meas(measure)]}),
        "visualContainerObjects": vco,
        "objects": {"categoryLabels": OFF(),
                    "dataLabels": [{"properties": {"color": color(TEXT), "fontSize": lit("24D"),
                                                   "fontFamily": lit("'Arial Black'")}}],
                    "card": [{"properties": {"barShow": lit("false"), "outlineStyle": lit("0D")}}]},
        "drillFilterOtherVisuals": True})
    if pill_measure:
        bg, fg = PILLS["grey" if pill == "status" else pill]
        text_card(page, f"{name}-pill", (x + 18, y + 88, min(region - 24, 250), 24), pill_measure, fg, 9,
                  bg=bg, bold=True, nav=nav, dynamic=pill == "status")
    if spark:
        fx, fy, c = spark
        put(page, f"{name}-spark", (x + region + 4, y + 34, w - region - 18, 72), {
            "visualType": "lineChart", "query": query({"Category": [fx], "Y": [fy]}),
            "visualContainerObjects": frame(bg=None, border=False, pad=False),
            "objects": {"categoryAxis": OFF(), "valueAxis": OFF(), "legend": OFF(),
                        "dataPoint": [series_color(fy, c)],
                        "lineStyles": [{"properties": {"strokeWidth": lit("2D"), "showMarker": lit("false")}}]},
            "drillFilterOtherVisuals": False})


def pill_slicer(page, name, pos, label, table, column, mode="Dropdown"):
    x, y, w, h = pos
    shape(page, f"{name}-bg", pos, WHITE, radius="22D", border="#DADDE3")
    textbox(page, f"{name}-label", (x + 16, y + 12, 110, 22), [(label, {"fontSize": "10pt", "color": "#4B5563"})])
    lw = 8 * len(label) + 22
    objects = {"data": [{"properties": {"mode": lit(s(mode))}}], "header": OFF(),
               "items": [{"properties": {"fontColor": color(TEXT), "bold": lit("true"), "fontSize": lit("10D"),
                                         "fontFamily": lit("'Arial'"),
                                         "outline": lit("'None'")}}]}
    if mode == "Between":
        objects["slider"] = OFF()
    put(page, name, (x + lw, y + 2, w - lw - 12, h - 4), {
        "visualType": "slicer", "query": query({"Values": [col(table, column)]}),
        "visualContainerObjects": frame(bg=None, border=False, pad=False),
        "objects": objects, "drillFilterOtherVisuals": True})


def sample_pill(page, pos):
    x, y, w, h = pos
    shape(page, f"{page}-sample-bg", pos, "#2F5BB7", radius="22D", lifted=True)
    textbox(page, f"{page}-sample", (x, y + 11, w, 24),
            [("Sample data", {"fontSize": "10pt", "fontWeight": "bold", "color": WHITE})], align="center")


def nav_button(page, target, label, icon, y, active):
    x, w, h = STRIP_W + 16, SIDEBAR_W - 32, 44
    if active:
        shape(page, f"{page}-nav-{target}-bg", (x, y, w, h), NAV_ACTIVE_BG, radius="8D")
    textbox(page, f"{page}-nav-{target}-txt", (x + 16, y + 11, w - 24, 24),
            [(f"{icon}   ", {"fontSize": "11pt", "color": WHITE if active else NAV_ICON}),
             (label, {"fontSize": "11pt", "color": WHITE if active else NAV_TEXT, "fontWeight": "bold"})])
    click_layer(page, f"{page}-nav-{target}", (x, y, w, h), ("PageNavigation", target))


def chrome(page, title, subtitle, number=None, pills=()):
    """Menu strip with ☰, slide-out menu panel (hidden), title block, filter pills, sample-data pill, footer."""
    global PANEL_MODE
    # Always visible: the narrow navy strip and the ☰ button that opens the panel
    shape(page, f"{page}-strip", (0, 0, STRIP_W, 1080), NAV_BG, radius="0D")
    textbox(page, f"{page}-menu-icon", (0, 22, STRIP_W, 36), [("☰", {"fontSize": "18pt", "color": "#C3CCDA"})],
            align="center")
    click_layer(page, f"{page}-menu-open", (8, 16, STRIP_W - 16, 44), ("Bookmark", bookmark_name(page, "open")))

    # Slide-out panel: same navy, logo, page links, Gold layer note, ✕ to close
    PANEL_MODE = True
    px = STRIP_W
    shape(page, f"{page}-panel", (px, 0, SIDEBAR_W, 1080), NAV_BG, radius="0D", lifted=True)
    shape(page, f"{page}-logo-bg", (px + 22, 28, 40, 40), BLUE, radius="10D")
    textbox(page, f"{page}-logo-icon", (px + 22, 33, 40, 30), [("◈", {"fontSize": "15pt", "color": WHITE})],
            align="center")
    textbox(page, f"{page}-logo-text", (px + 72, 33, 150, 32),
            [("DataSentinel", {"fontSize": "15pt", "fontWeight": "bold", "color": WHITE})])
    textbox(page, f"{page}-menu-x", (px + SIDEBAR_W - 44, 30, 30, 30), [("✕", {"fontSize": "14pt", "color": "#C3CCDA"})],
            align="center")
    click_layer(page, f"{page}-menu-close", (px + SIDEBAR_W - 48, 24, 40, 40), ("Bookmark", bookmark_name(page, "closed")))
    textbox(page, f"{page}-nav-label", (px + 26, 106, 200, 20),
            [("REPORT", {"fontSize": "9pt", "fontWeight": "bold", "color": "#7C8BA8"})])
    for i, (target, label, icon) in enumerate(NAV):
        nav_button(page, target, label, icon, 136 + i * 48, target == page)
    shape(page, f"{page}-gold-bg", (px + 18, 962, SIDEBAR_W - 36, 92), "#E9EEF7", radius="10D")
    textbox(page, f"{page}-gold-text", (px + 34, 976, SIDEBAR_W - 66, 70), [], paragraphs=[
        [("Gold layer", {"fontSize": "10pt", "fontWeight": "bold", "color": TEXT})],
        [("12 mart tables · refresh after each pipeline run", {"fontSize": "9pt", "color": SUB})]])
    PANEL_MODE = False

    textbox(page, f"{page}-title", (X0, 32, 900, 40), [(title, {"fontSize": "21pt", "fontWeight": "bold",
                                                                "color": TEXT})])
    textbox(page, f"{page}-subtitle", (X0, 80, 900, 24), [(subtitle, {"fontSize": "11pt", "color": "#4B5563"})])
    x = X1 - 116
    sample_pill(page, (x, 62, 116, 44))
    for label, table, column, width, mode in reversed(pills):
        x -= width + 10
        pill_slicer(page, f"{page}-pill-{column}", (x, 62, width, 44), label, table, column, mode)
    textbox(page, f"{page}-footer", (X0, 1046, 900, 22),
            [("DataSentinel · design preview with sample values, real values come from the Gold marts",
              {"fontSize": "8pt", "color": GREY})])
    if number:
        textbox(page, f"{page}-pageno", (X1 - 200, 1046, 200, 22),
                [(f"Page {number} of 6", {"fontSize": "8pt", "color": GREY})], align="right")


def cols(n, y, h, x0=X0, width=W, weights=None):
    weights = weights or [1] * n
    unit = (width - GAP * (n - 1)) / sum(weights)
    out, x = [], x0
    for wgt in weights:
        out.append((x, y, unit * wgt, h))
        x += unit * wgt + GAP
    return out


# ---------- chart recipes ----------
def axis_objects(legend=False, value_axis=True, labels=False):
    o = {"legend": [{"properties": {"show": lit("true" if legend else "false"), "position": lit("'Top'")}}],
         "valueAxis": [{"properties": {"show": lit("true" if value_axis else "false"),
                                       "gridlineShow": lit("true" if value_axis else "false")}}]}
    if labels:
        o["labels"] = [{"properties": {"show": lit("true")}}]
    return o


def track_bar(page, name, pos, category, value, track_measure, title, subtitle, color_of=None, meta=None,
              sort_dir="Descending", filters=None, label_fmt=None):
    """Horizontal bar on a light-grey track with the value printed on the bar (reference 'Worst 10 cells')."""
    tr = meas(track_measure)
    dp = [series_color(tr, TRACK)]
    dp.append(series_color_by_measure(value, color_of) if color_of and color_of.startswith("Color")
              else series_color(value, color_of or BLUE))
    objects = {
        "legend": OFF(), "valueAxis": OFF(),
        "categoryAxis": [{"properties": {"labelColor": color(TEXT), "fontSize": lit("13D")}}],
        "dataPoint": dp,
        "labels": [{"properties": {"show": lit("true"), "color": color(WHITE), "fontSize": lit("10D"),
                                   "labelPosition": lit("'InsideEnd'"), "bold": lit("true")}},
                   {"properties": {"showSeries": lit("false")}, "selector": {"metadata": ref(tr)}}],
    }
    chart(page, name, "barChart", pos, {"Category": [category], "Y": [value, tr]}, title, subtitle, objects,
          sort=(value, sort_dir), filters=filters, meta=meta)


def area(page, name, pos, category, value, title, subtitle, c, meta=None):
    chart(page, name, "areaChart", pos, {"Category": [category], "Y": [value]}, title, subtitle,
          {**axis_objects(), "dataPoint": [series_color(value, c)],
           "lineStyles": [{"properties": {"strokeWidth": lit("2D"), "showMarker": lit("false")}}]}, meta=meta)


def columns(page, name, pos, category, value, title, subtitle, c=BLUE, series=None, colors=None, meta=None,
            sort=None, legend=None):
    buckets = {"Category": [category], "Y": [value]}
    dp = []
    if series:
        buckets["Series"] = [series]
        for v, cc in (colors or []):
            dp.append(value_color(series["Column"]["Expression"]["SourceRef"]["Entity"],
                                  series["Column"]["Property"], v, cc))
    else:
        dp.append(series_color(value, c))
    chart(page, name, "columnChart", pos, buckets, title, subtitle,
          {**axis_objects(legend=bool(series) if legend is None else legend), "dataPoint": dp}, sort=sort, meta=meta)


def table(page, name, pos, fields, title, subtitle=None, sort=None, filters=None, rules=None, meta=None,
          displays=None):
    objects = {"values": rules or [], "total": [{"properties": {"totals": lit("false")}}]}
    chart(page, name, "tableEx", pos, {"Values": fields}, title, subtitle, objects, sort=sort, filters=filters,
          displays=displays, meta=meta)


SLA_COLORS = [("RESOLVED_IN_SLA", GREEN), ("ON_TRACK", BLUE), ("RESOLVED_LATE", AMBER),
              ("UNASSIGNED", GREY), ("BREACHED", RED)]
THREE = cols(3, KPI_Y, KPI_H)
FOUR = cols(4, KPI_Y, KPI_H)
FIVE = cols(5, KPI_Y, KPI_H)
HALF2 = cols(2, R2_Y, R2_H)       # row 2: two equal charts
HALF3 = cols(2, R3_Y, R3_H)       # row 3: two equal charts
PRIORITY_COLORS = [("Critical", RED), ("High", ORANGE), ("Medium", AMBER), ("Low", GREY)]
SEVERITY_COLORS = [("critical", RED), ("high", ORANGE), ("medium", AMBER), ("low", GREY)]


def hbar(page, name, pos, category, value, title, subtitle, c=BLUE, top=None, meta=None, colors=None):
    """Horizontal bars, biggest first, value printed at the end of each bar; optional top-N."""
    cat_tbl = category["Column"]["Expression"]["SourceRef"]["Entity"]
    cat_col = category["Column"]["Property"]
    dp = ([value_color(cat_tbl, cat_col, v, cc) for v, cc in colors] if colors
          else [series_color(value, c)])
    chart(page, name, "clusteredBarChart", pos, {"Category": [category], "Y": [value]}, title, subtitle,
          {"legend": OFF(), "valueAxis": OFF(),
           "categoryAxis": [{"properties": {"labelColor": color(TEXT), "fontSize": lit("13D")}}],
           "dataPoint": dp,
           "labels": [{"properties": {"show": lit("true"), "color": color(TEXT), "fontSize": lit("10D"),
                                      "labelPosition": lit("'OutsideEnd'")}}]},
          sort=(value, "Descending"), meta=meta or (f"top {top}" if top else None),
          filters=[filter_top_n(cat_tbl, cat_col, top, value["Measure"]["Property"])] if top else None)


# =====================================================================
# Page 1 Overview
P = "overview"
chrome(P, "Overview", "Is today's data healthy, and what needs attention first?", 1)
kpi(P, "ov-dq", FIVE[0], "DQ pass rate", BLUE, "DQ Pass Rate", "UI DQ Pill", "status",
    (col("mart_dq_trend", "run_date"), meas("DQ Trend Pass Rate"), BLUE), nav="data-quality")
kpi(P, "ov-breached", FIVE[1], "SLA breached", RED, "SLA Breached", "UI SLA Pill", "status", nav="incidents")
kpi(P, "ov-risk", FIVE[2], "EUR at risk (data defects)", ORANGE, "EUR At Risk", "UI Defect Pill", "status",
    (col("mart_revenue_assurance_trend", "run_ts"), meas("Recon Risk Run"), ORANGE), nav="revenue-assurance",
    units="1D")
kpi(P, "ov-unpaid", FIVE[3], "Unpaid EUR", ORANGE, "Unpaid EUR", "UI Collection Pill", "status", nav="customer-360",
    units="1D")
kpi(P, "ov-drop", FIVE[4], "Dropped call rate", PURPLE, "Drop Rate", "UI Drop Pill", "status",
    (col("mart_network_daily", "usage_date"), meas("Drop Rate"), PURPLE), nav="network-app")
chart(P, "ov-pass-trend", "lineChart", HALF2[0],
      {"Category": [col("mart_dq_trend", "run_date")], "Y": [meas("DQ Trend Pass Rate"), meas("Target DQ Pass")]},
      "Pass rate by day", "Gate rules, all datasets, vs 98% target",
      {**axis_objects(legend=True), "dataPoint": [series_color(meas("DQ Trend Pass Rate"), BLUE),
                                                  series_color(meas("Target DQ Pass"), GREY)],
       "lineStyles": [{"properties": {"strokeWidth": lit("2D"), "showMarker": lit("true"),
                                      "markerSize": lit("4D")}}]},
      displays={f"{M}.DQ Trend Pass Rate": "Pass rate", f"{M}.Target DQ Pass": "Target"}, meta="=UI Runs DQ")
track_bar(P, "ov-risk-type", HALF2[1], col("mart_revenue_assurance", "leak_type"), meas("Gross EUR At Risk"),
          "Leak Risk Track", "EUR at risk by leak type", "Latest reconciliation run, gross", ORANGE,
          meta="=UI Runs Recon")
chart(P, "ov-issues", "barChart", HALF3[0],
      {"Category": [col("Attention", "Area")], "Y": [meas("Attention Items")],
       "Series": [col("Attention", "Priority")]},
      "Open issues by area", "Failing DQ rules, late incidents and failing revenue checks, by priority",
      {**axis_objects(legend=True, labels=True),
       "categoryAxis": [{"properties": {"labelColor": color(TEXT), "fontSize": lit("13D")}}],
       "dataPoint": [value_color("Attention", "Priority", v, c) for v, c in PRIORITY_COLORS]},
      sort=(meas("Attention Items"), "Descending"), meta="=UI Attention Count")
columns(P, "ov-priority", HALF3[1], col("mart_incidents", "priority"), meas("Incident Count"),
        "Incidents by priority", "Count, by SLA status", series=col("mart_incidents", "sla_status"),
        colors=SLA_COLORS, sort=(col("mart_incidents", "priority"), "Ascending"), meta="=UI Incidents Total")

# =====================================================================
# Page 2 Data Quality
P = "data-quality"
chrome(P, "Data Quality", "Which datasets and rules fail, is it getting better, how many rows wait in quarantine?", 2,
       pills=[("Dataset", "mart_dq_health", "dataset", 250, "Dropdown"),
              ("Dimension", "mart_dq_health", "dimension", 250, "Dropdown")])
kpi(P, "dq-pass", THREE[0], "DQ pass rate", BLUE, "DQ Pass Rate", "UI DQ Pill", "status",
    (col("mart_dq_trend", "run_date"), meas("DQ Trend Pass Rate"), BLUE))
kpi(P, "dq-failing", THREE[1], "Failing rules", RED, "Failing Rules", "UI Failing Pill", "status")
kpi(P, "dq-quarantine", THREE[2], "Quarantined rows", PURPLE, "Quarantined Rows", "UI Quarantine Pill", "status")
chart(P, "dq-fail-dataset", "clusteredBarChart", HALF2[0],
      {"Category": [col("mart_dq_health", "dataset")], "Y": [meas("DQ Failure Rate")]},
      "Failure rate by dataset", "Failed / checked rows, worst first, red above 2%",
      {"legend": OFF(), "valueAxis": OFF(),
       "categoryAxis": [{"properties": {"labelColor": color(TEXT), "fontSize": lit("13D")}}],
       "dataPoint": [{"properties": {"fill": {"solid": {"color": {"expr": meas("Color Failure")}}}},
                      "selector": ALL_POINTS}],
       "labels": [{"properties": {"show": lit("true"), "color": color(TEXT), "fontSize": lit("10D"),
                                  "labelPosition": lit("'OutsideEnd'")}}]},
      sort=(meas("DQ Failure Rate"), "Descending"), meta="=UI Datasets DQ")
chart(P, "dq-quarantine", "barChart", HALF2[1],
      {"Category": [col("mart_quarantine", "dataset")], "Y": [meas("Quarantined Rows")],
       "Series": [col("mart_quarantine", "quarantine_status")]},
      "Quarantine by dataset", "Rows waiting, by quarantine status, most first",
      {**axis_objects(legend=True), "categoryAxis": [{"properties": {"labelColor": color(TEXT),
                                                                     "fontSize": lit("13D")}}]},
      sort=(meas("Quarantined Rows"), "Descending"), meta="=UI Quarantine Datasets")
hbar(P, "dq-rows-failed", HALF3[0], col("mart_dq_health", "check_name"), meas("Rows Failed"),
     "Rows failed by check", "Latest result per rule, failing rules only", RED, top=10)
hbar(P, "dq-severity", HALF3[1], col("mart_dq_health", "severity"), meas("Failing Rules"),
     "Failing rules by severity", "Number of rules that are not PASS", colors=SEVERITY_COLORS, meta="=UI Rules Total")

# =====================================================================
# Page 3 Revenue Assurance
P = "revenue-assurance"
chrome(P, "Revenue Assurance", "How many EUR leak between systems, where, and which cases must be investigated?", 3,
       pills=[("Defect or business", "mart_revenue_assurance", "is_data_defect", 300, "Dropdown")])
kpi(P, "ra-gross", THREE[0], "EUR at risk", ORANGE, "Gross EUR At Risk", "UI Gross Note", "amber",
    (col("mart_revenue_assurance_trend", "run_ts"), meas("Recon Risk Run"), ORANGE), units="1D")
kpi(P, "ra-failing", THREE[1], "Checks failing", RED, "Checks Failing", "UI Checks Total", "red")
kpi(P, "ra-cases", THREE[2], "Cases to investigate", BLUE, "Cases To Investigate", "UI Latest Run", "blue")
track_bar(P, "ra-risk-type", HALF2[0], col("mart_revenue_assurance", "leak_type"), meas("Gross EUR At Risk"),
          "Leak Risk Track", "EUR at risk by leak type", "Click a bar to filter the other charts", ORANGE,
          meta="=UI Checks Total")
hbar(P, "ra-mismatch", HALF2[1], col("mart_revenue_assurance", "leak_type"), meas("Mismatched Rows"),
     "Mismatched rows by check", "Rows that differ between the two systems", PURPLE, meta="=UI Checks Total")
hbar(P, "ra-reason", HALF3[0], col("mart_recon_mismatches", "reason"), meas("Cases To Investigate"),
     "Cases by reason", "Latest run, what the billing team investigates", BLUE, top=10)
hbar(P, "ra-entity", HALF3[1], col("mart_recon_mismatches", "entity_type"), meas("Case EUR"),
     "Case EUR by entity type", "EUR at risk in the open cases", ORANGE, meta="=UI Latest Run")

# =====================================================================
# Page 4 Incidents
P = "incidents"
chrome(P, "Incidents", "Who owns each incident, how late is it, does the team meet its deadlines?", 4,
       pills=[("Priority", "mart_incidents", "priority", 190, "Dropdown"),
              ("Team", "mart_incidents", "team", 200, "Dropdown"),
              ("Dataset", "mart_incidents", "dataset", 220, "Dropdown"),
              ("Status", "mart_incidents", "status", 200, "Dropdown")])
kpi(P, "in-open", THREE[0], "Open", ORANGE, "Open Incidents", "UI Open 7d", "grey",
    (col("mart_incidents", "opened_at"), meas("Incident Count"), ORANGE))
kpi(P, "in-breached", THREE[1], "Breached", RED, "SLA Breached", "UI SLA Pill", "status")
kpi(P, "in-resolved", THREE[2], "Resolved in SLA", GREEN, "Resolved In SLA", "UI Compliance Pill", "status")
columns(P, "in-priority", HALF2[0], col("mart_incidents", "priority"), meas("Open Incidents"), "Open by priority",
        "Open incidents, by SLA status", series=col("mart_incidents", "sla_status"), colors=SLA_COLORS,
        sort=(col("mart_incidents", "priority"), "Ascending"))
track_bar(P, "in-team", HALF2[1], col("mart_incidents", "team"), meas("Open Incidents"), "Team Workload Track",
          "Workload by team", "Open incidents per team", BLUE, meta="=UI Teams")
hbar(P, "in-dataset", HALF3[0], col("mart_incidents", "dataset"), meas("Open Incidents"),
     "Open incidents by dataset", "Where the open incidents come from", ORANGE, top=10)
columns(P, "in-hours", HALF3[1], col("mart_incidents", "priority"), meas("Avg Open Hours"),
        "Average hours open by priority", "Open incidents; deadlines P1 1 h, P2 4 h, P3 24 h, P4 72 h", PURPLE,
        sort=(col("mart_incidents", "priority"), "Ascending"))

# =====================================================================
# Page 5 Customer 360
P = "customer-360"
chrome(P, "Customer 360", "Who has paid, who owes us money, which customers have problems?", 5,
       pills=[("Status", "mart_customer_360", "status", 200, "Dropdown"),
              ("City", "mart_customer_360", "city", 210, "Dropdown"),
              ("Plan", "mart_customer_360", "plan_name", 220, "Dropdown")])
kpi(P, "cu-count", FOUR[0], "Customers", BLUE, "Customer Count", "UI Active Share", "blue")
kpi(P, "cu-billed", FOUR[1], "Billed EUR", PURPLE, "Billed EUR", "UI Invoices", "grey", units="1D")
kpi(P, "cu-paid", FOUR[2], "Paid EUR", GREEN, "Paid EUR", "UI Collection Pill", "status", units="1D")
kpi(P, "cu-unpaid", FOUR[3], "Unpaid EUR", ORANGE, "Unpaid EUR", "UI Unpaid Owe", "grey", units="1D")
chart(P, "cu-aging", "columnChart", HALF2[0],
      {"Category": [col("mart_customer_360", "payment_age_band")], "Y": [meas("Unpaid EUR")]},
      "Unpaid EUR by payment age", "Days since last payment, older is darker",
      {**axis_objects(), "dataPoint": [{"properties": {"fill": {"solid": {"color": {"expr": meas("Color Aging")}}}},
                                        "selector": ALL_POINTS}]},
      sort=(col("mart_customer_360", "payment_age_band"), "Ascending"),
      filters=[filter_not_in("mart_customer_360", "payment_age_band", ["Paid up"])])
track_bar(P, "cu-unpaid-plan", HALF2[1], col("mart_customer_360", "plan_name"), meas("Unpaid EUR"),
          "Unpaid Plan Track", "Unpaid EUR by plan", "Billed minus paid", ORANGE, meta="=UI Plans")
hbar(P, "cu-unpaid-city", HALF3[0], col("mart_customer_360", "city"), meas("Unpaid EUR"),
     "Unpaid EUR by city", "Where the money is owed", ORANGE, top=10)
chart(P, "cu-problems", "clusteredColumnChart", HALF3[1],
      {"Y": [meas("Customers With Dropped Calls"), meas("Customers With App Crashes"),
             meas("Customers With Plan Changes")]},
      "Customers with problems", "Number of customers affected",
      {**axis_objects(legend=True, labels=True),
       "dataPoint": [series_color(meas("Customers With Dropped Calls"), ORANGE),
                     series_color(meas("Customers With App Crashes"), RED),
                     series_color(meas("Customers With Plan Changes"), PURPLE)]},
      displays={f"{M}.Customers With Dropped Calls": "Dropped calls",
                f"{M}.Customers With App Crashes": "App crashes",
                f"{M}.Customers With Plan Changes": "Plan changes"},
      meta="=UI Customers Shown")

# =====================================================================
# Page 6 Network & App
P = "network-app"
chrome(P, "Network & App", "Where do calls drop, how does the app behave, and what does partner traffic cost?", 6,
       pills=[("Event type", "mart_network_daily", "event_type", 190, "Dropdown"),
              ("App version", "mart_app_usage", "app_version", 200, "Dropdown"),
              ("Date range", "mart_network_daily", "usage_date", 330, "Between")])
nd = "mart_network_daily"
kpi(P, "nw-voice", FOUR[0], "Voice minutes", GREEN, "Voice Minutes", "UI Voice Delta", "blue",
    (col(nd, "usage_date"), meas("Voice Minutes"), GREEN))
kpi(P, "nw-data", FOUR[1], "Data", PURPLE, "Data GB", "UI Data Delta", "blue",
    (col(nd, "usage_date"), meas("Data GB"), PURPLE))
kpi(P, "nw-drop", FOUR[2], "Drop rate", ORANGE, "Drop Rate", "UI Drop Pill", "status",
    (col(nd, "usage_date"), meas("Drop Rate"), ORANGE), decimals=1)
kpi(P, "nw-partner", FOUR[3], "Partner cost, unknown customer", AMBER, "Unknown Partner Cost EUR",
    "UI Partner Pill", "status", (col("mart_partner_usage", "period_start"), meas("Unknown Partner Cost EUR"),
                                    AMBER), units="1D")
area(P, "nw-drop-day", HALF2[0], col(nd, "usage_date"), meas("Drop Rate"), "Dropped call rate per day",
     "Voice calls only", ORANGE, meta="=UI Days Network")
track_bar(P, "nw-cells", HALF2[1], col("mart_cell_quality", "cell_id"), meas("Cell Drop Rate"), "Cell Drop Track",
          "Worst 10 cells", "Drop rate, minimum 20 voice calls", ORANGE, meta="top 10 shown",
          filters=[filter_top_n("mart_cell_quality", "cell_id", 10, "Cell Drop Rate")])
track_bar(P, "nw-crash", HALF3[0], col("mart_app_usage", "app_version"), meas("Crash Share"), "Crash Share Track",
          "Crash share by app version", "Crashes / events, red above the 2% limit", "Color Crash Share",
          meta="=UI Versions App")
columns(P, "nw-partner-cost", HALF3[1], col("mart_partner_usage", "period_start"), meas("Partner Cost K"),
        "Partner cost by service per month", "EUR thousands", series=col("mart_partner_usage", "service"),
        meta="=UI Months Partner")


# =====================================================================
PAGE_META = [("overview", "Overview"), ("data-quality", "Data Quality"), ("revenue-assurance", "Revenue Assurance"),
             ("incidents", "Incidents"), ("customer-360", "Customer 360"),
             ("network-app", "Network & App")]


def dump(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)


shutil.rmtree(PAGES)
for name, display in PAGE_META:
    page = {"$schema": PAGE_SCHEMA, "name": name, "displayName": display}
    page.update({"displayOption": "FitToPage", "height": 1080, "width": 1920})
    page["objects"] = {"background": [{"properties": {"color": color(BG), "transparency": lit("0D")}}],
                       "outspace": [{"properties": {"color": color(BG), "transparency": lit("0D")}}]}
    dump(os.path.join(PAGES, name, "page.json"), page)
names = set()
for page, name, doc in VISUALS:
    assert (page, name) not in names, name
    names.add((page, name))
    dump(os.path.join(PAGES, page, "visuals", name, "visual.json"), doc)
dump(os.path.join(PAGES, "pages.json"), {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.1.0/schema.json",
    "pageOrder": [n for n, _ in PAGE_META], "activePageName": "network-app"})

# Bookmarks: per page, "menu open" shows the panel visuals and "menu closed" hides them.
# They touch only the panel visuals, keep the current page and leave filters/data alone.
BOOKMARKS = os.path.join(DEF, "bookmarks")
shutil.rmtree(BOOKMARKS, ignore_errors=True)
items = []
for page, display in PAGE_META:
    panel = PANEL.get(page, [])
    if not panel:
        continue
    for state in ("open", "closed"):
        bname = bookmark_name(page, state)
        visual_state = lambda vt: {"visualType": vt, **({} if state == "open" else {"display": {"mode": "hidden"}})}
        dump(os.path.join(BOOKMARKS, f"{bname}.bookmark.json"), {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/bookmark/2.1.0/schema.json",
            "displayName": f"{display}: menu {state}",
            "name": bname,
            "options": {"targetVisualNames": [n for n, _ in panel], "applyOnlyToTargetVisuals": True,
                        "suppressActiveSection": True, "suppressData": True},
            "explorationState": {"version": "1.3", "activeSection": page,
                                 "sections": {page: {"visualContainers": {
                                     n: {"singleVisual": visual_state(vt)} for n, vt in panel}}}}})
        items.append({"name": bname})
dump(os.path.join(BOOKMARKS, "bookmarks.json"), {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/bookmarksMetadata/1.0.0/schema.json",
    "items": items})

theme_src = os.path.join(ROOT, "..", "theme", "sentinel_theme.json")
reg_dir = os.path.join(REPORT, "StaticResources", "RegisteredResources")
os.makedirs(reg_dir, exist_ok=True)
shutil.copy(theme_src, os.path.join(reg_dir, "sentinel_theme.json"))
rep_path = os.path.join(DEF, "report.json")
rep = json.load(open(rep_path, encoding="utf-8"))
rep["themeCollection"]["customTheme"] = {
    "name": "sentinel_theme.json",
    "reportVersionAtImport": rep["themeCollection"]["baseTheme"]["reportVersionAtImport"],
    "type": "RegisteredResources"}
rep["resourcePackages"] = [p for p in rep["resourcePackages"] if p["type"] != "RegisteredResources"] + [{
    "name": "RegisteredResources", "type": "RegisteredResources",
    "items": [{"name": "sentinel_theme.json", "path": "sentinel_theme.json", "type": "CustomTheme"}]}]
dump(rep_path, rep)
print(f"wrote {len(VISUALS)} visuals on {len(PAGE_META)} pages")
