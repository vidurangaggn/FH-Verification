#!/usr/bin/env python3
"""
FH Verification - Automated test runner for FHE Staging vs Production Solr validation.
Runs test cases from Readme.md and generates an HTML report.
"""

import html as html_lib
import json
import os
import re
import sys
from datetime import datetime, timezone
from urllib.parse import quote
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# Load config
try:
    import config as cfg
except ImportError:
    PRODUCTION_SOLR_URL = "http://172.20.162.40:8985"
    STAGING_SOLR_URL = "http://172.20.162.250:8985"
    REQUIRED_CORES = [
        "FINANCIAL_RATIOS", "FH_FINANCIAL_RATIOS", "EARNINGS", "EARNINGS_HISTORY",
        "ACTUAL_EARNINGS_HISTORY", "PERFORMANCE_DATA", "TREND_DATA", "TECH",
        "SUPPORT_RESISTANCE", "COMPANY_PROFILE", "COMPANY_PROFILE_IMAGE",
        "NEWS", "NEWS_SENTIMENT", "ANALYST_VIEW", "CONSOLIDATED_SCORE", "PEERS",
    ]
    EXCHANGE_FIELD = "EXCHANGE"
    VARIANCE_THRESHOLD_PERCENT = 5.0
    MAX_TIME_DIFF_HOURS = 4.0
    TC005_PASS_WHEN_STAGING_AHEAD = True
    REQUEST_TIMEOUT = 30
    FRESHNESS_PERIODS_HOURS = [6, 24, 72]
    FRESHNESS_PERIOD_END_HOURS_AGO = 1
    WRITE_COUNT_VARIANCE_THRESHOLD_PERCENT = 10.0
    NOHUP_LOG_PATH = None
    PERIOD_COUNT_CORE_MAP = {}
    NEWS_DATE_FIELD = "CREATED_ON"
    NEWS_DATE_PERIOD_MONTHS = 3
    NEWS_DATE_FORMAT = None
    RUN_MODE = "both"
else:
    PRODUCTION_SOLR_URL = cfg.PRODUCTION_SOLR_URL
    STAGING_SOLR_URL = cfg.STAGING_SOLR_URL
    REQUIRED_CORES = cfg.REQUIRED_CORES
    EXCHANGE_FIELD = getattr(cfg, "EXCHANGE_FIELD", "EXCHANGE")
    VARIANCE_THRESHOLD_PERCENT = getattr(cfg, "VARIANCE_THRESHOLD_PERCENT", 5.0)
    MAX_TIME_DIFF_HOURS = getattr(cfg, "MAX_TIME_DIFF_HOURS", 4.0)
    TC005_PASS_WHEN_STAGING_AHEAD = getattr(cfg, "TC005_PASS_WHEN_STAGING_AHEAD", True)
    REQUEST_TIMEOUT = getattr(cfg, "REQUEST_TIMEOUT", 30)
    FRESHNESS_PERIODS_HOURS = getattr(cfg, "FRESHNESS_PERIODS_HOURS", [6, 24, 72])
    FRESHNESS_PERIOD_END_HOURS_AGO = getattr(cfg, "FRESHNESS_PERIOD_END_HOURS_AGO", 1)
    WRITE_COUNT_VARIANCE_THRESHOLD_PERCENT = getattr(cfg, "WRITE_COUNT_VARIANCE_THRESHOLD_PERCENT", 10.0)
    NOHUP_LOG_PATH = getattr(cfg, "NOHUP_LOG_PATH", None)
    PERIOD_COUNT_CORE_MAP = getattr(cfg, "PERIOD_COUNT_CORE_MAP", {}) or {}
    NEWS_DATE_FIELD = getattr(cfg, "NEWS_DATE_FIELD", "CREATED_ON")
    NEWS_DATE_PERIOD_MONTHS = getattr(cfg, "NEWS_DATE_PERIOD_MONTHS", 3)
    NEWS_DATE_FORMAT = getattr(cfg, "NEWS_DATE_FORMAT", None)
    RUN_MODE = getattr(cfg, "RUN_MODE", "both").strip().lower()
    if RUN_MODE not in ("solr", "logs", "both"):
        RUN_MODE = "both"


# Sentinel for undefined variance (division by zero when prod count = 0)
VARIANCE_UNDEFINED = -1


def _variance_pass(variance_pct, threshold_pct):
    """Pass when variance is negative (staging has more or same) OR absolute variance is within threshold. Returns None when undefined (prod=0)."""
    if variance_pct is None or variance_pct == VARIANCE_UNDEFINED:
        return None
    try:
        v = float(variance_pct)
        if v < 0:
            return True   # negative variance = pass (e.g. staging has more docs)
        return abs(v) <= float(threshold_pct)
    except (TypeError, ValueError):
        return None


def solr_get(url, timeout=REQUEST_TIMEOUT):
    """GET request to Solr; returns (status_code, body_dict or None, error_msg)."""
    try:
        req = Request(url, headers={"Accept": "application/json"})
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body), None
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8")
            data = json.loads(body) if body.strip() else None
        except Exception:
            data = None
        return e.code, data, str(e)
    except URLError as e:
        return None, None, str(e.reason)
    except json.JSONDecodeError as e:
        return 200, None, f"Invalid JSON: {e}"
    except Exception as e:
        return None, None, str(e)


# --- Test Suite 1: Core Health Check (no admin API; Solr often blocks admin requests) ---

def run_tc002_core_health():
    """TC-002: Core health check - basic query for each core."""
    results = {"passed": True, "checks": [], "errors": []}
    for env_name, base_url in [("Production", PRODUCTION_SOLR_URL), ("Staging", STAGING_SOLR_URL)]:
        for core in REQUIRED_CORES:
            url = f"{base_url}/solr/{core}/select?q=*:*&rows=0&wt=json"
            status, data, err = solr_get(url)
            healthy = status == 200 and data and "response" in data and "numFound" in data.get("response", {})
            results["checks"].append({
                "core": core,
                "environment": env_name,
                "status_code": status,
                "healthy": healthy,
                "numFound": data.get("response", {}).get("numFound") if data else None,
                "error": err,
            })
            if not healthy:
                results["passed"] = False
    return results


# --- Test Suite 2: Exchange-Wise Data Coverage ---

# Cores to use for exchange-wise coverage: all required cores (each may have EXCHANGE or SOURCE_ID)
def _exchange_coverage_cores():
    """Use all REQUIRED_CORES for exchange coverage so every core is included."""
    return REQUIRED_CORES


def run_tc003_exchange_coverage():
    """TC-003: Get exchanges per core in Prod and Staging for all cores; identify common / prod-only / staging-only."""
    cores = _exchange_coverage_cores()
    results = {"passed": True, "exchanges": {}, "common": set(), "prod_only": set(), "staging_only": set(), "errors": [], "all_exchanges_per_core": {}, "field_per_core": {}}
    all_prod = set()
    all_staging = set()
    for core in cores:
        results["exchanges"][core] = {"production": [], "staging": []}
        field_used = EXCHANGE_FIELD
        results["field_per_core"][core] = field_used
        for env_name, base_url in [("Production", PRODUCTION_SOLR_URL), ("Staging", STAGING_SOLR_URL)]:
            q = quote("*:*")
            ff = quote(EXCHANGE_FIELD)
            url = f"{base_url}/solr/{core}/select?q={q}&rows=0&facet=on&facet.field={ff}&wt=json"
            status, data, err = solr_get(url)
            if err or status != 200:
                results["errors"].append(f"{core} ({env_name}): {err or status}")
                continue
            facets = (data.get("facet_counts") or {}).get("facet_fields") or {}
            buckets = facets.get(EXCHANGE_FIELD) or []
            exchanges = [buckets[i] for i in range(0, len(buckets), 2) if isinstance(buckets[i], str)]
            results["exchanges"][core][env_name.lower()] = exchanges
            if env_name == "Production":
                all_prod.update(exchanges)
            else:
                all_staging.update(exchanges)
        # Fallback: if EXCHANGE facet empty, try SOURCE_ID
        if not results["exchanges"][core]["production"] and not results["exchanges"][core]["staging"] and EXCHANGE_FIELD == "EXCHANGE":
            field_used = "SOURCE_ID"
            results["field_per_core"][core] = field_used
            for env_name, base_url in [("Production", PRODUCTION_SOLR_URL), ("Staging", STAGING_SOLR_URL)]:
                url = f"{base_url}/solr/{core}/select?q=*:*&rows=0&facet=on&facet.field=SOURCE_ID&wt=json"
                status, data, err = solr_get(url)
                if status == 200 and data:
                    facets = (data.get("facet_counts") or {}).get("facet_fields") or {}
                    buckets = facets.get("SOURCE_ID") or []
                    exchanges = [str(buckets[i]) for i in range(0, len(buckets), 2)]
                    results["exchanges"][core][env_name.lower()] = exchanges
                    if env_name == "Production":
                        all_prod.update(exchanges)
                    else:
                        all_staging.update(exchanges)
        # Union of all exchanges for this core (for TC-004: show every exchange, missing env shown as —)
        prod_set = set(results["exchanges"][core]["production"])
        stage_set = set(results["exchanges"][core]["staging"])
        results["all_exchanges_per_core"][core] = sorted(prod_set | stage_set)
    results["common"] = list(all_prod & all_staging)
    results["prod_only"] = list(all_prod - all_staging)
    results["staging_only"] = list(all_staging - all_prod)
    return results


def run_tc004_exchange_counts(tc003_result):
    """TC-004: Exchange-wise document count for all cores; all exchanges (union of prod + staging). No data = 0."""
    cores = _exchange_coverage_cores()
    results = {"passed": True, "tables": {}, "errors": []}
    all_exchanges_per_core = tc003_result.get("all_exchanges_per_core") or {}
    exchanges_per_core = tc003_result.get("exchanges") or {}
    field_per_core = tc003_result.get("field_per_core") or {}
    for core in cores:
        results["tables"][core] = []
        exchange_list = all_exchanges_per_core.get(core) or []
        prod_exchanges = set((exchanges_per_core.get(core) or {}).get("production") or [])
        stage_exchanges = set((exchanges_per_core.get(core) or {}).get("staging") or [])
        facet_field = field_per_core.get(core) or EXCHANGE_FIELD
        for ex in exchange_list:
            row = {"exchange": ex, "prod_count": None, "stage_count": None, "variance_pct": None, "pass": None}
            in_prod = ex in prod_exchanges
            in_stage = ex in stage_exchanges
            q = quote(f'{facet_field}:"{ex}"')
            if in_prod:
                url = f"{PRODUCTION_SOLR_URL}/solr/{core}/select?q={q}&rows=0&wt=json"
                status, data, err = solr_get(url)
                if status == 200 and data:
                    row["prod_count"] = (data.get("response") or {}).get("numFound")
                else:
                    results["errors"].append(f"{core} {ex} (Prod): {err or status}")
            else:
                row["prod_count"] = 0
            if in_stage:
                url = f"{STAGING_SOLR_URL}/solr/{core}/select?q={q}&rows=0&wt=json"
                status, data, err = solr_get(url)
                if status == 200 and data:
                    row["stage_count"] = (data.get("response") or {}).get("numFound")
                else:
                    results["errors"].append(f"{core} {ex} (Stage): {err or status}")
            else:
                row["stage_count"] = 0
            prod, stage = row["prod_count"], row["stage_count"]
            if prod is not None and stage is not None:
                if prod > 0:
                    # Signed variance: (prod - stage)/prod*100 (negative = staging has more)
                    row["variance_pct"] = round((prod - stage) / prod * 100, 2)
                    row["pass"] = _variance_pass(row["variance_pct"], VARIANCE_THRESHOLD_PERCENT)
                else:
                    # prod=0: variance undefined (division by zero); set -1
                    row["variance_pct"] = VARIANCE_UNDEFINED
                    row["pass"] = None
                if row["pass"] is False:
                    results["passed"] = False
            results["tables"][core].append(row)
    return results


# --- NEWS core: date-period comparison (CREATED_ON / DATETIME) ---

def _news_date_range_yyyymmddhhmmss(months):
    """Return (start_str, end_str) in YYYYMMDDHHmmss for last N months (UTC). For DATETIME field like 20250108161604."""
    now = datetime.now(timezone.utc)
    end_str = now.strftime("%Y%m%d%H%M%S")
    year, month = now.year, now.month
    month -= months
    while month <= 0:
        month += 12
        year -= 1
    # First day of that month (avoids day overflow)
    start_dt = datetime(year, month, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
    start_str = start_dt.strftime("%Y%m%d%H%M%S")
    return start_str, end_str


def run_tc_news_created_on(tc003_result=None):
    """NEWS core: compare document count where date field is within the last N months.
    Supports CREATED_ON (Solr date math) or DATETIME with format YYYYMMDDHHmmss (e.g. 20250108161604).
    When NEWS_DATE_FORMAT is YYYYMMDDHHmmss, also returns exchange-wise counts in the date period."""
    results = {"passed": True, "core": "NEWS", "prod_count": None, "stage_count": None,
               "variance_pct": None, "pass": None, "period_months": None, "field": None, "errors": [],
               "exchange_rows": []}
    if "NEWS" not in REQUIRED_CORES:
        return results
    field = getattr(sys.modules[__name__], "NEWS_DATE_FIELD", "CREATED_ON")
    months = getattr(sys.modules[__name__], "NEWS_DATE_PERIOD_MONTHS", 3)
    date_fmt = getattr(sys.modules[__name__], "NEWS_DATE_FORMAT", None) or ""
    results["field"] = field
    results["period_months"] = months
    use_string_range = (date_fmt.upper() == "YYYYMMDDHHMMSS")
    if use_string_range:
        start_str, end_str = _news_date_range_yyyymmddhhmmss(months)
        fq_val = quote(f"{field}:[{start_str} TO {end_str}]")
    else:
        fq_val = quote(f"{field}:[NOW-{months}MONTHS TO *]")
    for env_name, base_url in [("Production", PRODUCTION_SOLR_URL), ("Staging", STAGING_SOLR_URL)]:
        url = f"{base_url}/solr/NEWS/select?q=*:*&fq={fq_val}&rows=0&wt=json"
        status, data, err = solr_get(url)
        if err or status != 200:
            results["errors"].append(f"NEWS ({env_name}): {err or status}")
            continue
        n = (data.get("response") or {}).get("numFound")
        if env_name == "Production":
            results["prod_count"] = n
        else:
            results["stage_count"] = n
    # Exchange-wise count in date period (when DATETIME string format and we have exchange list)
    if use_string_range and tc003_result and results["errors"] == []:
        exchange_list = (tc003_result.get("all_exchanges_per_core") or {}).get("NEWS") or []
        ex_field = (tc003_result.get("field_per_core") or {}).get("NEWS") or EXCHANGE_FIELD
        for ex in exchange_list:
            q_ex = quote(f'{ex_field}:"{ex}"')
            row = {"exchange": ex, "prod_count": 0, "stage_count": 0, "variance_pct": None, "pass": None}
            for env_name, base_url in [("Production", PRODUCTION_SOLR_URL), ("Staging", STAGING_SOLR_URL)]:
                url = f"{base_url}/solr/NEWS/select?q={q_ex}&fq={fq_val}&rows=0&wt=json"
                status, data, err = solr_get(url)
                if status == 200 and data:
                    n = (data.get("response") or {}).get("numFound")
                    if env_name == "Production":
                        row["prod_count"] = n
                    else:
                        row["stage_count"] = n
            prod, stage = row["prod_count"], row["stage_count"]
            if prod is not None and stage is not None:
                if prod > 0:
                    row["variance_pct"] = round((prod - stage) / prod * 100, 2)
                    row["pass"] = _variance_pass(row["variance_pct"], VARIANCE_THRESHOLD_PERCENT)
                else:
                    row["variance_pct"] = VARIANCE_UNDEFINED
                    row["pass"] = None
                if row["pass"] is False:
                    results["passed"] = False
            results["exchange_rows"].append(row)
    prod, stage = results["prod_count"], results["stage_count"]
    if prod is not None and stage is not None and prod > 0:
        results["variance_pct"] = round((prod - stage) / prod * 100, 2)
        results["pass"] = _variance_pass(results["variance_pct"], VARIANCE_THRESHOLD_PERCENT)
        if results["pass"] is False:
            results["passed"] = False
    elif prod is not None and stage is not None:
        # prod=0: variance undefined (division by zero); set -1
        results["variance_pct"] = VARIANCE_UNDEFINED
        results["pass"] = None
    return results


# --- Test Suite 3: Data Freshness ---

def _parse_solr_ts(ts_str):
    """Parse Solr/ISO timestamp to datetime (stdlib only)."""
    if not ts_str:
        return None
    s = ts_str.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s.replace("+00:00", "").rstrip(), fmt)
        except ValueError:
            continue
    return None


def run_tc005_latest_timestamp():
    """TC-005: Latest update timestamp per core (Prod vs Staging)."""
    results = {"passed": True, "rows": [], "errors": []}
    for core in REQUIRED_CORES:
        row = {"core": core, "prod_latest": None, "stage_latest": None, "diff_hours": None, "pass": None}
        fl = quote("LAST_UPDATED_ON")
        sort = quote("LAST_UPDATED_ON desc")
        for env_name, base_url in [("Production", PRODUCTION_SOLR_URL), ("Staging", STAGING_SOLR_URL)]:
            url = f"{base_url}/solr/{core}/select?q=*:*&sort={sort}&rows=1&fl={fl}&wt=json"
            status, data, err = solr_get(url)
            if err or status != 200:
                results["errors"].append(f"{core} ({env_name}): {err or status}")
                continue
            docs = (data.get("response") or {}).get("docs") or []
            ts = docs[0].get("LAST_UPDATED_ON") if docs else None
            if env_name == "Production":
                row["prod_latest"] = ts
            else:
                row["stage_latest"] = ts
        if row["prod_latest"] and row["stage_latest"]:
            try:
                t1 = _parse_solr_ts(str(row["prod_latest"]))
                t2 = _parse_solr_ts(str(row["stage_latest"]))
                if t1 and t2:
                    # Normalize to naive UTC for subtraction
                    if getattr(t1, "tzinfo", None):
                        t1 = t1.astimezone(timezone.utc).replace(tzinfo=None)
                    if getattr(t2, "tzinfo", None):
                        t2 = t2.astimezone(timezone.utc).replace(tzinfo=None)
                    # Absolute gap for display; pass uses "staging behind prod" hours, not symmetric abs
                    gap = abs((t2 - t1).total_seconds()) / 3600
                    row["diff_hours"] = round(gap, 2)
                    behind = (t1 - t2).total_seconds() / 3600  # >0 = staging older than prod
                    if TC005_PASS_WHEN_STAGING_AHEAD and behind <= 0:
                        row["pass"] = True
                    else:
                        row["pass"] = behind <= MAX_TIME_DIFF_HOURS
                    if not row["pass"]:
                        results["passed"] = False
            except Exception:
                row["diff_hours"] = None
                row["pass"] = None
        results["rows"].append(row)
    return results


def run_tc006_schedule_verification(tc005_result):
    """TC-006: Schedule-based data update verification (uses TC-005 data + schedule context)."""
    # Schedule type hints from Readme (optional; actual config would come from application.properties)
    schedule_hints = {
        "EARNINGS": "Daily 5 AM",
        "NEWS_SENTIMENT": "Every 3 hours",
        "COMPANY_PROFILE": "Weekdays 6 AM",
        "EARNINGS_HISTORY": "Weekly (Friday)",
        "FINANCIAL_RATIOS": "Weekdays 7 AM",
        "ANALYST_VIEW": "Daily 4 AM",
        "PERFORMANCE_DATA": "Daily 3 AM",
        "TREND_DATA": "Daily 12 PM",
        "TECH": "Weekdays 4 AM & 2 PM",
        "PEERS": "Weekdays 9 AM",
        "NEWS": "Hourly/Every 3h",
        "CONSOLIDATED_SCORE": "Continuous (1h delay)",
    }
    results = {"passed": tc005_result.get("passed", True), "rows": []}
    for row in tc005_result.get("rows", []):
        r = dict(row)
        r["schedule_type"] = schedule_hints.get(r["core"], "—")
        results["rows"].append(r)
    return results


# --- Test Suite 3 (continued): Period-based freshness & nohup extraction times ---

def run_tc005b_freshness_by_period():
    """Freshness by document write count in closed periods ending in the past.
    Uses a window ending FRESHNESS_PERIOD_END_HOURS_AGO ago so in-progress runs
    (e.g. staging running while prod idle, or vice versa) don't skew counts."""
    results = {"passed": True, "periods": [], "errors": []}
    periods = getattr(sys.modules[__name__], "FRESHNESS_PERIODS_HOURS", [6, 24, 72])
    end_ago = getattr(sys.modules[__name__], "FRESHNESS_PERIOD_END_HOURS_AGO", 1)
    threshold = getattr(sys.modules[__name__], "WRITE_COUNT_VARIANCE_THRESHOLD_PERCENT", 10.0)
    for hours in periods:
        period_rows = []
        # Closed window ending 'end_ago' hours ago: [NOW-(hours+end_ago) TO NOW-end_ago]
        start_offset = hours + end_ago
        q = quote(f"LAST_UPDATED_ON:[NOW-{start_offset}HOUR TO NOW-{end_ago}HOUR]")
        core_map = getattr(sys.modules[__name__], "PERIOD_COUNT_CORE_MAP", {})
        for core in REQUIRED_CORES:
            # Use mapped core name for URL if configured (e.g. FINANCIAL_RATIOS_ALL)
            solr_core = core_map.get(core, core)
            row = {"core": core, "prod_count": None, "stage_count": None, "variance_pct": None, "pass": None, "error": None}
            err_parts = []
            for env_name, base_url in [("Production", PRODUCTION_SOLR_URL), ("Staging", STAGING_SOLR_URL)]:
                url = f"{base_url}/solr/{solr_core}/select?q={q}&rows=0&wt=json"
                status, data, err = solr_get(url)
                if err or status != 200:
                    results["errors"].append(f"{core} {hours}h ({env_name}): {err or status}")
                    err_parts.append(f"{env_name[:4]}: {status or err}")
                    continue
                n = (data.get("response") or {}).get("numFound")
                if env_name == "Production":
                    row["prod_count"] = n
                else:
                    row["stage_count"] = n
            if err_parts:
                row["error"] = "; ".join(err_parts)
            prod, stage = row["prod_count"], row["stage_count"]
            if prod is not None and stage is not None:
                if prod > 0:
                    # Signed variance (negative accepted); pass when abs(variance) <= threshold
                    row["variance_pct"] = round((prod - stage) / prod * 100, 2)
                    row["pass"] = _variance_pass(row["variance_pct"], threshold)
                else:
                    # prod=0: variance undefined (division by zero); set -1
                    row["variance_pct"] = VARIANCE_UNDEFINED
                    row["pass"] = None
                if row["pass"] is False:
                    results["passed"] = False
            period_rows.append(row)
        results["periods"].append({
            "hours": hours,
            "end_hours_ago": end_ago,
            "rows": period_rows,
        })
    return results


# Log task class name -> Solr core (extraction tasks only; SolrInitializer "synced" is not tracked)
TASK_CLASS_TO_CORE = {
    "BenzingaNewsExtractionTask": "NEWS",
    "CompanyNewsExtractionTask": "NEWS",
    "MarketNewsExtractionTask": "NEWS",
    "DbNewsExtractorTask": "NEWS",
    "NewsExtractionTask": "NEWS",
    "NewsSentimentExtractionTask": "NEWS_SENTIMENT",
    "NewsSentimentHistoryTask": "NEWS_SENTIMENT",
    "EarningsExtractionTask": "EARNINGS",
    "EarningsHistoryTask": "EARNINGS_HISTORY",
    "ActualEarningsHistoryTask": "ACTUAL_EARNINGS_HISTORY",
    "AnalystViewExtractionTask": "ANALYST_VIEW",
    "AnalystViewHistoryTask": "ANALYST_VIEW",
    "CompanyProfileExtractionTask": "COMPANY_PROFILE",
    "CompanyProfileExtractionWithAllImagesTask": "COMPANY_PROFILE_IMAGE",
    "CompanyProfileImageUploaderTask": "COMPANY_PROFILE_IMAGE",
    "CalcFinancialRatiosSolrWriter": "FINANCIAL_RATIOS",
    "FHFinancialRatiosSolrWriter": "FH_FINANCIAL_RATIOS",
    "PerformanceDataExtractionTask": "PERFORMANCE_DATA",
    "TrendDataExtractionTask": "TREND_DATA",
    "TechDataExtractorTask": "TECH",
    "SupportAndResistanceExtractionTask": "SUPPORT_RESISTANCE",
    "SupportAndResistance15And30ExtractionTask": "SUPPORT_RESISTANCE",
    "PeersExtractionTask": "PEERS",
    "ConsolidatedScoreExtractionTask": "CONSOLIDATED_SCORE",
    "ETFExtractionTask": "ETF",
}
# SolrInitializer logs "synced CORE_ALL" -> core name
RE_SYNCED = re.compile(r"synced\s+(\w+)_ALL", re.I)

# Per-task start patterns: only count a run start when the log line matches this task's start phrase.
# Ensures extraction is exactly captured (start/end pair for the same task).
TASK_START_PATTERNS = {
    "BenzingaNewsExtractionTask": ("started from", "Extracting ", "Data Extraction Started", "Loading "),
    "CompanyNewsExtractionTask": ("Extracting Company News Data", "started from", "Data extraction started"),
    "MarketNewsExtractionTask": ("Extracting ", "started from", "Data Extraction Started"),
    "DbNewsExtractorTask": ("started from", "Loading ", "Data extraction started"),
    "NewsExtractionTask": ("Extracting ", "started from", "Data Extraction Started", "Loading "),
    "NewsSentimentExtractionTask": ("started from", "Extracting ", "Data extraction started"),
    "NewsSentimentHistoryTask": ("started from", "Extracting ", "Data extraction started"),
    "EarningsExtractionTask": ("started from", "Extracting ", "Delta-import", "Loading "),
    "EarningsHistoryTask": ("started from", "Extracting ", "Delta-import", "Loading "),
    "ActualEarningsHistoryTask": ("started from", "Extracting ", "Actual Earnings", "Loading "),
    "AnalystViewExtractionTask": ("started from", "Extracting ", "Analyst View", "Loading "),
    "AnalystViewHistoryTask": ("started from", "Extracting ", "Analyst View", "Loading "),
    "CompanyProfileExtractionTask": ("Extracting Company Profile Data", "started from", "Loading "),
    "CompanyProfileExtractionWithAllImagesTask": ("Extracting Company Profile", "started from", "Loading "),
    "CompanyProfileImageUploaderTask": ("Image Download", "started from", "Loading "),
    "CalcFinancialRatiosSolrWriter": ("started from", "Loading ", "Delta-import", "Batch "),
    "FHFinancialRatiosSolrWriter": ("started from", "Loading ", "Delta-import", "Batch "),
    "PerformanceDataExtractionTask": ("Extracting Performance Data", "started from", "Data extraction started"),
    "TrendDataExtractionTask": ("Extracting Trend Data", "started from", "Data extraction started"),
    "TechDataExtractorTask": ("Extracting Tech Data", "started from", "Data extraction started"),
    "SupportAndResistanceExtractionTask": ("Extracting Support and Resistance Data", "started from", "Data extraction started"),
    "SupportAndResistance15And30ExtractionTask": ("Extracting Support and Resistance 15m and 30m Data", "started from", "Data extraction started"),
    "PeersExtractionTask": ("Extracting Peers Data", "started from", "Data extraction started"),
    "ConsolidatedScoreExtractionTask": ("Extracting Consolidated Score Data", "started from", "Data extraction started"),
    "ETFExtractionTask": ("Extracting ETF Data", "started from", "Data extraction started"),
}
# Generic fallback when task is not in TASK_START_PATTERNS (e.g. unknown task class)
DEFAULT_START_PATTERNS = (
    "started from", "started from:", "Extracting data for", "Data Extraction Started",
    "Data extraction started", "Loading ", "Started ", "started ", "Delta-import",
    "Extracting Company News Data", "Extracting ETF Data", "Extracting Performance Data",
    "Extracting Trend Data", "Extracting Tech Data", "Extracting Support and Resistance Data",
    "Extracting Support and Resistance 15m and 30m Data", "Extracting Peers Data",
    "Extracting Consolidated Score Data",
)

# Per-task end patterns: only count completion when the log line matches this task's completion phrase.
TASK_END_PATTERNS = {
    "BenzingaNewsExtractionTask": ("Batch completed", "Extraction completed", "Completed Extracting", "Task Completed exiting", "completed"),
    "CompanyNewsExtractionTask": ("Batch completed", "Extraction completed", "Completed Extracting", "Extracting Company News Data completed", "Task Completed exiting"),
    "MarketNewsExtractionTask": ("Batch completed", "Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "DbNewsExtractorTask": ("Batch completed", "Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "NewsExtractionTask": ("Batch completed", "Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "NewsSentimentExtractionTask": ("Batch completed", "Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "NewsSentimentHistoryTask": ("Batch completed", "Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "EarningsExtractionTask": ("Batch completed", "Extraction completed", "Completed Extracting", "Completed writing", "Task Completed exiting"),
    "EarningsHistoryTask": ("Batch completed", "Extraction completed", "Completed Extracting", "Completed writing", "Task Completed exiting"),
    "ActualEarningsHistoryTask": ("Batch completed", "Actual Earnings History", "Extraction completed", "Task Completed exiting"),
    "AnalystViewExtractionTask": ("Batch completed", "Analyst View Data", "Extraction completed", "Task Completed exiting"),
    "AnalystViewHistoryTask": ("Batch completed", "Analyst View Data", "Extraction completed", "Task Completed exiting"),
    "CompanyProfileExtractionTask": ("Extracting Company Profile Data completed", "All Company Profile Data completed", "Extraction completed", "Task Completed exiting"),
    "CompanyProfileExtractionWithAllImagesTask": ("Extracting Company Profile Data completed", "All Company Profile Data completed", "Image Download Completed", "Task Completed exiting"),
    "CompanyProfileImageUploaderTask": ("Image Download Completed", "Extraction completed", "Task Completed exiting"),
    "CalcFinancialRatiosSolrWriter": ("Batch completed", "reportDataCaching", "processing completed", "All processing completed"),
    "FHFinancialRatiosSolrWriter": ("Batch completed", "reportDataCaching", "processing completed", "All processing completed"),
    "PerformanceDataExtractionTask": ("performance data", "Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "TrendDataExtractionTask": ("Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "TechDataExtractorTask": ("Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "SupportAndResistanceExtractionTask": ("Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "SupportAndResistance15And30ExtractionTask": ("Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "PeersExtractionTask": ("Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "ConsolidatedScoreExtractionTask": ("consolidated data", "Extraction completed", "Completed Extracting", "Task Completed exiting"),
    "ETFExtractionTask": ("Extraction completed", "Completed Extracting", "Task Completed exiting"),
}
DEFAULT_END_PATTERNS = (
    "Batch completed", "reportDataCaching", "Extraction is completed", "Extraction completed",
    "All processing completed", "All extraction threads completed", "Task Completed exiting",
    "Completed Extracting", "Completed extraction", "Completed writing",
    "Extracting Company Profile Data completed", "Data completed", "All Company Profile Data completed",
    "consolidated data", "performance data", "Analyst View Data", "Actual Earnings History",
    "Image Download Completed", "extraction completed", "processing completed",
)


def _parse_nohup_ts(ts_str):
    """Parse nohup timestamp e.g. 2025-10-21T10:48:18,970 to datetime for duration calc."""
    if not ts_str:
        return None
    s = ts_str.strip().replace(",", ".")  # nohup uses comma for ms
    try:
        return datetime.strptime(s[:23], "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        try:
            return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None


def _duration_str(start_ts, end_ts):
    """Return (time_range_str, duration_str) e.g. ('10:48:09 – 11:15:23', '27 min')."""
    if not start_ts or not end_ts:
        return "—", "—"
    t1 = _parse_nohup_ts(start_ts)
    t2 = _parse_nohup_ts(end_ts)
    if not t1 or not t2:
        return start_ts + " – " + end_ts, "—"
    sec = (t2 - t1).total_seconds()
    if sec < 0:
        sec = -sec
    if sec >= 60:
        dur = f"{int(round(sec / 60))} min"
    else:
        dur = f"{int(round(sec))} s"
    time_range = t1.strftime("%H:%M:%S") + " – " + t2.strftime("%H:%M:%S")
    return time_range, dur


def _parse_nohup_line(line):
    """Parse one nohup log line. Returns (timestamp_str, short_class, full_class, message, thread) or None.
    Thread is used so overlapping runs of the same task on different threads are tracked separately."""
    stripped = re.sub(r"\x1b\[[0-9;]*m", "", line)
    ts_match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2},\d{3})", stripped)
    if not ts_match:
        return None
    ts = ts_match.group(1)
    rest = stripped[ts_match.end() :].strip()
    # Thread: [thread] before the logger class (e.g. [main], [scheduled-task-pool-2])
    thread_match = re.search(r"\s+\[([^\]]+)\]\s+[A-Za-z0-9_.]+:", rest)
    thread = thread_match.group(1).strip() if thread_match else ""
    class_match = re.search(r"\]\s+([A-Za-z0-9_.]+)\s*:", rest)
    if not class_match:
        return None
    full_class = class_match.group(1)
    message = rest[class_match.end() :].strip()
    short_class = full_class.split(".")[-1] if "." in full_class else full_class
    return (ts, short_class, full_class, message, thread)


def parse_nohup_log(log_path, max_lines=None):
    """Stream nohup log and extract job extraction start/end times per task and thread.
    Tracks (task, thread) so overlapping runs (e.g. next schedule starts on another thread
    while previous run still going) are counted separately."""
    result = {}  # (task_key, thread) -> list of { start, end, core }
    runs = {}    # (task_key, thread) -> current run { start, end, core }
    path = log_path or getattr(sys.modules[__name__], "NOHUP_LOG_PATH", None)
    if not path or not os.path.isfile(path):
        return {"tasks": [], "errors": ["NOHUP_LOG_PATH not set or file not found"], "path": path}
    line_count = 0
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line_count += 1
                if max_lines and line_count > max_lines:
                    break
                # Only extraction task logs (exclude SolrInitializer / "synced")
                if "com.mubasher.finhub.tasks" not in line or "SolrInitializer" in line:
                    continue
                parsed = _parse_nohup_line(line)
                if not parsed:
                    continue
                ts, short_class, full_class, message, thread = parsed
                core = TASK_CLASS_TO_CORE.get(short_class)
                if not core:
                    if "com.mubasher.finhub.tasks." in full_class:
                        core = short_class
                    else:
                        continue
                task_key = short_class
                run_key = (task_key, thread)
                if run_key not in runs:
                    runs[run_key] = {"start": None, "end": None, "core": core or short_class, "start_pattern": None, "start_log": None}
                # Per-task start/end patterns so we track exact logs for each job's start and end
                start_patterns_for_task = TASK_START_PATTERNS.get(short_class, DEFAULT_START_PATTERNS)
                end_patterns_for_task = TASK_END_PATTERNS.get(short_class, DEFAULT_END_PATTERNS)
                start_matched = next((p for p in start_patterns_for_task if p in message), None)
                end_pattern_matched = next((p for p in end_patterns_for_task if p in message), None)
                log_snippet = (message[:140] + "…") if len(message) > 140 else message
                # If same line matches both start and end, treat as end only (avoid 0s run / same log for both)
                if start_matched and not end_pattern_matched:
                    runs[run_key]["start"] = ts
                    runs[run_key]["end"] = None
                    runs[run_key]["start_pattern"] = start_matched
                    runs[run_key]["start_log"] = log_snippet
                if end_pattern_matched:
                    runs[run_key]["end"] = ts
                    if runs[run_key]["start"] is not None:
                        if run_key not in result:
                            result[run_key] = []
                        result[run_key].append({
                            "start": runs[run_key]["start"],
                            "end": runs[run_key]["end"],
                            "core": runs[run_key]["core"],
                            "start_pattern": runs[run_key].get("start_pattern"),
                            "end_pattern": end_pattern_matched,
                            "start_log": runs[run_key].get("start_log"),
                            "end_log": log_snippet,
                        })
                        runs[run_key]["start"] = None
                        runs[run_key]["start_pattern"] = None
                        runs[run_key]["start_log"] = None
    except Exception as e:
        return {"tasks": [], "errors": [str(e)], "path": path, "lines_read": line_count}
    task_list = []
    for (task_name, thread_name), run_list in result.items():
        if not run_list:
            continue
        last = run_list[-1]
        start_ts = last.get("start")
        end_ts = last.get("end")
        time_range, duration = _duration_str(start_ts, end_ts)
        task_list.append({
            "task": task_name,
            "thread": thread_name or "—",
            "core": last.get("core", task_name),
            "last_start": start_ts,
            "last_end": end_ts,
            "time_range": time_range,
            "duration": duration,
            "run_count": len(run_list),
            "start_pattern": last.get("start_pattern"),
            "end_pattern": last.get("end_pattern"),
            "start_log": last.get("start_log"),
            "end_log": last.get("end_log"),
        })
    task_list.sort(key=lambda x: (x.get("core") or "", x.get("task") or "", x.get("thread") or ""))
    return {"tasks": task_list, "errors": [], "path": path, "lines_read": line_count}


# --- Report Generation ---

def build_logs_report(nohup_result, output_path):
    """Generate HTML report for logs-only mode (job extraction times from nohup)."""
    run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    path = nohup_result.get("path") or "—"
    lines_read = nohup_result.get("lines_read", 0)
    tasks = nohup_result.get("tasks", [])
    errors = nohup_result.get("errors", [])

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FH Verification – Logs Report</title>
<style>
body {{ font-family: Segoe UI, system-ui, sans-serif; margin: 24px; background: #f5f5f5; }}
h1 {{ color: #1a1a2e; border-bottom: 2px solid #16213e; padding-bottom: 8px; }}
h2 {{ color: #16213e; margin-top: 24px; }}
table {{ border-collapse: collapse; width: 100%; margin: 12px 0; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: left; }}
th {{ background: #16213e; color: #fff; }}
tr:nth-child(even) {{ background: #f9f9f9; }}
.fail {{ color: #c23030; font-weight: bold; }}
.footer {{ margin-top: 24px; color: #666; font-size: 14px; }}
</style>
</head>
<body>
<h1>FH Verification – Logs Report</h1>
<p><strong>Run time:</strong> {run_time}</p>
<p><strong>Mode:</strong> Logs only (job extraction times from nohup).</p>
<p><strong>Log file:</strong> {path}</p>
<p><strong>Lines read:</strong> {lines_read:,}</p>
"""
    if errors:
        html += f"<p class='fail'>Errors: {'; '.join(errors)}</p>\n"
    html += """
<h2>Job extraction times (by task and thread)</h2>
<p>Every job's start and end times are tracked using <strong>exact log lines</strong>. The table shows the log snippet that marked <strong>start</strong> and <strong>end</strong> for each job (per task and thread).</p>
"""
    if tasks:
        html += "<table>\n<thead><tr><th>Task / Core</th><th>Thread</th><th>Start pattern</th><th>End pattern</th><th>Start log (exact)</th><th>End log (exact)</th><th>Last run start</th><th>Last run end</th><th>Time range</th><th>Duration</th><th>Runs seen</th></tr></thead>\n<tbody>\n"
        for t in tasks:
            sl = t.get("start_log")
            el = t.get("end_log")
            start_log = html_lib.escape(str(sl)) if sl else "—"
            end_log = html_lib.escape(str(el)) if el else "—"
            html += f"<tr><td>{t.get('task')} / {t.get('core')}</td><td>{t.get('thread') or '—'}</td><td>{t.get('start_pattern') or '—'}</td><td>{t.get('end_pattern') or '—'}</td><td><code style='font-size:11px;word-break:break-word;'>{start_log}</code></td><td><code style='font-size:11px;word-break:break-word;'>{end_log}</code></td><td>{t.get('last_start') or '—'}</td><td>{t.get('last_end') or '—'}</td><td>{t.get('time_range') or '—'}</td><td>{t.get('duration') or '—'}</td><td>{t.get('run_count', 0)}</td></tr>\n"
        html += "</tbody></table>\n"
    else:
        html += "<p>No task runs found. Check that NOHUP_LOG_PATH points to a finhub-extractor nohup log and that the log contains extraction task lines (start/end patterns).</p>\n"
    html += f"""
<div class="footer">Generated by FH Verification script (mode=logs).</div>
</body>
</html>
"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    return output_path


def build_html_report(tc002, tc003, tc004, tc005, tc006, tc005b, nohup_result, output_path, run_mode="both", tc_news_date=None):
    """Generate HTML report. run_mode: 'solr' (Solr only), 'both' (Solr + logs). Core availability (admin API) omitted; health check only."""
    run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    def tr(s):
        return "PASS" if s else "FAIL"
    def tr_class(p):
        """Return lowercase class for pass/fail so CSS .pass / .fail apply (red for fail)."""
        if p is None:
            return ""
        return "pass" if p else "fail"

    # Summary (Suite 1 = health check only; no admin/core-existence check)
    suite1_pass = tc002.get("passed", False)
    news_pass = (tc_news_date or {}).get("passed", True)
    suite2_pass = tc003.get("passed", True) and tc004.get("passed", True) and news_pass
    suite3_pass = (tc005.get("passed", True) and tc006.get("passed", True) and
                   (tc005b.get("passed", True) if tc005b else True))
    overall = suite1_pass and suite2_pass and suite3_pass
    title = "FHE Staging vs Production Solr – Verification Report" if run_mode == "both" else "FHE Staging vs Production Solr – Verification Report (Solr only)"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
body {{ font-family: Segoe UI, system-ui, sans-serif; margin: 24px; background: #f5f5f5; }}
h1 {{ color: #1a1a2e; border-bottom: 2px solid #16213e; padding-bottom: 8px; }}
h2 {{ color: #16213e; margin-top: 24px; }}
h3 {{ color: #0f3460; }}
table {{ border-collapse: collapse; width: 100%; margin: 12px 0; background: #fff; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: left; }}
th {{ background: #16213e; color: #fff; }}
tr:nth-child(even) {{ background: #f9f9f9; }}
.pass {{ color: #0d8050; font-weight: bold; }}
.fail {{ color: #c23030; font-weight: bold; }}
td.fail {{ color: #c23030; font-weight: bold; }}
tr.fail {{ background: #ffebee; }}
.summary {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 16px 0; }}
.summary .card {{ background: #fff; padding: 16px 24px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
.summary .card.pass {{ border-left: 4px solid #0d8050; }}
.summary .card.fail {{ border-left: 4px solid #c23030; }}
pre {{ background: #eee; padding: 12px; overflow-x: auto; font-size: 13px; }}
.footer {{ margin-top: 24px; color: #666; font-size: 14px; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p><strong>Run time:</strong> {run_time}</p>
<p><strong>Production:</strong> {PRODUCTION_SOLR_URL} | <strong>Staging:</strong> {STAGING_SOLR_URL}</p>

<div class="summary">
<div class="card {'pass' if overall else 'fail'}"><strong>Overall</strong>: <span class="{tr(overall).lower()}">{tr(overall)}</span></div>
<div class="card {'pass' if suite1_pass else 'fail'}">Suite 1 (Health)</div>
<div class="card {'pass' if suite2_pass else 'fail'}">Suite 2 (Exchange)</div>
<div class="card {'pass' if suite3_pass else 'fail'}">Suite 3 (Freshness)</div>
</div>

<h2>Test Suite 1: Core Health Check</h2>
<p>Core availability (admin API) is skipped because Solr often blocks admin requests.</p>

<h3>TC-002: Core Health Check</h3>
<table>
<thead><tr><th>Core</th><th>Environment</th><th>Status</th><th>Healthy</th><th>numFound</th></tr></thead>
<tbody>
"""
    for c in tc002.get("checks", []):
        healthy = c.get("healthy")
        cl = tr_class(healthy)
        html += f"<tr><td>{c.get('core')}</td><td>{c.get('environment')}</td><td>{c.get('status_code') or '—'}</td><td class=\"{cl}\">{tr(healthy)}</td><td>{c.get('numFound') if c.get('numFound') is not None else '—'}</td></tr>\n"
    html += "</tbody></table>\n"

    html += """
<h2>Test Suite 2: Exchange-Wise Data Coverage</h2>

<h3>TC-003: Exchange Coverage</h3>
"""
    html += f"<p><strong>Common exchanges:</strong> {', '.join(tc003.get('common', [])) or '—'}</p>\n"
    html += f"<p><strong>Production only:</strong> {', '.join(tc003.get('prod_only', [])) or '—'}</p>\n"
    html += f"<p><strong>Staging only:</strong> {', '.join(tc003.get('staging_only', [])) or '—'}</p>\n"
    if tc003.get("errors"):
        html += f"<p class='fail'>Errors: {'; '.join(tc003['errors'])}</p>\n"

    html += "<h3>TC-004: Exchange-Wise Document Count (all cores, all exchanges)</h3>\n"
    html += "<p>Each core lists every exchange present in either Production or Staging; missing in one env is shown as —.</p>\n"
    for core in _exchange_coverage_cores():
        rows = tc004.get("tables", {}).get(core, [])
        html += f"<h4>{core}</h4>\n"
        if not rows:
            html += "<p>No exchange data for this core (facet empty or core not queried).</p>\n"
            continue
        html += "<table>\n<thead><tr><th>Exchange</th><th>Prod Count</th><th>Stage Count</th><th>Variance %</th><th>Pass</th></tr></thead>\n<tbody>\n"
        for r in rows:
            v = r.get("variance_pct")
            p = r.get("pass")
            v_str = "—" if v is None or v == VARIANCE_UNDEFINED else str(v)
            p_str = tr(p) if p is not None else "—"
            pc = r.get("prod_count")
            sc = r.get("stage_count")
            pc_str = pc if pc == "—" or pc is None else str(pc)
            sc_str = sc if sc == "—" or sc is None else str(sc)
            if pc is None:
                pc_str = "—"
            if sc is None:
                sc_str = "—"
            row_cl = "fail" if p is False else ""
            cell_cl = tr_class(p) if p is not None else ""
            html += f"<tr class=\"{row_cl}\"><td>{r.get('exchange')}</td><td>{pc_str}</td><td>{sc_str}</td><td>{v_str}</td><td class=\"{cell_cl}\">{p_str}</td></tr>\n"
        html += "</tbody></table>\n"

    # NEWS core: CREATED_ON date-period comparison (configurable months)
    if tc_news_date and tc_news_date.get("field"):
        html += "<h3>NEWS core: Date-period comparison (" + html_lib.escape(str(tc_news_date.get("field", "CREATED_ON"))) + ")</h3>\n"
        html += f"<p>Documents with <strong>{tc_news_date.get('field', 'CREATED_ON')}</strong> in the last <strong>{tc_news_date.get('period_months', 3)} months</strong>. Variance can be negative (staging has fewer).</p>\n"
        if tc_news_date.get("errors"):
            html += f"<p class='fail'>Errors: {'; '.join(tc_news_date['errors'])}</p>\n"
        html += "<table>\n<thead><tr><th>Core</th><th>Field</th><th>Period</th><th>Prod count</th><th>Stage count</th><th>Variance %</th><th>Pass</th></tr></thead>\n<tbody>\n"
        nd = tc_news_date
        p_nd = nd.get("pass")
        row_cl = "fail" if p_nd is False else ""
        cell_cl = tr_class(p_nd) if p_nd is not None else ""
        nd_var = nd.get("variance_pct")
        nd_var_str = "—" if nd_var is None or nd_var == VARIANCE_UNDEFINED else str(nd_var)
        html += f"<tr class=\"{row_cl}\"><td>{nd.get('core', 'NEWS')}</td><td>{nd.get('field', '—')}</td><td>Last {nd.get('period_months', '—')} months</td><td>{nd.get('prod_count') if nd.get('prod_count') is not None else '—'}</td><td>{nd.get('stage_count') if nd.get('stage_count') is not None else '—'}</td><td>{nd_var_str}</td><td class=\"{cell_cl}\">{tr(p_nd) if p_nd is not None else '—'}</td></tr>\n"
        html += "</tbody></table>\n"
        # Exchange-wise count in date period (when DATETIME YYYYMMDDHHmmss format)
        if tc_news_date.get("exchange_rows"):
            html += "<h4>NEWS exchange-wise count (same date period)</h4>\n"
            html += "<table>\n<thead><tr><th>Exchange</th><th>Prod count</th><th>Stage count</th><th>Variance %</th><th>Pass</th></tr></thead>\n<tbody>\n"
            for r in tc_news_date["exchange_rows"]:
                v = r.get("variance_pct")
                p = r.get("pass")
                v_str = "—" if v is None or v == VARIANCE_UNDEFINED else str(v)
                row_cl = "fail" if p is False else ""
                cell_cl = tr_class(p) if p is not None else ""
                html += f"<tr class=\"{row_cl}\"><td>{r.get('exchange')}</td><td>{r.get('prod_count')}</td><td>{r.get('stage_count')}</td><td>{v_str}</td><td class=\"{cell_cl}\">{tr(p) if p is not None else '—'}</td></tr>\n"
            html += "</tbody></table>\n"

    html += """
<h2>Test Suite 3: Data Freshness</h2>

<h3>TC-005b: Period-based write count (recommended)</h3>
<p>Uses a closed window ending in the past so in-progress runs (e.g. staging running while prod idle) don't skew counts.</p>
<p>All cores from config are listed; if count is "—" the query failed (see <strong>Error</strong> column, e.g. 404 = core not found, 400 = field LAST_UPDATED_ON missing).</p>
"""
    if tc005b and tc005b.get("periods"):
        for period in tc005b.get("periods", []):
            h = period.get("hours", 0)
            end_ago = period.get("end_hours_ago", 0)
            window_label = f"Last {h}h (ended {end_ago}h ago)" if end_ago else f"Last {h}h"
            html += f"<h4>{window_label}</h4>\n<table>\n<thead><tr><th>Core</th><th>Prod count</th><th>Stage count</th><th>Variance %</th><th>Pass</th><th>Error</th></tr></thead>\n<tbody>\n"
            for r in period.get("rows", []):
                p = r.get("pass")
                err = r.get("error") or "—"
                row_cl = "fail" if p is False else ""
                cell_cl = tr_class(p) if p is not None else ""
                r_var = r.get("variance_pct")
                r_var_str = "—" if r_var is None or r_var == VARIANCE_UNDEFINED else str(r_var)
                html += f"<tr class=\"{row_cl}\"><td>{r.get('core')}</td><td>{r.get('prod_count') if r.get('prod_count') is not None else '—'}</td><td>{r.get('stage_count') if r.get('stage_count') is not None else '—'}</td><td>{r_var_str}</td><td class=\"{cell_cl}\">{tr(p) if p is not None else '—'}</td><td title='{err}'>{err if err != '—' else ''}</td></tr>\n"
            html += "</tbody></table>\n"
        if tc005b.get("errors"):
            html += f"<p class='fail'>Errors: {'; '.join(tc005b['errors'])}</p>\n"
    else:
        html += "<p>No period-based data (check FRESHNESS_PERIODS_HOURS).</p>\n"

    tc005_blurb = (
        f"<p>TC-005: pass when Staging is at most <strong>{MAX_TIME_DIFF_HOURS}h</strong> behind Production "
        f"(Staging newer than Prod passes). <strong>Diff</strong> is the absolute gap in hours.</p>\n"
        if TC005_PASS_WHEN_STAGING_AHEAD
        else f"<p>TC-005: pass when absolute latest-timestamp gap ≤ <strong>{MAX_TIME_DIFF_HOURS}h</strong>.</p>\n"
    )
    html += f"""
<h3>TC-005 & TC-006: Latest update timestamp & schedule (reference)</h3>
{tc005_blurb}<table>
<thead><tr><th>Core</th><th>Schedule Type</th><th>Prod Latest</th><th>Stage Latest</th><th>Diff (hrs)</th><th>Pass</th></tr></thead>
<tbody>
"""
    for r in (tc006 or {}).get("rows", []):
        p = r.get("pass")
        row_cl = "fail" if p is False else ""
        cell_cl = tr_class(p) if p is not None else ""
        html += f"<tr class=\"{row_cl}\"><td>{r.get('core')}</td><td>{r.get('schedule_type')}</td><td>{r.get('prod_latest') or '—'}</td><td>{r.get('stage_latest') or '—'}</td><td>{r.get('diff_hours') or '—'}</td><td class=\"{cell_cl}\">{tr(p) if p is not None else '—'}</td></tr>\n"
    html += "</tbody></table>\n"

    if tc005 and tc005.get("errors"):
        html += f"<p class='fail'>Errors: {'; '.join(tc005['errors'])}</p>\n"

    if run_mode == "both":
        html += """
<h3>Job extraction times (from nohup log)</h3>
"""
        if nohup_result and nohup_result.get("tasks"):
            html += f"<p><strong>Log:</strong> {nohup_result.get('path', '—')} (lines read: {nohup_result.get('lines_read', 0):,})</p>\n"
            html += "<p>Every job's start and end times with <strong>exact log lines</strong> used to track each job. Start log / End log show the snippet that marked that run.</p>\n"
            html += "<table>\n<thead><tr><th>Task / Core</th><th>Thread</th><th>Start pattern</th><th>End pattern</th><th>Start log (exact)</th><th>End log (exact)</th><th>Last run start</th><th>Last run end</th><th>Time range</th><th>Duration</th><th>Runs seen</th></tr></thead>\n<tbody>\n"
            for t in nohup_result.get("tasks", []):
                sl = t.get("start_log")
                el = t.get("end_log")
                start_log = html_lib.escape(str(sl)) if sl else "—"
                end_log = html_lib.escape(str(el)) if el else "—"
                html += f"<tr><td>{t.get('task')} / {t.get('core')}</td><td>{t.get('thread') or '—'}</td><td>{t.get('start_pattern') or '—'}</td><td>{t.get('end_pattern') or '—'}</td><td><code style='font-size:11px;word-break:break-word;'>{start_log}</code></td><td><code style='font-size:11px;word-break:break-word;'>{end_log}</code></td><td>{t.get('last_start') or '—'}</td><td>{t.get('last_end') or '—'}</td><td>{t.get('time_range') or '—'}</td><td>{t.get('duration') or '—'}</td><td>{t.get('run_count', 0)}</td></tr>\n"
            html += "</tbody></table>\n"
        elif nohup_result and nohup_result.get("errors"):
            html += f"<p class='fail'>Nohup: {nohup_result.get('path') or 'path not set'}. Errors: {'; '.join(nohup_result['errors'])}</p>\n"
        else:
            html += "<p>Set <code>config.NOHUP_LOG_PATH</code> to the finhub-extractor nohup log file to see job extraction times (file is streamed, safe for large logs).</p>\n"

    tc005_footer = (
        f"TC-005: staging behind prod ≤ {MAX_TIME_DIFF_HOURS}h (staging ahead OK)"
        if TC005_PASS_WHEN_STAGING_AHEAD
        else f"TC-005: abs(prod - stage) <= {MAX_TIME_DIFF_HOURS}h"
    )
    html += f"""
<div class="footer">Generated by FH Verification script (mode={run_mode}). Thresholds: variance &lt;= {VARIANCE_THRESHOLD_PERCENT}%, {tc005_footer}, write-count variance &lt;= {getattr(sys.modules[__name__], 'WRITE_COUNT_VARIANCE_THRESHOLD_PERCENT', 10)}%.</div>
</body>
</html>
"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    return output_path


def main():
    out_dir = "."
    run_mode = getattr(sys.modules[__name__], "RUN_MODE", "both")
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if run_mode == "logs":
        # Mode 2: Logs report only
        nohup_path = getattr(sys.modules[__name__], "NOHUP_LOG_PATH", None)
        if not nohup_path or not os.path.isfile(nohup_path):
            print("RUN_MODE=logs requires NOHUP_LOG_PATH to point to an existing nohup log file.")
            sys.exit(1)
        report_path = f"{out_dir}/FH_Verification_Logs_Report_{ts}.html"
        print("Running FH Verification (mode=logs)...")
        print(f"  Parsing nohup log ({nohup_path})...")
        nohup_result = parse_nohup_log(nohup_path)
        print(f"    Lines read: {nohup_result.get('lines_read', 0):,}, tasks: {len(nohup_result.get('tasks', []))}")
        build_logs_report(nohup_result, report_path)
        print(f"Report written to: {report_path}")
        sys.exit(0)

    # Mode "solr" or "both": run Solr verification
    report_path = f"{out_dir}/FH_Verification_Report_{ts}.html"
    print(f"Running FH Verification (mode={run_mode})...")
    print("  TC-002: Core health check...")
    tc002 = run_tc002_core_health()
    print("  TC-003: Exchange coverage...")
    tc003 = run_tc003_exchange_coverage()
    print("  TC-004: Exchange-wise counts...")
    tc004 = run_tc004_exchange_counts(tc003)
    print("  NEWS: date-period comparison (DATETIME/CREATED_ON)...")
    tc_news_date = run_tc_news_created_on(tc003)
    print("  TC-005: Latest timestamps...")
    tc005 = run_tc005_latest_timestamp()
    print("  TC-006: Schedule verification...")
    tc006 = run_tc006_schedule_verification(tc005)
    print("  TC-005b: Period-based write counts...")
    tc005b = run_tc005b_freshness_by_period()
    nohup_result = {"tasks": [], "errors": [], "path": None}
    if run_mode == "both":
        nohup_path = getattr(sys.modules[__name__], "NOHUP_LOG_PATH", None)
        if nohup_path and os.path.isfile(nohup_path):
            print(f"  Parsing nohup log ({nohup_path})...")
            nohup_result = parse_nohup_log(nohup_path)
            print(f"    Lines read: {nohup_result.get('lines_read', 0):,}, tasks: {len(nohup_result.get('tasks', []))}")
        else:
            nohup_result["path"] = nohup_path

    build_html_report(tc002, tc003, tc004, tc005, tc006, tc005b, nohup_result, report_path, run_mode=run_mode, tc_news_date=tc_news_date)
    print(f"Report written to: {report_path}")

    # Exit 0 if overall pass, 1 otherwise (Solr modes only)
    suite1 = tc002.get("passed", False)
    suite2 = (tc003.get("passed", True) and tc004.get("passed", True) and
              (tc_news_date or {}).get("passed", True))
    suite3 = (tc005.get("passed", True) and tc006.get("passed", True) and tc005b.get("passed", True))
    sys.exit(0 if (suite1 and suite2 and suite3) else 1)


if __name__ == "__main__":
    main()
