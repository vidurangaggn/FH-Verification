# FH Verification - Test Configuration
# Edit these to match your environment

# Run mode: "solr" = Solr verification only (cores, health, exchange, freshness).
#           "logs" = Logs report only (nohup job extraction times). Requires NOHUP_LOG_PATH.
#           "both" = Run Solr verification and logs, single combined report.
RUN_MODE = "solr"

# Solr base URLs (no trailing slash). Port 8985 per Readme.
PRODUCTION_SOLR_URL = "http://172.20.178.40:8985"
STAGING_SOLR_URL = "http://172.20.178.240:8985"

# All 16 required cores from config/solr.yml (per Readme)
REQUIRED_CORES = [
    "FINANCIAL_RATIOS",
    "FH_FINANCIAL_RATIOS",
    "EARNINGS",
    "EARNINGS_HISTORY",
    "ACTUAL_EARNINGS_HISTORY",
    "PERFORMANCE_DATA",
    "TREND_DATA",
    "TECH",
    "SUPPORT_RESISTANCE",
    "COMPANY_PROFILE",
    "COMPANY_PROFILE_IMAGE",
    "NEWS",
    "NEWS_SENTIMENT",
    "ANALYST_VIEW",
    "CONSOLIDATED_SCORE",
    "PEERS",
]

# Field name for exchange (try EXCHANGE first; some cores may use SOURCE_ID)
EXCHANGE_FIELD = "EXCHANGE"

# Variance threshold (%). Pass if |Prod - Stage| / Prod * 100 <= this
VARIANCE_THRESHOLD_PERCENT = 5.0

# Data freshness: max allowed hours difference between Prod and Stage latest update
MAX_TIME_DIFF_HOURS = 4.0

# Period-based freshness: compare document write counts in these time windows (hours)
# Avoids false results when a job is running at "latest timestamp" check time
FRESHNESS_PERIODS_HOURS = [6, 24, 72]

# End the count window this many hours in the past so both Prod and Staging have
# had time to finish runs (e.g. staging may be running while prod is idle, or vice versa).
# Window for period N hours = [NOW-(N+end_hours_ago) to NOW-end_hours_ago].
FRESHNESS_PERIOD_END_HOURS_AGO = 1

# For period-based freshness: pass if (Prod - Stage) count variance <= this %
WRITE_COUNT_VARIANCE_THRESHOLD_PERCENT = 10.0

# NEWS core: date-based comparison using CREATED_ON (e.g. "2023-01-05T10:20:10Z")
# Count documents with CREATED_ON in the last N months (Prod vs Staging)
NEWS_DATE_FIELD = "CREATED_ON"
NEWS_DATE_PERIOD_MONTHS = 3

# Optional: if your Solr uses different core names for the period-count query (e.g. with _ALL suffix),
# set this to map logical name -> actual core name. Example: {"FINANCIAL_RATIOS": "FINANCIAL_RATIOS_ALL"}.
# Leave empty to use REQUIRED_CORES names as-is.
PERIOD_COUNT_CORE_MAP = {}

# Optional: path to finhub-extractor nohup log file to extract job run times.
# Log is streamed line-by-line (safe for very large files, e.g. 800k+ lines).
# Leave None to skip nohup parsing.
NOHUP_LOG_PATH = "nohup-21-10-2025_10_48_15_AM.out"  # e.g. "nohup-21-10-2025_10_48_15_AM.out"

# Request timeout in seconds
REQUEST_TIMEOUT = 30

