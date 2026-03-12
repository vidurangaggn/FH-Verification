# FHE Staging Setup | Finhub Solr Comparison

# Test Cases

## Production vs Staging Solr Validation

## Test Environment Details

#### Solr Servers:

```
Environment SG NV
Production 172.20.178.408985 172.20.162.40
Staging 172.20.178.2408985 172.20.162.250
```
#### FHE Instances in Staging Setup:

```
Instance\Environment SG NV
Main 172.20.178.95 172.20.162.
News 172.20.178.195 172.20.162.
Support 172.20.178.195 172.20.162.
```
## Test Execution Guidelines

### Pre-requisites

####  Access to both Production and Staging Solr admin consoles

####  Access to Solr query interfaces (or curl commands)

####  Understanding of expected data freshness (based on FH Task Schedules)

## Test Suite 1: Core Existence and Accessibility

### TC-001: Verify All Required Solr Cores Exist

#### Objective:  Ensure all 16 required Solr cores are present in staging environment

#### Test Data:  Core names from config/solr.yml

#### Test Steps:

####  Access Staging Solr Admin UI http://<staging-solr>: 8985 /solr/#/~cores

####  Verify each core exists

#### Expected Cores:

```
# Core Name Status Comments
1 FINANCIAL_RATIOS ☐ EXISTS Calculated financial ratios
2 FH_FINANCIAL_RATIOS ☐ EXISTS Finnhub financial ratios
3 EARNINGS ☐ EXISTS Earnings forecasts
4 EARNINGS_HISTORY ☐ EXISTS Historical earnings
5 ACTUAL_EARNINGS_HISTORY ☐ EXISTS Actual earnings results
```

```
# Core Name Status Comments
6 PERFORMANCE_DATA ☐ EXISTS Performance metrics
7 TREND_DATA ☐ EXISTS Trend indicators
8 TECH ☐ EXISTS Technical indicators
9 SUPPORT_RESISTANCE ☐ EXISTS Support/resistance levels
10 COMPANY_PROFILE ☐ EXISTS Company metadata
11 COMPANY_PROFILE_IMAGE ☐ EXISTS Company logos
12 NEWS ☐ EXISTS News articles
13 NEWS_SENTIMENT ☐ EXISTS News sentiment analysis
14 ANALYST_VIEW ☐ EXISTS Analyst recommendations
15 CONSOLIDATED_SCORE ☐ EXISTS Aggregated scores
16 PEERS ☐ EXISTS Peer comparisons
```
### TC-002: Core Health Check

#### Objective:  Verify all cores are healthy and queryable

#### Test Steps:  For each core, execute a basic query:

#### curl "http://<solr-url>:8985/solr/CORE_NAME/select?q=*:*&rows=0&wt=json"

#### Expected Result:

#### Response status: 200 OK

#### JSON response with numFound field

#### No error messages

#### Test Matrix:

```
Core Name Production Health Staging Health Match
FINANCIAL_RATIOS ☐ HEALTHY ☐ HEALTHY ☐ YES
FH_FINANCIAL_RATIOS ☐ HEALTHY ☐ HEALTHY ☐ YES
EARNINGS ☐ HEALTHY ☐ HEALTHY ☐ YES
... (repeat for all cores)
```
## Test Suite 2: Exchange-Wise Data Coverage Validation

#### Note:  Since the new staging setup has new exchanges/sources MENA sources) configured compared to

#### production, we compare data coverage by exchange rather than total document counts.

### TC-003: Exchange Coverage and Availability Check

#### Objective:  Identify which exchanges are available in both Production and Staging, and which are unique to each

#### environment

#### Test Steps:

####  Get list of all exchanges in Production for key cores

####  Get list of all exchanges in Staging

####  Identify common exchanges, production-only, and staging-only exchanges


#### Sample Query:

#### http://172.20.178.408985/solr/EARNINGS/select?facet.field=EXCHANGE&facet=on&q=*%3A*&rows=

#### Exchange Comparison Summary:

```
Environment Unique Exchanges Comments
Production Only List codes) Exchanges being retired/not migrated
Staging Only List codes) New exchanges added in staging
Common to Both List codes) Exchanges to validate in detail
```
#### Pass Criteria:

#### All expected staging exchanges are identified

#### No unexpected exchanges appear

#### Common exchanges are documented for detailed comparison in TC

### TC-004: Exchange-Wise Document Count Comparison

#### Objective:  For common exchanges between Production and Staging, compare document counts to ensure data

#### completeness

#### Pre-requisite:  Complete TC003 to identify common exchanges

#### Test Data:  Use common exchanges identified in TC003 (e.g., NSDQ, NYSE, AMEX, LSE, SGX, HKEX, etc.)

#### Test Steps:

####  For each common exchange, get document count by core

####  Compare counts and calculate variance

####  Document any significant discrepancies

#### Sample Query:

#### curl "http://172.20.178.408985/solr/EARNINGS/select?q=EXCHANGENSDQ&rows=0&wt=json" | jq '.response.num

#### Found'

#### # Calculate variance

#### # Variance %  |Prod  Stage)| / Prod * 100

#### Alternative: Use facet query to get all exchanges at once:

#### http://172.20.178.408985/solr/EARNINGS/select?facet.field=EXCHANGE&facet=on&q=*%3A*&rows=

#### Results Table  FINANCIAL_RATIOS Core:


```
Exchange Prod Count Stage Count Variance % Comments
NSDQ
NYSE
AMEX
LSE
Add other exchanges)
```
#### Results Table  FH_FINANCIAL_RATIOS Core:

```
Exchange Prod Count Stage Count Variance % Comments
Same as above)
```
#### Results Table  EARNINGS Core:

```
Exchange Prod Count Stage Count Variance % Comments
Same as above)
```
#### Results Table  NEWS Core:

```
Exchange Prod Count Stage Count Variance % Comments
Same as above)
```
#### Results Table  TECH Core:

```
Exchange Prod Count Stage Count Variance % Comments
Same as above)
```
#### Results Table  ANALYST_VIEW Core:

```
Exchange Prod Count Stage Count Variance % Comments
Same as above)
```
#### Pass Criteria:

#### For common exchanges, variance  5% (allowing for timing differences in extraction)

#### Any variance  5% must be documented and explained

#### Troubleshooting Notes:

#### If SOURCE_ID field doesn't exist, check the data in Solr Core to find correct exchange field name

#### Possible field names: SOURCE_ID, EXCHANGE, etc.

## Test Suite 3: Data Freshness Validation

### TC-005: Latest Update Timestamp Check

#### Objective:  Verify staging contains recent data comparable to production

#### Test Steps:  For each core, query the most recent document by update timestamp

#### Results:

```
Core Name Prod Latest Stage Latest Time Diff (hrs)
FINANCIAL_RATIOS
FH_FINANCIAL_RATIOS
EARNINGS
PERFORMANCE_DATA
```

```
Core Name Prod Latest Stage Latest Time Diff (hrs)
TREND_DATA
TECH
NEWS
NEWS_SENTIMENT
ANALYST_VIEW
```
#### Pass Criteria:  Time difference  4 hours

### TC-006: Schedule-Based Data Update Verification

#### Objective:  Verify extraction tasks are running according to their configured schedules in staging

#### Important Note:  Different tasks have different schedules (daily, weekly, hourly, weekday-only, etc.). Before testing

#### each core, check the job schedule in config/application.properties and adjust the test timeframe accordingly.

### Step 1: Identify Job Schedule

#### Before testing a core, find its schedule in config/application.properties:

#### Example Schedules:

#### # Daily jobs

#### earnings.schedule=0 0 5 * * * # Runs daily at 5 AM

#### analyst.view.schedule=0 0 4 * * * # Runs daily at 4 AM

#### performance.data.extraction.schedule=0 0 3 * * * # Runs daily at 3 AM

#### # Hourly/Sub-daily jobs

#### news.sentiment.schedule=0 0 */3 * * * # Runs every 3 hours

#### benzinga.news.schedule=0 0 */1 * * * # Runs every 1 hour

#### db.news.schedule=0 */5 * * * * # Runs every 5 minutes

#### # Weekday-only jobs MONFRI

#### company.profile.schedule=0 0 6 * * MONFRI # Runs weekdays at 6 AM

#### financial.ratios.schedule=0 0 7 * * MONFRI # Runs weekdays at 7 AM

#### db.tech.schedule=0 0 4,14 * * MONFRI # Runs weekdays at 4 AM & 2 PM

#### # Weekly jobs

#### earnings.history.schedule=0 0 23 * * FRI # Runs Fridays at 11 PM

#### all.company.profile.image.schedule=0 30 14 * * SAT # Runs Saturdays at 230 PM

### Step 2: Check Latest Update Timestamp

#### For each core, get the most recent update timestamp:

### Step 3: Verify Based on Schedule Type

#### Choose the appropriate validation based on the schedule:

#### A. For High-Frequency Jobs Hourly/Every few hours):

#### Examples: NEWS, NEWS_SENTIMENT, DB_NEWS

#### Check updates within last 612 hours

#### B. For Daily Jobs:

#### Examples: EARNINGS, ANALYST_VIEW, PERFORMANCE_DATA

#### Check updates within last 24 36 hours

#### If testing before schedule time, check previous day's data


#### C. For Weekday-Only Jobs MONFRI

#### Examples: COMPANY_PROFILE, FINANCIAL_RATIOS, TECH, PEERS

#### Important  Onl y test on weekdays; these jobs don't run on weekends

#### On weekends, latest data will be from Friday

#### D. For Weekly Jobs:

#### Examples: EARNINGS_HISTORY F riday), COMPANY_PROFILE_IMAGE Saturday)

#### Check updates within last 7 days

#### Verify update occurred on correct day of week

### Step 4: Record Results

#### Test Results Template:

```
Core Name
```
```
Schedule
Found in
Config
( Get the actual
value from
production
config )
```
```
Schedule Type
```
```
Expected
Update
Window
```
```
Prod Latest Stage Latest Match?
```
```
EARNINGS 0 0 5 * * * Daily  5 AM Last 24 hours
NEWS_SENTIMENT 0 0 */3 * * * Every 3 hours Last 6 hours
```
```
COMPANY_PROFILE 0 0 6 * * MONFRI Weekdays  6AM
```
```
Last 24h
(weekdays) /
Last 72h
(weekends)
EARNINGS_HISTORY 0 0 23 * * FRI Weekly F riday)Last 7 days
```
```
FINANCIAL_RATIOS 0 0 7 * * MONFRI Weekdays  7AM
```
```
Last 24h
(weekdays) /
Last 72h
(weekends)
ANALYST_VIEW 0 0 4 * * * Daily  4 AM Last 24 hours
PERFORMANCE_DATA 0 0 3 * * * Daily  3 AM Last 24 hours
TREND_DATA 0 0 12 * * * Daily  12 PM Last 24 hours
```
```
TECH 0 0 4,14 * *MONFRI Weekdays  4AM & 2 PM
```
```
Last 24h
(weekdays) /
Last 72h
(weekends)
```
```
PEERS 0 0 9 * * MONFRI Weekdays  9AM
```
```
Last 24h
(weekdays) /
Last 72h
(weekends)
NEWS Multipleschedules Hourly/Every3h Last 6 hours
CONSOLIDATED_SCORE Fixed delay (1h) Continuous Last 2 hours
```
### Pass Criteria

#### Based on schedule type:

```
Schedule Type Pass Criteria
High-Frequency  (hourly, every 3h, every 5min) Latest update within last 6 hours in both environments
```
```
Daily
```
```
Latest update within last 24 36 hours; both
environments updated on same calendar day (after
schedule time)
```

```
Schedule Type Pass Criteria
```
```
Weekday-only
```
```
On weekdays: updated today (after schedule time)
On weekends: latest update from Friday
Within 24h on weekdays, 72h on weekends
Weekly Latest updatcorrect day of weeke within 7 days; update occurred on
Continuous  (fixed delay) Latest update within 2x the configured delay interval
```
#### General Guidelines:

#### Production and Staging should have similar update patterns

#### Time difference between Prod and Stage latest updates should be < 2 hours for same schedule

### Example Walkthrough

#### Testing EARNINGS Core:

####  Find Schedule: earnings.schedule= 0 0 5 * * *  Runs daily at 5 AM

####  Current Time:  200 PM (after 5 AM, so today's job should have run)

####  Check Production:

#### curl "http://<prod-solr>:8985/solr/EARNINGS/select?q=*:*&sort=LAST_UPDATED_ON%20desc&rows=1&fl=LAS

#### T_UPDATED_ON&wt=json"

#### # Result: 20260108T051523Z

####  Check Staging:

#### curl "http://<staging-solr>:8985/solr/EARNINGS/select?q=*:*&sort=LAST_UPDATED_ON%20desc&rows=1&fl=L

#### AST_UPDATED_ON&wt=json"

#### # Result: 20260108T051845Z

####  Evaluation:

#### Both updated today 20260108 ✓

#### Both updated around 5 AM as scheduled ✓

#### Time difference: 3 minutes (acceptable) ✓

#### PASS

#### Notes for Testers:

#### Always check config/application.properties for the actual schedule before testing

#### Test weekday-only jobs on actual weekdays for accurate results

---

## Running the Automated Tests

An automated script runs all test steps above and generates an HTML report.

### Prerequisites

- **Python 3.7+** (no extra packages required; uses standard library only)
- Network access to Production and Staging Solr (default: `172.20.178.40:8985` and `172.20.178.240:8985`)

### Configuration

Edit `config.py` to set:

- **`RUN_MODE`** – What to run and report:
  - **`"solr"`** – Solr verification only (cores, health, exchange coverage, freshness). Report: `FH_Verification_Report_*.html`. No log parsing.
  - **`"logs"`** – Logs report only (nohup job extraction times). Requires `NOHUP_LOG_PATH`. Report: `FH_Verification_Logs_Report_*.html`. No Solr requests.
  - **`"both"`** (default) – Run Solr verification and log parsing; single combined report.
- `PRODUCTION_SOLR_URL` / `STAGING_SOLR_URL` – Solr base URLs (used when mode is `solr` or `both`)
- `VARIANCE_THRESHOLD_PERCENT` – pass threshold for exchange count variance (default 5%)
- `MAX_TIME_DIFF_HOURS` – max allowed hours between Prod and Staging latest update (default 4)
- **Freshness (recommended):** `FRESHNESS_PERIODS_HOURS` – time windows for write-count comparison, e.g. `[6, 24, 72]` (default). Counts use a **closed window ending in the past** (`FRESHNESS_PERIOD_END_HOURS_AGO`, default 1 hour) so that if staging is mid-run and prod is idle (or vice versa), counts are comparable.
- `FRESHNESS_PERIOD_END_HOURS_AGO` – end the count window this many hours ago (default 1). E.g. "last 24h" becomes "from 25h ago to 1h ago".
- `WRITE_COUNT_VARIANCE_THRESHOLD_PERCENT` – pass threshold for period write-count variance (default 10%)
- **Nohup log (optional):** `NOHUP_LOG_PATH` – path to the finhub-extractor nohup log file (e.g. `nohup-21-10-2025_10_48_15_AM.out`). The script streams the file line-by-line to extract job extraction start/end times per task; safe for very large logs (800k+ lines).

### Run the tests

```bash
python run_fh_verification_tests.py
```

The script runs:

- **TC-002** – Core health check (select query for each core). Core availability (admin API) is **not** run because Solr often blocks admin requests.
- **TC-003** – Exchange coverage (common / prod-only / staging-only)
- **TC-004** – Exchange-wise document count comparison (variance %)
- **TC-005** – Latest update timestamp per core (Prod vs Staging) – reference only
- **TC-006** – Same timestamps with schedule-type context – reference only
- **TC-005b** – **Period-based freshness:** document write counts in last 6h / 24h / 72h (or as configured). Compares Prod vs Staging counts; pass when variance is within threshold. This is the recommended freshness check.
- **Nohup extraction times** – When `NOHUP_LOG_PATH` is set, parses the finhub-extractor nohup log and reports last run start/end time **per (task, thread)**. Overlapping runs (e.g. next schedule starts on another thread while the previous run is still going) are tracked separately.

### Report

- **Mode `solr` or `both`:** `FH_Verification_Report_YYYYMMDD_HHMMSS.html` – pass/fail summary, core health checks, exchange comparison, period-based freshness, latest-timestamp reference; with **job extraction times from nohup** only when mode is `both` and `NOHUP_LOG_PATH` is set.
- **Mode `logs`:** `FH_Verification_Logs_Report_YYYYMMDD_HHMMSS.html` – log file path, lines read, and job extraction times table (task / thread / last run start–end / runs seen).

Exit code: `0` if all Solr suites pass (or when mode is `logs`), `1` otherwise (e.g. for CI).

