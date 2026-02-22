# Task 1 — API Exploration Findings

## Overview
This document summarizes the findings from exploring the provided REST APIs for the DISH Second Course Data Engineer technical assessment (Task 1). The goal is to validate access, understand response shapes, document schemas, pagination, filters, and key operational/data-quality observations.

## Base URL
`https://dish-second-course-gateway-2tximoqc.nw.gateway.dev`

## Authentication
Requests require an API key passed via header:

- `X-API-Key: $API_KEY`



```bash
export API_KEY='YOUR_API_KEY_HERE'
```

## Commands used (tested)

```bash
# Daily visits (paginated)
curl -sS -H "X-API-Key: $API_KEY"   "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev/daily-visits?page=1&limit=10"   | python3 -m json.tool

# GA sessions (nested) for a specific date (paginated)
curl -sS -H "X-API-Key: $API_KEY"   "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev/ga-sessions-data?page=1&limit=5&date=20170801"   | python3 -m json.tool
```

---

## 1) `GET /daily-visits` (Simple flat data)

### Purpose
Returns aggregated daily visit counts.

### Request parameters (observed)
- `page` (int, starting at 1): page number
- `limit` (int): records per page
- `start_date` (string, `YYYY-MM-DD`, optional): filter lower bound
- `end_date` (string, `YYYY-MM-DD`, optional): filter upper bound

### Response structure (observed)
Top-level keys:
- `filters_applied`
- `metadata`
- `pagination`
- `records`

#### `filters_applied`
Echoes the date filters actually applied:
- `start_date` (string or null)
- `end_date` (string or null)

#### `metadata`
Observed fields:
- `api_version` (string)
- `dataset` (string) — e.g. `bigquery-public-data.google_analytics_sample.daily_visits`
- `records_returned` (int)

#### `pagination`
Observed fields:
- `page` (int)
- `limit` (int)
- `total_pages` (int)
- `total_records` (int)
- `has_next` (bool)
- `has_previous` (bool)

**Pagination behavior:** increment `page` until `has_next == false`.

#### `records` schema
Each record contains:
- `visit_date` (string, `YYYY-MM-DD`)
- `total_visits` (int)

Example record:
```json
{ "visit_date": "2017-08-01", "total_visits": 2587 }
```

### Date filtering behavior (validated)
A filtered request matching the assessment range:

```bash
curl -sS -H "X-API-Key: $API_KEY"   "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev/daily-visits?start_date=2016-08-01&end_date=2017-08-01&limit=3&page=1"   | python3 -m json.tool
```

Observed:
- `filters_applied` reflects the provided dates.
- The first returned record includes `visit_date = 2017-08-01`, indicating the `end_date` boundary is included in results.
- `total_records` decreases from **367** (unfiltered sample) to **366** (filtered sample), confirming the filter changes the dataset and prevents out-of-range data (e.g., `2017-08-02` was observed in an unfiltered call).


**ETL implication:** always apply `start_date` and `end_date` during ingestion to strictly match the expected time window.

---

## 2) `GET /ga-sessions-data` (Nested session data)

### Purpose
Returns session-level data (nested JSON) for a specific date. Records contain nested objects and arrays that require a flattening/normalization strategy for tabular storage.

### Request parameters (observed)
- `date` (string, `YYYYMMDD`): date selector (e.g., `20170801`)
- `page` (int): page number
- `limit` (int): records per page
- Optional filters returned in `filters_applied`:
  - `channel_grouping`
  - `country`
  - `device_category`

### Response structure (observed)
Top-level keys:
- `filters_applied`
- `metadata`
- `pagination`
- `records`

#### `metadata`
Observed fields:
- `api_version` (string)
- `dataset` (string) — e.g. `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
- `records_returned` (int)

#### `pagination`
Same page-based pattern as `/daily-visits`:
- `page`, `limit`, `total_pages`, `total_records`, `has_next`, `has_previous`

**Pagination behavior:** increment `page` until `has_next == false`.

#### `records` schema (observed)
Each record represents a session and contains:

**Scalar fields**
- `date` (string, `YYYYMMDD`)
- `fullVisitorId` (string)
- `visitId` (int)
- `visitNumber` (int)
- `visitStartTime` (int, epoch seconds)
- `channelGrouping` (string)

**Nested objects**
- `device`: `{ browser, isMobile, operatingSystem }`
- `geoNetwork`: `{ continent, subContinent, country, region, city, ... }`
- `totals`: `{ visits, hits, pageviews, bounces (nullable), newVisits (nullable) }`
- `trafficSource`: `{ source, medium, referralPath (nullable), keyword (nullable), adContent (nullable) }`

**Nested arrays**
- `customDimensions`: list of `{ index: int, value: string }` (can be empty)
- `hits_sample`: list of hit objects (variable length), e.g. `{ hitNumber, hostname, pagePath, pageTitle, time, type, isInteraction }`

### Data-quality observations
- Nullability is present in multiple fields (e.g., `trafficSource.keyword`, `trafficSource.adContent`, `totals.newVisits`, `totals.bounces`).
- Arrays are variable-length and may be empty.

### Flattening implication (for subsequent ETL)
Because `records` include arrays (`hits_sample`, `customDimensions`), a clear normalization decision is required:
- **Option A (simpler):** store arrays as JSON/repeated fields and load one row per session.
- **Option B (normalized):** create additional child tables (e.g., `hits`, `custom_dimensions`) keyed by `(date, fullVisitorId, visitId)` to preserve one-to-many relationships without row explosion.


## Task 2 — ETL Pipeline (API → Transform → BigQuery)

### Overview
This task implements an **ETL pipeline** in Python to ingest data from the provided API endpoints, transform the payloads into tabular format, and load the results into **Google BigQuery**.

### Endpoints
- `daily-visits`
- `ga-sessions-data`

### ETL Approach
#### 1) Extract
- Calls the API using `requests`
- Uses `X-API-Key` header for authentication
- Handles pagination using `page`, `limit`, and `pagination.has_next`
- Retries transient failures (`429`, `500`, `502`, `503`, `504`) with exponential backoff
- Saves **raw JSON responses** page-by-page to local storage (`raw_responses/`)

#### 2) Transform
##### Daily Visits
- Converts records into a pandas DataFrame
- Casts:
  - `visit_date` → `DATE`
  - `total_visits` → numeric
- Adds metadata columns:
  - `load_timestamp`
  - `source_file`
  - `api_version`
  - `source_dataset`

##### GA Sessions
- Converts nested JSON records into a tabular structure
- Flattens one-level nested objects into columns using prefixes:
  - `device_*`
  - `geoNetwork_*`
  - `totals_*`
  - `trafficSource_*`
- Stores arrays as JSON strings:
  - `customDimensions_json`
  - `hits_sample_json`
- Adds metadata columns:
  - `load_timestamp`
  - `source_file` (date partition, e.g. `20160801`)
  - `api_version`
  - `source_dataset`

#### 3) Load
- Loads transformed pandas DataFrames into BigQuery using `google-cloud-bigquery`
- Uses explicit schemas for both tables
- Ensures the target dataset exists (creates it if missing)

---

### BigQuery Tables Created
- `daily_visits`
- `ga_sessions`

### Raw JSON Storage
Raw API responses are stored locally for traceability and reprocessing:

- `raw_responses/daily_visits/...`
- `raw_responses/ga_sessions/date=YYYYMMDD/...`

This satisfies the requirement to store the raw JSON payloads before transformation.

---

### Data Quality / Validation
The pipeline includes:
- Row count validation (`extracted rows == API pagination.total_records`)
- Null checks on key columns
- Duplicate key check for GA sessions on:
  - `(date, fullVisitorId, visitId)`

---

### Metadata Columns Added
Both target tables include metadata columns for lineage and auditability:
- `load_timestamp`
- `source_file`
- `api_version`
- `source_dataset`

---

### Assumptions / Design Choices
- **ETL (not ELT)** was implemented to align with task instructions
- `ga-sessions-data` nested arrays (`hits_sample`, `customDimensions`) are stored as JSON strings to keep the output tabular and avoid row explosion
- The current implementation runs as a **full refresh** for submission simplicity
- For production hardening, a `MERGE` (upsert) strategy can be added for `ga_sessions`

---

### How to Run

#### 1) Set environment variables (via `.env`)
Example:

```env
API_KEY=your_api_key
GCP_PROJECT=your-gcp-project-id
BQ_DATASET=dish_second_course
BQ_LOCATION=EU
GOOGLE_APPLICATION_CREDENTIALS= workspaces/dish_case/pipeline/service_account.json
```
#### 2) Install dependencies
```
uv add requests pandas python-dotenv google-cloud-bigquery pyarrow
```
## Task 3 - Airflow Orchestration (`etl_google_analytics_dag.py`)

### Overview
I created an Airflow DAG named **`etl_google_analytics_dag`** to orchestrate the ETL pipeline built in Task 2.

The DAG reuses the ETL helper functions from `pipeline/data_pipeline.py` and runs them using Airflow `PythonOperator` tasks.

---

### DAG File
- **Path:** `airflow/dags/etl_google_analytics_dag.py`

---

### Schedule
The DAG is scheduled to run:

- **Wednesdays at 06:00 AM**
- **Wednesdays at 06:00 PM**

Cron expression used:
```cron
0 6,18 * * 3
```

---

### Retry / Timeout Configuration
Configured according to the task requirements:

- **Retries:** `2`
- **Retry delay:** `5 minutes`
- **Task timeout:** `3 minutes` per task (`execution_timeout`)

---

### DAG Task Flow
The DAG contains the following tasks:

1. **`start`** (`EmptyOperator`)
   - Dummy start task for readability

2. **`daily_visits_etl`** (`PythonOperator`)
   - Extracts `/daily-visits` with pagination
   - Saves raw JSON payloads
   - Transforms data into tabular format
   - Runs data quality checks
   - Loads to BigQuery (`WRITE_TRUNCATE`)

3. **`ga_sessions_etl`** (`PythonOperator`)
   - Extracts `/ga-sessions-data` for one logical date (`ds_nodash`)
   - Saves raw JSON payloads
   - Flattens nested JSON into tabular columns
   - Runs data quality checks
   - Loads to BigQuery (`WRITE_APPEND`)

4. **`summary`** (`PythonOperator`)
   - Logs DAG run metadata (`run_id`, `ds`, `ds_nodash`)

5. **`end`** (`EmptyOperator`)
   - Dummy end task for readability

Task dependency order:
```text
start -> daily_visits_etl -> ga_sessions_etl -> summary -> end
```

---

### How the DAG passes variables to the ETL
The DAG does not overwrite the ETL script. Instead, it imports ETL functions from `pipeline/data_pipeline.py` and passes values as function arguments.

Examples:
- Daily visits date range is passed using:
  - `DAILY_START_DATE`
  - `DAILY_END_DATE`
- GA sessions date is passed using Airflow runtime context:
  - `context["ds_nodash"]` (format `YYYYMMDD`)

Optional environment variable overrides are supported:
- `DAILY_START_DATE`
- `DAILY_END_DATE`
- `GA_DATE_OVERRIDE`

---

### Why `catchup=False`
I set `catchup=False` so the DAG runs only for current/future schedules and does not automatically backfill old runs.

This is intentional because:
- historical backfill was already handled in Task 2
- automatic catchup could trigger many old runs
- `ga_sessions` uses incremental append logic and catchup can increase duplicate risk without upsert logic

---

### Notes on GA Incremental Loading
The `ga_sessions_etl` task loads one Airflow logical date (`ds_nodash`) per run and uses `WRITE_APPEND`.

For production hardening, I would improve this by:
- loading into a staging table
- then using a BigQuery `MERGE` into the final table on a business key:
  - `(date, fullVisitorId, visitId)`

This would make reruns idempotent and prevent duplicates.

---

## Bonus - Data Quality Monitoring

### What I would monitor
- **Row counts** per task/run (extracted vs loaded)
- **Nulls in key fields**
  - `daily_visits`: `visit_date`, `total_visits`
  - `ga_sessions`: `date`, `fullVisitorId`, `visitId`
- **Duplicate rate** on GA session key:
  - `(date, fullVisitorId, visitId)`
- **API extraction consistency**
  - Compare extracted row count with `pagination.total_records`
- **BigQuery load failures**
  - failed jobs, rejected rows, schema mismatches

### How I would handle failures
- Fail the task if:
  - API row count mismatch
  - critical nulls exceed threshold
  - load job fails
- Use Airflow retries (already configured)
- Keep raw JSON payloads for debugging and replay

### Alerts
In production I would add:
- Airflow email/Slack alerts on task failure
- monitoring on repeated retry failures
- BigQuery job monitoring / logging

---

## Bonus - Data Governance and Compliance

### Metadata / Lineage
The pipeline stores lineage metadata in both target tables:
- `load_timestamp`
- `source_file`
- `api_version`
- `source_dataset`

It also stores:
- raw JSON payloads (traceability)
- run manifests (if enabled)

### Governance Practices
- Secrets are not hardcoded (API key and GCP config are loaded from environment variables)
- Service account credentials are handled through Google Application Default Credentials (ADC)
- `.env` and service account JSON files are excluded from version control

### Privacy / Compliance Considerations
- Review source fields for any PII before production usage
- Hash or mask identifiers if required
- Apply least-privilege IAM roles for BigQuery access
- Document schemas and ownership in a data catalog / README

