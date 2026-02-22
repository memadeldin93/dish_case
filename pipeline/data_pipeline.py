import os
import json
import time
from datetime import datetime, timezone

import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

BASE_URL = "https://dish-second-course-gateway-2tximoqc.nw.gateway.dev"
RAW_DIR = "raw_responses"
MANIFEST_DIR = "run_manifests"


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def load_env():
    load_dotenv()
    api_key = os.getenv("API_KEY")
    gcp_project = os.getenv("GCP_PROJECT")
    bq_dataset = os.getenv("BQ_DATASET")
    bq_location = os.getenv("BQ_LOCATION")  # optional

    if not api_key:
        raise RuntimeError("API_KEY missing. Put it in .env as API_KEY=...")

    if not gcp_project or not bq_dataset:
        raise RuntimeError("GCP_PROJECT and BQ_DATASET missing. Put them in .env")

    return api_key, gcp_project, bq_dataset, bq_location


def save_raw(payload, folder, filename):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_manifest(manifest, filename):
    os.makedirs(MANIFEST_DIR, exist_ok=True)
    path = os.path.join(MANIFEST_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


def get_json(api_key, endpoint, params, timeout=30, retries=5):
    url = f"{BASE_URL}/{endpoint}"
    headers = {"X-API-Key": api_key}

    last_error = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)

            # retry for rate limit or transient server errors
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"Retryable HTTP {r.status_code}: {r.text[:200]}")

            r.raise_for_status()
            return r.json()

        except Exception as e:
            last_error = e
            if attempt == retries:
                break
            sleep_s = min(20, 1 * (2 ** attempt))
            print(f"[WARN] Request failed (attempt {attempt+1}/{retries+1}): {e}")
            print(f"[WARN] Sleeping {sleep_s}s then retrying...")
            time.sleep(sleep_s)

    raise RuntimeError(f"Request failed after retries. Last error: {last_error}")


def extract_all_pages(api_key, endpoint, base_params, limit, raw_subdir, raw_prefix):
    page = 1
    has_next = True
    all_records = []
    last_payload = {}

    while has_next:
        params = dict(base_params)  # copy so base_params stays unchanged
        params["page"] = page
        params["limit"] = limit

        payload = get_json(api_key, endpoint, params)
        last_payload = payload

        save_raw(
            payload,
            folder=os.path.join(RAW_DIR, raw_subdir),
            filename=f"{raw_prefix}_page={page}.json",
        )

        records = payload.get("records", []) or []
        pagination = payload.get("pagination", {}) or {}

        print(f"[INFO] {endpoint}: page={page} records={len(records)} has_next={pagination.get('has_next')}")
        all_records.extend(records)

        has_next = bool(pagination.get("has_next", False))
        page += 1

    # row-count validation (important for correctness)
    expected = (last_payload.get("pagination", {}) or {}).get("total_records")
    if expected is not None and expected != len(all_records):
        raise RuntimeError(f"Row count mismatch for {endpoint}: expected {expected}, got {len(all_records)}")

    return all_records, last_payload


def transform_daily_visits(records, last_payload, start_date, end_date):
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # type coercion
    df["visit_date"] = pd.to_datetime(df["visit_date"], errors="coerce").dt.date
    df["total_visits"] = pd.to_numeric(df["total_visits"], errors="coerce")

    # metadata / lineage
    df["load_timestamp"] = utc_now_iso()
    df["source_file"] = f"daily_visits_{start_date}_to_{end_date}"
    df["api_version"] = last_payload.get("metadata", {}).get("api_version")
    df["source_dataset"] = last_payload.get("metadata", {}).get("dataset")

    return df


def flatten_one_level(prefix, obj, out_row):
    for k, v in obj.items():
        out_row[f"{prefix}_{k}"] = v


def transform_ga_sessions(records, last_payload, date):
    if not records:
        return pd.DataFrame()

    rows = []
    load_ts = utc_now_iso()
    api_version = last_payload.get("metadata", {}).get("api_version")
    source_dataset = last_payload.get("metadata", {}).get("dataset")

    for rec in records:
        row = {
            "date": rec.get("date"),
            "fullVisitorId": rec.get("fullVisitorId"),
            "visitId": rec.get("visitId"),
            "visitNumber": rec.get("visitNumber"),
            "visitStartTime": rec.get("visitStartTime"),
            "channelGrouping": rec.get("channelGrouping"),
        }

        if isinstance(rec.get("device"), dict):
            flatten_one_level("device", rec["device"], row)
        if isinstance(rec.get("geoNetwork"), dict):
            flatten_one_level("geoNetwork", rec["geoNetwork"], row)
        if isinstance(rec.get("totals"), dict):
            flatten_one_level("totals", rec["totals"], row)
        if isinstance(rec.get("trafficSource"), dict):
            flatten_one_level("trafficSource", rec["trafficSource"], row)

        # arrays stored as JSON strings (tabular ETL compromise)
        row["customDimensions_json"] = json.dumps(rec.get("customDimensions", []), ensure_ascii=False)
        row["hits_sample_json"] = json.dumps(rec.get("hits_sample", []), ensure_ascii=False)

        # metadata / lineage
        row["load_timestamp"] = load_ts
        row["source_file"] = date
        row["api_version"] = api_version
        row["source_dataset"] = source_dataset

        rows.append(row)

    df = pd.DataFrame(rows)

    # numeric fields
    df["visitId"] = pd.to_numeric(df["visitId"], errors="coerce")
    df["visitNumber"] = pd.to_numeric(df["visitNumber"], errors="coerce")
    df["visitStartTime"] = pd.to_numeric(df["visitStartTime"], errors="coerce")

    return df


def run_quality_checks_daily(df):
    print("[CHECK] daily_visits row count:", len(df))
    print("[CHECK] daily_visits null visit_date:", int(df["visit_date"].isna().sum()))
    print("[CHECK] daily_visits null total_visits:", int(df["total_visits"].isna().sum()))
    if (df["total_visits"].dropna() < 0).any():
        raise RuntimeError("daily_visits contains negative total_visits")


def run_quality_checks_ga(df):
    print("[CHECK] ga_sessions row count:", len(df))
    print("[CHECK] ga_sessions null date:", int(df["date"].isna().sum()))
    print("[CHECK] ga_sessions null fullVisitorId:", int(df["fullVisitorId"].isna().sum()))
    print("[CHECK] ga_sessions null visitId:", int(df["visitId"].isna().sum()))

    dup_count = int(df.duplicated(subset=["date", "fullVisitorId", "visitId"]).sum())
    print("[CHECK] ga_sessions duplicate keys (date, fullVisitorId, visitId):", dup_count)


def bq_client(gcp_project, bq_location=None):
    return bigquery.Client(project=gcp_project, location=bq_location)


def ensure_dataset(client, dataset_id, location=None):
    try:
        client.get_dataset(dataset_id)
    except Exception:
        ds = bigquery.Dataset(dataset_id)
        if location:
            ds.location = location
        client.create_dataset(ds, exists_ok=True)
        print(f"[INFO] Created dataset {dataset_id}")


def load_df_to_bq(client, dataset_id, table_name, df, schema, write_disposition):
    if df.empty:
        print(f"[WARN] DataFrame is empty. Skipping load for {dataset_id}.{table_name}")
        return

    table_id = f"{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"[INFO] Loaded {len(df)} rows into {table_id}. Table now has {table.num_rows} rows.")


def schema_daily_visits():
    return [
        bigquery.SchemaField("visit_date", "DATE"),
        bigquery.SchemaField("total_visits", "INT64"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("source_file", "STRING"),
        bigquery.SchemaField("api_version", "STRING"),
        bigquery.SchemaField("source_dataset", "STRING"),
    ]


def schema_ga_sessions():
    return [
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("fullVisitorId", "STRING"),
        bigquery.SchemaField("visitId", "INT64"),
        bigquery.SchemaField("visitNumber", "INT64"),
        bigquery.SchemaField("visitStartTime", "INT64"),
        bigquery.SchemaField("channelGrouping", "STRING"),

        bigquery.SchemaField("device_browser", "STRING"),
        bigquery.SchemaField("device_isMobile", "BOOL"),
        bigquery.SchemaField("device_operatingSystem", "STRING"),

        bigquery.SchemaField("geoNetwork_city", "STRING"),
        bigquery.SchemaField("geoNetwork_country", "STRING"),
        bigquery.SchemaField("geoNetwork_region", "STRING"),
        bigquery.SchemaField("geoNetwork_continent", "STRING"),
        bigquery.SchemaField("geoNetwork_subContinent", "STRING"),
        bigquery.SchemaField("geoNetwork_networkDomain", "STRING"),

        bigquery.SchemaField("totals_visits", "INT64"),
        bigquery.SchemaField("totals_hits", "INT64"),
        bigquery.SchemaField("totals_pageviews", "INT64"),
        bigquery.SchemaField("totals_bounces", "INT64"),
        bigquery.SchemaField("totals_newVisits", "INT64"),

        bigquery.SchemaField("trafficSource_source", "STRING"),
        bigquery.SchemaField("trafficSource_medium", "STRING"),
        bigquery.SchemaField("trafficSource_referralPath", "STRING"),
        bigquery.SchemaField("trafficSource_keyword", "STRING"),
        bigquery.SchemaField("trafficSource_adContent", "STRING"),

        bigquery.SchemaField("customDimensions_json", "STRING"),
        bigquery.SchemaField("hits_sample_json", "STRING"),

        bigquery.SchemaField("load_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("source_file", "STRING"),
        bigquery.SchemaField("api_version", "STRING"),
        bigquery.SchemaField("source_dataset", "STRING"),
    ]


def main():
    api_key, gcp_project, bq_dataset, bq_location = load_env()
    client = bq_client(gcp_project, bq_location)

    dataset_id = f"{gcp_project}.{bq_dataset}"
    ensure_dataset(client, dataset_id, bq_location)

    run_started_at = utc_now_iso()

    # -----------------------------
    # DAILY VISITS ETL (full refresh)
    # -----------------------------
    daily_start_date = "2016-08-01"
    daily_end_date = "2017-08-01"

    daily_records, daily_last = extract_all_pages(
        api_key=api_key,
        endpoint="daily-visits",
        base_params={"start_date": daily_start_date, "end_date": daily_end_date},
        limit=500,
        raw_subdir="daily_visits",
        raw_prefix=f"daily_visits_{daily_start_date}_to_{daily_end_date}",
    )

    daily_df = transform_daily_visits(daily_records, daily_last, daily_start_date, daily_end_date)
    run_quality_checks_daily(daily_df)

    load_df_to_bq(
        client=client,
        dataset_id=dataset_id,
        table_name="daily_visits",
        df=daily_df,
        schema=schema_daily_visits(),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    save_manifest(
        {
            "run_started_at": run_started_at,
            "endpoint": "daily-visits",
            "filters": {"start_date": daily_start_date, "end_date": daily_end_date},
            "rows_extracted": len(daily_records),
            "rows_transformed": len(daily_df),
            "api_version": (daily_last.get("metadata", {}) or {}).get("api_version"),
            "source_dataset": (daily_last.get("metadata", {}) or {}).get("dataset"),
            "pagination": daily_last.get("pagination", {}),
        },
        filename=f"daily_visits_{daily_start_date}_to_{daily_end_date}_manifest.json"
    )

    # -----------------------------
    # GA SESSIONS ETL (full refresh over date range)
    # -----------------------------
    ga_start_date = "2016-08-01"
    ga_end_date = "2016-09-01"
    # If the case study expects a wider range, change the dates above.

    ga_frames = []
    ga_total_extracted = 0
    ga_dates_processed = []

    for d in pd.date_range(start=ga_start_date, end=ga_end_date, freq="D"):
        ga_date = d.strftime("%Y%m%d")
        ga_dates_processed.append(ga_date)

        ga_records, ga_last = extract_all_pages(
            api_key=api_key,
            endpoint="ga-sessions-data",
            base_params={"date": ga_date},
            limit=200,
            raw_subdir=f"ga_sessions/date={ga_date}",
            raw_prefix=f"ga_sessions_{ga_date}",
        )

        ga_total_extracted += len(ga_records)

        ga_df_day = transform_ga_sessions(ga_records, ga_last, ga_date)
        run_quality_checks_ga(ga_df_day)

        ga_frames.append(ga_df_day)

        save_manifest(
            {
                "run_started_at": run_started_at,
                "endpoint": "ga-sessions-data",
                "filters": {"date": ga_date},
                "rows_extracted": len(ga_records),
                "rows_transformed": len(ga_df_day),
                "api_version": (ga_last.get("metadata", {}) or {}).get("api_version"),
                "source_dataset": (ga_last.get("metadata", {}) or {}).get("dataset"),
                "pagination": ga_last.get("pagination", {}),
            },
            filename=f"ga_sessions_{ga_date}_manifest.json"
        )

    if ga_frames:
        ga_df_all = pd.concat(ga_frames, ignore_index=True)
    else:
        ga_df_all = pd.DataFrame()

    # final GA duplicate check on combined frame
    if not ga_df_all.empty:
        dup_count_all = int(ga_df_all.duplicated(subset=["date", "fullVisitorId", "visitId"]).sum())
        print("[CHECK] ga_sessions combined duplicate keys:", dup_count_all)

    # Full refresh strategy for submission simplicity (prevents duplicates on rerun)
    load_df_to_bq(
        client=client,
        dataset_id=dataset_id,
        table_name="ga_sessions",
        df=ga_df_all,
        schema=schema_ga_sessions(),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    save_manifest(
        {
            "run_started_at": run_started_at,
            "endpoint": "ga-sessions-data",
            "filters": {"start_date": ga_start_date, "end_date": ga_end_date},
            "dates_processed": ga_dates_processed,
            "rows_extracted_total": ga_total_extracted,
            "rows_transformed_total": int(len(ga_df_all)),
        },
        filename=f"ga_sessions_{ga_start_date}_to_{ga_end_date}_summary_manifest.json"
    )

    print("[INFO] ETL complete.")


if __name__ == "__main__":
    main()