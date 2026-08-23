# `raw_sentix_data`: Sentix and Romeo

This schema stores licensed weekly Sentix survey observations and versioned
derived signals. The dashboard contract is `raw_sentix_data.signal_latest`.

## Runtime configuration

- PostgreSQL Airflow Connection: `keynum-central-pg`.
- Vendor Airflow HTTP Connection: `sentix_api`.
  - Host: `https://api.sentix.de`
  - Login: Sentix userid
  - Password: Sentix passcode
  - Extra: `{"token": "..."}`
- Alert recipients: Airflow Variable `sentix_alert_emails`, stored as a JSON
  list of addresses.
- SMTP: configure and test Airflow connection `smtp_default` before enabling
  the DAG.

Never put credentials, request bodies, the legacy dump, or the legacy workbook
in Git or Airflow logs. The current shared credential has exposure history and
must be rotated in coordination with the legacy parallel run.

## Initial migration

1. Before changing the legacy host, export `kndev.sentix` read-only, restrict
   file permissions, and record a SHA-256 checksum. Store it outside Git.
2. Obtain `romeo_sp500.xlsx` from the legacy host and store it outside Git.
3. Apply `migrations/20260823_add_sentix_romeo.sql`.
4. Install `requirements-sentix.txt` into every scheduler/worker environment
   and restart those processes.
5. Trigger `sync_sentix_romeo_weekly_dag` with `{"mode": "backfill"}`.
6. Import only retired codes:

   ```bash
   /opt/airflow/venv/bin/python -m scripts.sentix_migration \
     import-retired --legacy-dump /secure/path/kndev_sentix.tsv
   ```

7. Run all acceptance checks:

   ```bash
   /opt/airflow/venv/bin/python -m scripts.sentix_migration \
     data --legacy-dump /secure/path/kndev_sentix.tsv
   /opt/airflow/venv/bin/python -m scripts.sentix_migration \
     signal --xlsx /secure/path/romeo_sp500.xlsx
   /opt/airflow/venv/bin/python -m scripts.sentix_migration \
     retired --legacy-dump /secure/path/kndev_sentix.tsv
   ```

Keep the legacy Sunday email running and compare it with `signal_latest` for at
least three consecutive Sundays before cutover.

## Signal semantics

Only `romeo.sp500` / `romeo-A-1` is published. The stored value is unlagged:

- `1.0`: fully long
- `0.5`: half long
- `0.0`: flat
- `-1.0`: fully short in the verified legacy implementation

Do not use `-1` for a real allocation until Hans confirms whether the product
intent is short or out of market. The consumer must also choose and document
its execution lag. A rule change requires a new `spec_version`; never rewrite
or relabel an older version.

## Monitoring

- DAG: `sync_sentix_romeo_weekly_dag`, Sunday 20:00 Europe/Berlin.
- Feed health: `raw_sentix_data.sentix_health`.
- Signal health: `raw_sentix_data.signal_health`.
- Latest dashboard value: `raw_sentix_data.signal_latest`.

For vendor `backfill` and `incremental` runs, `sentix_load_run.codes_seen`
means the number of codes present on the response's latest observation date.
Likewise, `sentix_series.is_active` means that the code is present on that
latest date; older observations remain stored when a code becomes inactive.
For a `legacy_import` run, `codes_seen` is the number of retired codes imported.

The DAG retries twice and sends a credential-safe email after final task
failure. An empty `sentix_alert_emails` variable is a deployment blocker.
