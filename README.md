# klaviyo-snowflake-sync

Incremental extraction of Klaviyo profiles and events into Snowflake, plus an
optional dbt project (`transform/`) that models the raw tables.

The pipeline reads the Klaviyo REST API (`/api/profiles` and `/api/events`) and
MERGEs the results into raw Snowflake tables. Each stream keeps a durable
checkpoint (watermark, active window and pagination cursor) in a state table,
so runs are short, bounded and safe to stop at any point. The next run
continues where the last one stopped.

It exists because a full reload of a large Klaviyo account is slow and runs into
API rate limits, and because a naive "fetch everything since the last max
timestamp" loader can skip or duplicate rows when it is interrupted. This
project keeps the extraction incremental and the loads idempotent.

## Status

This is extracted from a Klaviyo to Snowflake pipeline Linus Tse built and runs for a DTC brand, generalized so it stands alone. The standalone version passes its unit tests (22 Java, 53 Python) but has not yet been run end to end against a live Klaviyo account in this form. Run it against a development schema first, and open an issue if something breaks.

The dbt layer under `transform/` builds and passes its tests on DuckDB with synthetic sample data, and parses for Snowflake, but has not yet been run against a live Snowflake account.

Built by [Greyfield Data](https://www.greyfielddata.com), a Snowflake, dbt, and Sigma consultancy for ecommerce and consumer brands.

## What it loads

| Table | Key | Columns |
| --- | --- | --- |
| `RAW_KLAVIYO_PROFILES` | `ID` | `EMAIL`, `EMAIL_MARKETING`, `SMS_MARKETING`, `UPDATED` |
| `RAW_KLAVIYO_EVENTS` | `METRIC_ID`, `ID` | `PROFILE_ID`, `EVENT_DATETIME` |
| `KLAVIYO_SYNC_STATE` | `JOB_KEY` | checkpoint, lease and status per stream |

`EMAIL_MARKETING` and `SMS_MARKETING` are the profile's
`can_receive_email_marketing` and `can_receive_sms_marketing` consent flags.
The profile request uses a sparse fieldset, so only these fields are
transferred. Event payload properties are not stored; add columns and parsing
in `IncrementalSync.parsePage` if you need them.

## Architecture

```
            cron / scheduler
                  |
   +--------------+----------------+-------------------+
   |              |                |                   |
klaviyo-     klaviyo_batch.py   klaviyo-catchup.sh   check_klaviyo_capacity.py
sync-cycle.sh  (time-boxed run   (back-to-back slices  (reads batch snapshot,
(one slice     of all streams    after an outage or    warns when lag keeps
every 5 min)   to a fixed goal)  for a backfill)       growing)
   |              |                |
   +------+-------+----------------+
          |
   klaviyo-incremental.sh JOB_KEY
          |
   IncrementalSync (Java)  ---- HTTPS ---->  Klaviyo API
          |                                  429 / 5xx -> Retry-After or backoff
          | one transaction per page group:
          |   stage rows -> MERGE -> cursor + watermark + lease check
          v
   Snowflake: RAW_KLAVIYO_PROFILES, RAW_KLAVIYO_EVENTS, KLAVIYO_SYNC_STATE
          ^
   CohortHealth (Java): validates all streams, advances the shared cutoff
```

### Job keys

Each stream is a job key: `profiles`, or `event:<METRIC_ID>` for each metric ID
in `KLAVIYO_EVENT_METRICS`. Every job key has one row in `KLAVIYO_SYNC_STATE`.
Workers refuse job keys that are not configured.

### Java classes (`com.greyfielddata.klaviyo`)

- `SyncPlan`: pure logic. Job keys, query URLs, watermark rules, commit
  grouping, lease length and retry delays. Fully unit tested.
- `IncrementalSync`: one bounded worker run for one job key.
- `CohortHealth`: checks all streams and advances the shared target cutoff
  when every stream has reached it. `--batch-status` prints state as JSON and
  changes nothing.
- `StateBootstrap`: `seed` inserts missing state rows for the configured job
  keys; `inspect` prints state and row counts.
- `SnowflakeSession`: connection from environment variables, key-pair auth,
  session timeouts, and a shutdown hook that rolls back on SIGTERM.

### Runners (`scripts/`)

- `klaviyo-incremental.sh JOB_KEY`: validates the key and starts one worker.
- `klaviyo-sync-cycle.sh`: picks the next job key in a fair rotation (profiles
  between each event stream) from the current five-minute slot, runs it, then
  runs `CohortHealth`. Meant for cron every five minutes.
- `klaviyo_batch.py`: one time-boxed run (default three hours) that brings every
  stream up to a goal fixed at start. Events get short cycles, profiles get one
  long cycle per round. Writes `state/klaviyo-batch-freshness.json`.
- `klaviyo-catchup.sh`: runs sync-cycle slices back to back until a fresh
  cohort completes or its budget ends. Writes a JSON latch on failure.
- `check_klaviyo_capacity.py`: reads the batch snapshot and a short history,
  and warns when the profiles lag grows across consecutive batches, before it
  crosses the freshness limit.

Use either the five-minute cycle or the batch runner as your regular schedule.
The catch-up runner is for recovery. Do not run the batch and catch-up runners
at the same time; both should take the same lock (see below).

## Requirements

- Java 17 or newer and Maven 3.8 or newer
- Python 3.9 or newer (runners and tests; standard library only)
- bash, `flock` and `timeout` (util-linux and coreutils)
- A Klaviyo private API key with read access to profiles and events
- A Snowflake user with key-pair authentication

## Setup

1. Build:

   ```bash
   mvn package
   ```

   This compiles to `target/classes`, runs the unit tests and copies runtime
   jars (Snowflake JDBC, json-simple) to `target/lib`. The scripts use
   `target/classes:target/lib/*` unless `KLAVIYO_CLASSPATH` is set.

2. Create the Snowflake objects. Edit the placeholders first.

   ```
   sql/02_raw_tables.sql   raw tables (run in your database and schema)
   sql/03_sync_state.sql   state table
   sql/01_role.sql         least-privilege role and grants
   ```

3. Configure the environment:

   ```bash
   cp .env.example .env    # then edit .env
   set -a; . ./.env; set +a
   ```

   | Variable | Required | Default | Purpose |
   | --- | --- | --- | --- |
   | `KLAVIYO_API_KEY_FILE` | yes | | file containing only the Klaviyo private API key |
   | `KLAVIYO_EVENT_METRICS` | no | empty | comma-separated metric IDs to sync as event streams |
   | `SNOWFLAKE_ACCOUNT` | yes* | | account identifier (`<YOUR_ACCOUNT>`) |
   | `SNOWFLAKE_JDBC_URL` | no | | full JDBC URL, overrides `SNOWFLAKE_ACCOUNT` |
   | `SNOWFLAKE_USER` | yes | | service user |
   | `SNOWFLAKE_PRIVATE_KEY_FILE` | yes | | PKCS#8 private key file |
   | `SNOWFLAKE_PRIVATE_KEY_FILE_PWD` | no | | passphrase for an encrypted key |
   | `SNOWFLAKE_WAREHOUSE` | yes | | warehouse |
   | `SNOWFLAKE_DATABASE` | no | `RAW` | database |
   | `SNOWFLAKE_SCHEMA` | no | `KLAVIYO` | schema holding all three tables |
   | `SNOWFLAKE_ROLE` | no | `KLAVIYO_PIPELINE_ROLE` | role |
   | `KLAVIYO_MAX_SECONDS` | no | `120` | worker time budget (30 to 1800) |
   | `KLAVIYO_MAX_PAGES` | no | `50` | worker page budget (1 to 2000) |
   | `KLAVIYO_PAGES_PER_COMMIT` | no | `100` | profile pages per transaction (1 to 100) |
   | `KLAVIYO_MAX_NO_PROGRESS_HOURS` | no | `10` | `CohortHealth` flags a stream with no progress for this long |
   | `KLAVIYO_INITIAL_WATERMARK` | no | `2020-01-01T00:00:00Z` | starting point for newly seeded streams |
   | `KLAVIYO_STATUS_DIR` | no | `./state` | snapshots, latches and locks |
   | `KLAVIYO_DEV_SCHEMA` | no | `KLAVIYO_SYNC_DEV` | the only schema where test failpoints are allowed |

   \* unless `SNOWFLAKE_JDBC_URL` is set.

   Klaviyo metric IDs are listed by `GET https://a.klaviyo.com/api/metrics`.
   Pick the metrics you need (for example the email engagement metrics) and put
   their IDs in `KLAVIYO_EVENT_METRICS`.

4. Seed the state table:

   ```bash
   java -cp 'target/classes:target/lib/*' \
     --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
     com.greyfielddata.klaviyo.StateBootstrap seed
   ```

   Run `seed` again after adding a metric ID. Existing rows are never changed.
   A new stream joins the current cohort cutoff, so the cohort will not advance
   until the new stream has caught up from `KLAVIYO_INITIAL_WATERMARK`.

## Running

One worker run, by hand:

```bash
scripts/klaviyo-incremental.sh profiles
scripts/klaviyo-incremental.sh event:<METRIC_ID>
```

Output is one line per event, prefixed `KLAVIYO_`, for example
`KLAVIYO_SYNC_PROGRESS`, `KLAVIYO_SYNC_WINDOW_COMPLETE`, `KLAVIYO_SYNC_INCOMPLETE`
(budget used, resume next run), `KLAVIYO_SYNC_CAUGHT_UP` and `KLAVIYO_SYNC_TIMING`
(aggregate rows, pages and milliseconds). Logs never contain profile IDs,
emails, payloads or cursors. Exit code 75 means another worker holds the lease
for that stream.

Example crontab, choose one regular schedule:

```cron
# Option A: one slice every five minutes
*/5 * * * * cd /opt/klaviyo-snowflake && . ./env.sh && scripts/klaviyo-sync-cycle.sh >> logs/cycle.log 2>&1

# Option B: twice a day, time-boxed
17 1,13 * * * cd /opt/klaviyo-snowflake && . ./env.sh && flock -n state/klaviyo-run.lock python3 scripts/klaviyo_batch.py >> logs/batch.log 2>&1

# Optional: capacity warning after each batch
47 4,16 * * * cd /opt/klaviyo-snowflake && python3 scripts/check_klaviyo_capacity.py --freshness state/klaviyo-batch-freshness.json --history state/klaviyo-lag-history.json
```

`env.sh` stands for whatever loads your environment (for example
`set -a; . ./.env; set +a`). Paths are examples.

Recovery after an outage:

```bash
scripts/klaviyo-catchup.sh
```

It takes `state/klaviyo-run.lock` (same lock as the batch example), runs
slices of up to `KLAVIYO_CATCHUP_SLICE_SECONDS` (default 230) under `timeout`,
and stops when a cohort completes with a cutoff no older than
`KLAVIYO_CATCHUP_MAX_COMPLETED_CUTOFF_AGE_SECONDS` (default 3600), or when
`KLAVIYO_CATCHUP_MAX_SECONDS` (default 12 hours) runs out. Stopping early is
safe. For a long run, start it under a process supervisor with memory and CPU
limits, for example `systemd-run`.

Inspect state:

```bash
java -cp 'target/classes:target/lib/*' \
  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
  com.greyfielddata.klaviyo.StateBootstrap inspect
```

## Transform

`transform/` is a dbt project (dbt-snowflake) that turns the raw tables into
staging views and analysis-ready tables. It only uses the columns the sync
stores. There are no event payload properties, so there is no revenue, campaign
or flow attribution here.

### Models

| Model | Type | What it is for |
| --- | --- | --- |
| `stg_klaviyo__profiles` | view | One row per profile ID (latest by `UPDATED`). Email lowercased and trimmed, empty email as null, consent flags as booleans (null becomes false). |
| `stg_klaviyo__events` | view | One row per event, keyed on `metric_id` and `event_id`, with `metric_name` from the metric map. |
| `dim_klaviyo_profiles` | table | Current state of each profile: email, email domain, email and SMS consent. |
| `fct_klaviyo_events` | incremental | One row per event with metric name, profile, timestamp and date. Reprocesses the last `klaviyo_events_lookback_days` (default 3) on each run. |
| `klaviyo_profile_engagement` | table | Per profile: first and last event, total events, count and last timestamp for Opened Email, Clicked Email and Placed Order, days since last event, and active in last 30, 90 and 365 days flags. |
| `klaviyo_mailable_profiles` | table | Profiles with email consent and an email address. Optionally only those active within N days. |
| `klaviyo_shopify_customer_bridge` | table, disabled | EXAMPLE ONLY. Joins Klaviyo profiles to a Shopify customers table that you load yourself, on normalized email. |

Timestamps are converted to UTC without a time zone. Every model has
descriptions and tests (`unique`, `not_null`, `accepted_values`,
`relationships`) in the `schema.yml` files next to it. Two tests only warn:
`metric_name` not null in `stg_klaviyo__events` (a metric ID is missing from
the map) and the events to profiles relationship (events can arrive before
their profile is synced).

### Metric map setup

The sync stores metric IDs, not names. The models get names from the seed
`transform/seeds/klaviyo_metric_map.csv` (`metric_id,metric_name`). It ships
with placeholder rows that you must replace:

```csv
metric_id,metric_name
REPLACE_WITH_OPENED_EMAIL_METRIC_ID,Opened Email
REPLACE_WITH_CLICKED_EMAIL_METRIC_ID,Clicked Email
REPLACE_WITH_PLACED_ORDER_METRIC_ID,Placed Order
```

1. List your metric IDs with `GET https://a.klaviyo.com/api/metrics` (the
   same IDs you put in `KLAVIYO_EVENT_METRICS`).
2. Replace each placeholder ID and add a row for every other metric you sync.
   If you fill the optional `LOOKUP_KLAVIYO_METRICS` table from
   `sql/02_raw_tables.sql`, you can export it into this file.
3. Keep the names `Opened Email`, `Clicked Email` and `Placed Order` exactly,
   or change the `klaviyo_engagement_metrics` var to the names you use.
4. Run `dbt seed`, then `dbt build --full-refresh` so existing events get the
   new names.

### Variables

Set in `transform/dbt_project.yml` or with `--vars`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `klaviyo_database` | `RAW` | database holding the raw tables |
| `klaviyo_schema` | `KLAVIYO` | schema holding the raw tables |
| `klaviyo_profiles_table` | `RAW_KLAVIYO_PROFILES` | profiles table name |
| `klaviyo_events_table` | `RAW_KLAVIYO_EVENTS` | events table name |
| `klaviyo_engagement_metrics` | Opened Email, Clicked Email, Placed Order | metric names that get count and last-seen columns in `klaviyo_profile_engagement` |
| `klaviyo_activity_windows_days` | `[30, 90, 365]` | activity flag windows |
| `klaviyo_mailable_active_within_days` | `null` | when set, `klaviyo_mailable_profiles` keeps only profiles with an event in this many days |
| `klaviyo_events_lookback_days` | `3` | incremental lookback for `fct_klaviyo_events` |
| `klaviyo_as_of_timestamp` | `null` | fixed reference time for activity flags; null means now (UTC) |
| `klaviyo_enable_shopify_bridge` | `false` | build the example Shopify bridge |
| `shopify_database`, `shopify_schema`, `shopify_customers_table` | `RAW`, `SHOPIFY`, `CUSTOMERS` | location of your Shopify customers table (example) |
| `shopify_customer_id_column`, `shopify_customer_email_column` | `ID`, `EMAIL` | its ID and email columns (example) |

If you change `klaviyo_engagement_metrics` or `klaviyo_activity_windows_days`,
update the column tests in `models/marts/_klaviyo__marts.yml` to match.

### Running

```bash
cd transform
python3 -m venv .venv && . .venv/bin/activate
pip install dbt-core dbt-snowflake
cp profiles.example.yml profiles.yml   # git-ignored; or use ~/.dbt/profiles.yml
# set SNOWFLAKE_ACCOUNT and the DBT_SNOWFLAKE_* variables it reads
dbt seed --target snowflake
dbt build --target snowflake
```

The dbt user needs read access to the raw schema and a schema it can write to
(`DBT_SNOWFLAKE_DATABASE` and `DBT_SNOWFLAKE_SCHEMA`, default
`ANALYTICS.KLAVIYO_MODELS`). The pipeline role from `sql/01_role.sql` is not
meant for this.

Mailable profiles active in the last 90 days:

```bash
dbt build --target snowflake --select klaviyo_mailable_profiles \
  --vars '{klaviyo_mailable_active_within_days: 90}'
```

### Local test run on synthetic data

The `duckdb` target in `profiles.example.yml` runs the whole project locally,
without Snowflake:

```bash
pip install dbt-core dbt-duckdb
dbt build --target duckdb
```

On the duckdb target the models read the `transform/seeds/example_synthetic_*`
seeds instead of the raw tables. These files are SYNTHETIC: made-up IDs
(`SYNTH_...`) and addresses on the reserved `example.com`, `example.org` and
`example.net` domains. They are only used by the duckdb target and are disabled
on Snowflake. To also build the example bridge locally:

```bash
dbt build --target duckdb --vars '{klaviyo_enable_shopify_bridge: true}'
```

The SQL avoids dialect-specific functions where it can and uses dbt
cross-database macros (`dbt.dateadd`, `dbt.datediff`, `dbt.split_part`,
`dbt.concat`). Time zone handling is one small dispatched macro with a
Snowflake and a DuckDB version (`macros/klaviyo_helpers.sql`).

### Shopify bridge (example)

`klaviyo_shopify_customer_bridge` is an example of joining to another source,
not a supported Shopify integration. This repo does not load Shopify. To try
it, load a Shopify customers table, point the `shopify_*` vars at it and set
`klaviyo_enable_shopify_bridge: true`. It matches on lowercased, trimmed email
and is many-to-many, because one email can belong to several profiles or
customers.

## Tests

```bash
mvn test              # Java unit tests (JUnit 5)
pytest tests          # Python and shell runner tests
# or, without pytest:
cd tests && python3 -m unittest
```

For the dbt project, see the local test run under Transform.

No test needs network access, Klaviyo or Snowflake. The Java tests cover the
planning logic, including a simulation of crashes and lost cursors across page
and commit boundaries with many timestamp ties. The Python tests run the shell
scripts against stub `java`, `flock`, `timeout` and `date` binaries, test the
batch runner against a fake runtime, and statically check that the JDBC code
keeps its checkpoint protocol.

To exercise crash recovery against a real Snowflake account, point
`SNOWFLAKE_SCHEMA` at a dev schema equal to `KLAVIYO_DEV_SCHEMA`, create the
tables there, and set `KLAVIYO_FAILPOINT` to `after-merge-before-checkpoint` or
`after-checkpoint-before-commit`. The worker refuses failpoints in any other
schema.

## Design choices

### Watermarks, windows and cursors

Each stream works through 24-hour windows starting at its committed watermark.
Inside a window it follows Klaviyo's `links.next` cursor. Rows, the MERGE, the
new cursor and the new watermark are committed in one Snowflake transaction,
guarded by the stream's lease token and a `VERSION` counter. A crash before the
commit rolls everything back; a crash after it loses nothing. The database
also refuses to move a watermark backwards (`greatest(...)`).

Profiles are requested sorted by `updated` ascending, so after each committed
page group the watermark can advance to the largest `updated` in that group
minus one second. Rows that share that timestamp may continue on the next
page; the one-second overlap means they are read again rather than skipped,
and the ID-keyed MERGE absorbs the repeat. If a group is not in ascending
order or has a row outside the window (for example a profile edited during the
crawl), the watermark stays where it was for that group.

If Klaviyo rejects a saved cursor (HTTP 400, 404 or 410), the worker clears it
and re-queries from the committed watermark. Event streams keep a window-level
watermark and re-read the previous 24 hours of each window, to pick up events
that arrive late.

All streams share a target cutoff (now minus five minutes when the cohort
starts). `CohortHealth` moves every stream to a new cutoff only after all of
them have reached the current one, so the previous cutoff is a consistent
"loaded through" point for downstream jobs.

### Profile `updated` changes on any property change

Klaviyo's profile `updated` timestamp changes when any profile property
changes, including properties this pipeline does not load and properties that
Klaviyo or connected integrations update from activity. A filter on `updated`
is therefore much wider than "email or consent changed". On busy accounts a
large share of the profiles returned can be unchanged in the columns stored
here, and a bulk property update can make a large part of the account
eligible at once.

The pipeline keeps the full `updated` filter on purpose. Narrower filters
(for example only subscribed profiles) would miss unsubscribes, suppressions
and email changes. The costs are handled instead by:

- a sparse fieldset, so each page carries only the fields that are stored;
- grouping up to 100 profile pages per transaction, because a MERGE into the
  full profiles table costs roughly the same for a small or a larger staged
  batch; a crash replays at most one uncommitted group;
- a lagging-capacity check (`check_klaviyo_capacity.py`) that warns when
  profiles arrive faster than they are processed.

The MERGE only overwrites a profile when the incoming `updated` is the same or
newer, so replays and out-of-order pages never regress a row.

### Rate limits

Klaviyo returns HTTP 429 with a `Retry-After` header when a rate limit is hit.
The worker retries 429 and 5xx responses: it waits `Retry-After` seconds
(capped at 60), or uses exponential backoff from one second, capped at 30
seconds, plus 250 to 1000 ms of jitter. Every wait is bounded by the worker's
own time budget, so a throttled run ends cleanly with its checkpoints intact
instead of hanging. Only one worker runs per stream (lease), and the runners
run streams one at a time, which keeps request volume modest. Connect and read
timeouts are 15 and 60 seconds.

### Idempotent loads

Every write is a MERGE on the natural key (`ID` for profiles, `METRIC_ID`
plus `ID` for events), after deduplicating the staged rows. Re-reading a page,
a window overlap, a replay after a crash, or running the same window twice all
produce the same table contents. `sql/migrations/` holds a one-time pattern
for deduplicating tables that were previously loaded append-only: back up with
a zero-copy clone, build deduplicated copies, then `ALTER TABLE ... SWAP WITH`
(the swap is its own rollback).

### Other safeguards

- Pagination URLs from the API are followed only if they are HTTPS on
  `a.klaviyo.com` with no user info or custom port.
- Snowflake identifiers from configuration are validated before use in SQL.
- Sessions set `LOCK_TIMEOUT`, `STATEMENT_TIMEOUT_IN_SECONDS` and
  `ABORT_DETACHED_QUERY`; a shutdown hook rolls back an open transaction on
  SIGTERM.
- Leases are longer than the worker's whole budget, so a slow run cannot lose
  its lease mid-commit. An expired lease can be taken over by the next run.
- Error text stored in `LAST_ERROR` is one line, at most 500 characters, and
  holds no response bodies.

## Limitations

- Profiles deleted in Klaviyo are not removed from `RAW_KLAVIYO_PROFILES`. An
  incremental `updated` query only returns profiles that still exist, so a
  separate reconciliation is needed if you must track deletions.
- Only the fields listed above are stored.
- The API revision is pinned in `SyncPlan.API_REVISION`. Check Klaviyo's
  changelog before changing it.

## License

MIT. See [LICENSE](LICENSE).
