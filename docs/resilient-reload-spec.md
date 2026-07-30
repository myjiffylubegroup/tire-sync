# Resilient reload for `tire_inventory` — spec (PROPOSAL, not yet applied)

**Status:** draft for review · **Author:** drafted with Claude · **Date:** 2026-07-30
**Trigger:** 2026-07-30 outage — USAutoForce delivered a structurally-valid feed with
`QuantityAvailable` and `RetailPrice` zeroed on all ~11.6k parts. The destructive
truncate-then-reload wiped the last good inventory, and TireFinder quoted nothing
for hours. The same-day guardrail (data-quality gate) stops the *all-zeros* case,
but the underlying load pattern is still "destroy, then hope the new data is good."

**Goal:** a bad, partial, or failed load can **never** leave `tire_inventory` empty
or corrupted. Readers always see the last known-good data. Failed loads alert but
cause **zero downtime**.

**Scope of this doc:** `tire_inventory` only (tire quoting — highest revenue risk).
The same pattern later applies to the other externally-fed full-reload tables
(`tt_smart_vehicles` next). Per the architecture rules, tire tables are **PCJL
monorepo** (`~/Developer/supabase`), not greets-core — so the DDL migration lands
there, and the ETL change lands in `tire-sync`.

---

## 1. Current flow (the weakness)

`insert_usventure_inventory()` in `app.py`:

```python
supabase.table('tire_inventory').delete().neq('created_at', '1900-01-01').execute()  # wipe FIRST
for batch in batches:
    supabase.table('tire_inventory').insert(batch).execute()                          # then refill
```

Two structural problems:
1. **Destroy-first.** The live table is emptied before the new data is known-good.
   Today's gate raises *before* this function, so it helps — but any load that
   passes the gate and then fails partway (batch 3 of 6 errors, network drop,
   Render restart) leaves the live table **half-loaded**, and there is no copy to
   fall back to.
2. **Not transactional.** There is a window where readers see an empty or partial
   table on every single normal load.

## 2. Target flow — stage → validate → atomic swap → snapshot

```
download + parse CSV                      (unchanged)
   │
   ▼
validate prepared_records IN MEMORY       (existing gate + new anomaly checks)
   │  fail ─► raise DataQualityError ─► live table UNTOUCHED, alert, exit  ◄── no downtime
   │  pass
   ▼
load into tire_inventory_staging          (psycopg2 COPY — fast, like EWT)
   │
   ▼
atomic swap in ONE transaction:
   BEGIN;
     TRUNCATE tire_inventory;
     INSERT INTO tire_inventory SELECT * FROM tire_inventory_staging;
   COMMIT;                                 (readers see old rows until COMMIT, then new;
   │                                        any error ► ROLLBACK ► old data intact)
   ▼
refresh tire_inventory_last_good FROM staging   (validated snapshot, for manual rollback)
```

Key properties:
- **Never empty:** MVCC means readers see the old rows right up to `COMMIT`, then the
  new rows. No empty/partial window, even on a normal load.
- **All-or-nothing:** a failure mid-swap rolls back to the prior good data.
- **Last-good retained:** `tire_inventory_last_good` always holds the most recent
  *validated* load. Instant manual rollback:
  `INSERT INTO tire_inventory SELECT * FROM tire_inventory_last_good;`
- **Reader impact: none.** `tire_inventory` keeps its name, indexes, grants, and the
  `search_tire_inventory_v2` RPC. The edge function and frontend are untouched.

> Why TRUNCATE+INSERT-from-staging inside a txn rather than `ALTER TABLE … RENAME`
> swap: rename swaps are faster but entangle indexes, grants, and the RPC's
> table dependency, and risk a reader hitting a half-renamed state. Copying ~11.6k
> rows staging→live inside a transaction is sub-second and keeps table identity
> stable. Simplicity wins here.

## 3. Validation (what "good" means)

Runs in memory on `prepared_records`, **before** any DB write.

**Hard gate (already shipped 2026-07-30):**
- `len(records) >= MIN_USVENTURE_RECORDS` (5000)
- `stock_pct >= 2%` **and** `retail_pct >= 25%`  ← today's zeroed feed = 0% / 0%

**New — anomaly checks vs trailing good loads** (catch partial corruption the hard
gate misses):
- Row count within ±X% of the trailing median of the last N *good* loads.
- `% with stock` and `% with retail` not down more than Y points vs trailing median.
- `avg(cost)` within ±Z% of trailing median.
- Thresholds start generous and are tuned from the per-load stats we log (below).

Any check fails ► `raise DataQualityError` ► live table untouched ► 🚨 alert ►
HTTP 200 `{status: rejected}` (Zapier treats as handled, no retry-storm — already
shipped).

## 4. Schema changes (PCJL monorepo migration)

New objects, identical column layout to `tire_inventory`:
- `tire_inventory_staging` — scratch load target (truncated each run).
- `tire_inventory_last_good` — last validated snapshot (rollback source).

Add per-load stats to `tire_data_sync_log` for anomaly history (nullable, backfill-safe):
- `parts_with_stock int`, `parts_with_retail int`, `avg_cost numeric`.

Grants: staging/last_good need **no** external grants (service-role writer only);
readers never touch them.

## 5. ETL changes (`tire-sync/app.py`)

- Rewrite `insert_usventure_inventory()` to: COPY into staging (psycopg2, reuse
  `get_db_conn()` / `SUPABASE_DB_URL`), then run the swap transaction, then refresh
  last_good. Replaces the supabase-py REST delete+insert.
- Add anomaly checks to the gate in `sync_usventure_data()`; write the three stat
  columns into the sync-log row.
- Keep the in-memory gate exactly where it is (reject before touching the DB at all).

## 6. Rollout (per the rules: versioned, template→PCJL, each step approved)

1. **Migration** in `~/Developer/supabase` creating the two tables + 3 log columns.
   Show DDL, approve, apply to PCJL. (Create tables BEFORE deploying code.)
2. **Deploy** `tire-sync` app.py change to Render.
3. `tire_inventory_last_good` self-populates on the first validated load (today's
   live data is zeroed, so it stays empty until a good feed lands — expected).
4. Backfill the 3 log stat columns lazily (nulls are fine for history).

**Fail-safe during rollout:** the code checks the staging/last_good tables exist and
falls back to a plain transactional truncate+insert if not, so a code/migration
ordering slip can't brick the sync.

## 7. Effort / risk

- ~½ day. Low risk: readers and the RPC are untouched; the change is confined to how
  the writer stages and commits.
- Net effect: today's failure mode — *bad feed → empty table → lost sales* — becomes
  *bad feed → keep serving last-good inventory → one alert email → nobody at the
  stores notices.*

## 8. Later (not in this pass)
- Apply the same pattern to `tt_smart_vehicles`, then MOTOR EWT / VCdb.
- Freshness heartbeat: alert if no *validated* load in > N hours.
- Confirm Supabase PITR is enabled on PCJL as the whole-DB disaster-recovery catch-all.
