# Where MechanicalFinder's data comes from

MechanicalFinder (Tire_Quote_Tool, `src/MechanicalFinder.jsx`) combines two
licensed datasets. Both live in the Tire Finder Supabase project
(`vzsitlasfekjkvsaukmh`), and both are loaded by this service. **Neither one
refreshes by itself.** Someone has to run each load by hand.

| What the CSA sees | Dataset | Tables | Loaded by |
|---|---|---|---|
| Year / make / model / submodel / engine picker | AutoCare **VCdb** (the ACES vehicle database) | `vcdb_*` (14 tables) | `POST /sync/vcdb` (manual file upload) |
| Labor operations and hours | MOTOR **GEN4.5 Mechanical EWT** (Estimated Work Times) | `ewt_labor`, `ewt_applications`, `ewt_applications_vcdb_attribute_xref` | `POST /sync/ewt` (pulls from MOTOR SFTP) |

The datasets are joined on VCdb `base_vehicle_id`. If VCdb has a vehicle and
EWT doesn't, the vehicle appears in the picker but returns no labor. That's
how the 2025 Jetta showed up in September 2026.

## MOTOR EWT (labor times)

- **Source:** MOTOR SFTP `delivery.motor.com`, user `revyourcause_MIS`, folder
  `/ECommerce_Data/GEN4.5_MechLabor/`. The file is named
  `Mechanical_EWT_ACES_YYYYMMDD.zip`. The password is in Render as
  `MOTOR_FTP_PASSWORD`. This account also delivers the weekly TireTechSmart
  file (`/Specifications_Data/TireTech/`).
- **How to load:** `POST https://tire-sync.onrender.com/sync/ewt` with header
  `X-API-Key: $SYNC_API_KEY`. The service picks the newest dated zip on the
  SFTP, so you don't need to download anything.
- **Downtime:** the sync TRUNCATES all three EWT tables, commits, and then
  COPYs one make at a time. The load takes about 20 minutes, and **MechanicalFinder
  returns no labor for that whole time.** If the load fails partway, the tables
  stay partly or completely empty until a successful rerun. Run it after
  hours.
- **Not scheduled.** The code comments say "trigger monthly via Zapier", but
  the sync log shows only two loads have ever happened, both run by hand.

Load history (`tire_data_sync_log`, `sync_type = 'motor_ewt'`):

| Loaded | MOTOR file | Rows |
|---|---|---|
| 2026-03-18 | `Mechanical_EWT_ACES_20260113.zip` | ~72.9M |
| 2026-04-17 | `Mechanical_EWT_ACES_20260330.zip` | ~72.9M |

Coverage from the 20260330 file: model-year 2025 is only partly covered, and
2026 is essentially absent. As of 2026-09-21 the SFTP has
`Mechanical_EWT_ACES_20260804.zip` (posted 2026-08-04), which hasn't been
loaded yet.

## AutoCare VCdb (vehicle picker)

- **Source:** download it manually from **autocarevip.com** (AutoCare
  Association subscription). Use the NA / LDMDHDPS / enUS **MySQL** dump, e.g.
  `AutoCare_VCdb_NA_LDMDHDPS_enUS_MySQL_20260226.sql`. It is latin-1
  encoded.
- **How to load:**
  ```
  curl -X POST https://tire-sync.onrender.com/sync/vcdb \
       -H "X-API-Key: $SYNC_API_KEY" \
       -F "sql_file=@AutoCare_VCdb_NA_LDMDHDPS_enUS_MySQL_YYYYMMDD.sql"
  ```
  Upload the unzipped `.sql` file.
- **Intended cadence:** monthly. A daily delta sync through the AutoCare API
  (`AUTOCARE_VIP_TOKEN`) was planned but has never been built.

Load history: one load, **2026-03-21**, from the 20260226 dump (1.36M rows).
The sync log only records "AutoCare VCdb MySQL dump", not the file date.

## Archive

Keep a copy of every file we load in the Drive folder
[Jiffy Lube Ops - Data Automation Project](https://drive.google.com/drive/folders/1JrwVF3fr8PGItV2TMppYFAY_F5FV18dj).
The folder also holds the EWT format spec
(`MOTOR-GEN4.5-MechanicalEWT-CDK(v2 0).pdf`). The 20260113 EWT file is
archived there. The 20260330 EWT file and the 20260226 VCdb dump were never
archived.

MOTOR drops a marker file next to each EWT zip (e.g.
`Q2_GEN4.5 VCdb - 04-30-2026.txt` next to the 20260804 file). The marker names
the VCdb release the EWT file was built against. Load a VCdb at least that
recent.

To download from the SFTP with FileZilla on a Mac, save to a folder like
`~/motor-files`. macOS blocks FileZilla from Downloads, Desktop and Documents
unless you grant it access in Privacy & Security → Files & Folders.

## Refresh checklist

1. Log into the MOTOR SFTP and check for a `Mechanical_EWT_ACES_*` file newer
   than the last one loaded (see the table above).
2. Refresh VCdb **first** (autocarevip.com → `/sync/vcdb`), so the vehicles
   in the new EWT file are already in the picker.
3. After hours, run `/sync/ewt`.
4. Confirm the load: `select * from tire_data_sync_log where sync_type in
   ('motor_ewt','autocare_vcdb') order by id desc limit 3;` should show a
   `completed` row with the new file name.
5. Spot-check a vehicle that was missing before, e.g. the 2025 VW Jetta
   (base_vehicle_id 177696): `select count(*) from ewt_applications where
   base_vehicle_id = 177696;` should be above 0.
6. Add the new load to the history tables in this doc, and copy the files you
   loaded into the Drive archive.
