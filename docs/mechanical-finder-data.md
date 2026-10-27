# Where MechanicalFinder's data comes from

MechanicalFinder (Tire_Quote_Tool, `src/MechanicalFinder.jsx`) combines two
licensed datasets. Both live in the Tire Finder Supabase project
(`vzsitlasfekjkvsaukmh`), and both are loaded by this service. **Neither one
refreshes by itself.** Someone has to run each load by hand.

| What the CSA sees | Dataset | Tables | Loaded by |
|---|---|---|---|
| Year / make / model / submodel / engine picker | AutoCare **VCdb** (the ACES vehicle database) | `vcdb_*` (14 tables) | `POST /sync/vcdb` (manual file upload) |
| Labor operations and hours | MOTOR **GEN4.5 Mechanical EWT** (Estimated Work Times) | `ewt_labor`, `ewt_applications`, `ewt_applications_vcdb_attribute_xref` | `POST /sync/ewt` (pulls from MOTOR SFTP) |
| Labor MOTOR doesn't have (shown with a "Shop estimate — not MOTOR" note) | Our own shop estimates | `ewt_labor_supplement` | SQL by hand (see [Supplemental labor](#supplemental-labor)) |

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
- **Progress:** `curl` prints nothing until the load finishes. Makes load in
  alphabetical order, so `select make_name, count(*) from ewt_labor group by 1`
  shows how far along it is. `GET /status` (no key needed) shows the run's
  status.
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
| 2026-09-21 | `Mechanical_EWT_ACES_20260804.zip` | ~73.8M (19.5 min) |

The 20260804 file raised model-year 2025 coverage from 278 to 340 VCdb base
vehicles and 2026 coverage from 1 to 58. That added the 2025 and 2026 VW Jetta,
which were missing before. MOTOR's coverage of the newest model years grows
with each release, so check for a new file every quarter.

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
and 20260804 EWT files are archived there. The 20260330 EWT file and the 20260226 VCdb dump were never
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
7. Check whether the new file filled any of the gaps in `ewt_labor_supplement`
   (see the next section). If it did, deactivate those supplement rows.

## Supplemental labor

Some generations are thin in MOTOR's own file, not in our load, so
reloading won't fix them. Example: 2011–2018 Ram 2500/3500 have only five
BRAKES operations (front axle brakes R&R, and no rear axle, rotor or caliper).
2010 and 2019+ are complete. Other thin ones found 2026-10-27, not yet
supplemented: 2014–15 Audi RS7, 2012–15 Honda Crosstour, 2013–16 Lincoln MKZ
and 2015–16 Mercedes C300.

`ewt_labor_supplement` (migration `ewt_labor_supplement`, applied 2026-10-27;
source in `supabase/sql/ewt_labor_supplement.sql`) holds shop-estimated
operations for those gaps.

- **The EWT sync doesn't touch it.** `/sync/ewt` truncates its three tables by
  name, and this table isn't one of them.
- **How it reaches MechanicalFinder:** `ewt_labor_tree` and `ewt_labor_search`
  UNION in active rows for the base vehicle. The `ewt-labor-search` edge
  function and the frontend didn't change.
  - Each row comes back as `mechanical_estimating_id = -supplement_id`.
    MOTOR ids are positive, so the two can't collide, and the cart, the React
    keys and the quote save path treat it like any MOTOR row.
    `mechanical_quote_items` has no FK on that column.
  - `labor_price` is computed from `mechanical_labor_rate`, like MOTOR rows.
  - The footnote must start with `Shop estimate — not MOTOR` (a CHECK
    enforces it). It shows under the line in MechanicalFinder and on the
    saved quote.
- **Filters:** `engine_config_id` NULL means every engine on the base vehicle.
  Drivetrain markers in `motor_db_description` (4WD, RWD …) filter it the same
  way they filter MOTOR rows. There's no VCdb attribute xref, so SubModel and
  other Tier 1 filters don't apply. If an operation only fits one submodel,
  say so in the description.
- **UI conventions, same as MOTOR:** `is_additional_operation = true` gives
  the ADD-ON badge and indent. The word "Each" in `motor_db_description` turns
  on the EACH badge and the quantity stepper.
- **Hours need sign-off.** Fill `source_note` (where the figure came from)
  and `approved_by` on every row. Prefer MOTOR's figure for the neighbouring
  generation, in MOTOR's own wording.

### What's seeded

2011–2018 Ram 2500 (base vehicles 97150, 118690, 124258, 128169, 130184,
137051, 140918, 145143) and 3500 (97151, 118694, 124259, 128212, 130185,
137052, 140917, 145144). All rows are BRAKES › BRAKE COMPONENTS, copied from
MOTOR's 2010 Dodge Ram and approved by Sean on 2026-10-27:

| Description | Hours | Add-on |
|---|---|---|
| REAR AXLE; Ram 2500, 3500 (brakes R&R) | 0.9 | no |
| To R&I Or R&R Rotor, Add; Front; Each | 0.2 | yes |
| To R&I Or R&R Rotor, Add; Rear; w/o Dual Rear Wheels; Each | 0.2 | yes |
| To R&I Or R&R Rotor, Add; Rear; w/Dual Rear Wheels; Each (3500 only) | 0.6 | yes |
| To R&R Caliper, Add; Front; Each | 0.2 | yes |
| To R&R Caliper, Add; Rear; Each | 0.2 | yes |

MOTOR's 2019+ figures (rotor and caliper R&R both sides, 1.2 hr each) were not
used, because they're standalone jobs that include pulling the wheel. Stacked
on the 0.9 brake job, they'd charge for it twice: a rear pads + rotors +
calipers job would come to 3.3 hr instead of 1.7.

### Add a gap

1. Confirm the gap is in MOTOR, not in our load: compare the operation count
   for the base vehicle against the generations on either side.
   ```sql
   select a.base_vehicle_id, count(distinct a.mechanical_estimating_id)
   from ewt_applications a join ewt_labor l using (mechanical_estimating_id)
   where a.base_vehicle_id in (...) and l.motor_db_section = 'BRAKES'
   group by 1;
   ```
2. Pull the neighbouring generation's rows (group, subgroup, operation,
   qualifier, description, hours) to copy their shape.
3. Get Sean's OK on the hours.
4. Insert the rows:
   ```sql
   insert into ewt_labor_supplement
     (base_vehicle_id, motor_db_section, motor_db_group, motor_db_subgroup,
      motor_db_operation, qualifier_description, motor_time,
      is_additional_operation, motor_db_description, source_note, approved_by)
   values (...);
   ```
   Leave `motor_db_footnote` at its default, or append to it (for example
   `'Shop estimate — not MOTOR. Does not Include: Bleed Master Cylinder.'`).
5. Verify:
   `select * from ewt_labor_search(<base_vehicle_id>, '<SECTION>', null, null, null, <engine_config_id>, <vehicle_id>) where mechanical_estimating_id < 0;`
   The rows should come back with `labor_price`.

### Retire a row

Once MOTOR covers an operation, set `active = false` on the supplement row.
Don't delete it, because saved quotes may still carry its negative id.
