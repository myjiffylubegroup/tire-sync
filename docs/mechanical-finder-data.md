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
7. Rerun the gap audit ([Find gaps](#find-gaps)) and check whether the new
   file filled any of the gaps in `ewt_labor_supplement`. If it did,
   deactivate those supplement rows.

## Supplemental labor

Some generations are thin in MOTOR's own file, not in our load, so
reloading won't fix them. Example: 2011–2018 Ram 2500/3500 have only five
BRAKES operations (front axle brakes R&R, and no rear axle, rotor or caliper).
2010 and 2019+ are complete. Supplemented so far: that Ram, 2013–2020 Lincoln
MKZ, 2012–2015 Honda Crosstour, 2015–2016 Mercedes C300, 2014–2015 Audi
RS7 and 2018–2023 Chevrolet Traverse (brakes), plus front ball joints and
fluid changes on several platform twins and the gap audit's second batch
(see [What's seeded](#whats-seeded)).
To find more, run the [gap audit](#find-gaps).

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
- **The description names the car it's attached to, never the source.** It
  shows on the customer's quote, so a Traverse row reads "REAR AXLE;
  Traverse", not MOTOR's "REAR AXLE; Enclave". Swap the model name and keep
  the rest of MOTOR's wording; the source goes in `source_note`. Copied fluid
  rows also carry the source's drive type ("Telluride; AWD") so a FWD car
  doesn't get offered it. Every earlier row was relabelled on 2026-10-27
  (`supabase/sql/ewt_labor_supplement_relabel.sql`).

### What's seeded

**Ram 2500/3500 2011–2018:** 2500 (base vehicles 97150, 118690, 124258, 128169, 130184,
137051, 140918, 145143) and 3500 (97151, 118694, 124259, 128212, 130185,
137052, 140917, 145144). All rows are BRAKES › BRAKE COMPONENTS, copied from
MOTOR's 2010 Dodge Ram and approved by Sean on 2026-10-27:

| Description | Hours | Add-on |
|---|---|---|
| REAR AXLE; Ram 2500 (or Ram 3500) (brakes R&R) | 0.9 | no |
| To R&I Or R&R Rotor, Add; Front; Each | 0.2 | yes |
| To R&I Or R&R Rotor, Add; Rear; w/o Dual Rear Wheels; Each | 0.2 | yes |
| To R&I Or R&R Rotor, Add; Rear; w/Dual Rear Wheels; Each (3500 only) | 0.6 | yes |
| To R&R Caliper, Add; Front; Each | 0.2 | yes |
| To R&R Caliper, Add; Rear; Each | 0.2 | yes |

MOTOR's 2019+ figures (rotor and caliper R&R both sides, 1.2 hr each) were not
used, because they're standalone jobs that include pulling the wheel. Stacked
on the 0.9 brake job, they'd charge for it twice: a rear pads + rotors +
calipers job would come to 3.3 hr instead of 1.7.

**Lincoln MKZ 2013–2020** (base vehicles 124247, 127778, 130057, 134765,
140517, 145332, 147391, 150500). MOTOR applies only a brake vacuum sensor to
these. Its CD4 Ford Fusion rows are labelled "Fusion, MKZ" and are identical
for 2013–2020, so they're copied verbatim, with the label naming the MKZ
(seeded 2026-10-27):

| Description | Hours | Add-on |
|---|---|---|
| BOTH AXLES; MKZ | 1.6 | no |
| FRONT AXLE; MKZ | 0.9 | no |
| REAR AXLE; MKZ | 0.9 | no |
| To R&I Or R&R Rotor, Add; Front; Both Sides / One Side | 0.4 / 0.2 | yes |
| To R&I Or R&R Rotor, Add; Rear; Both Sides / One Side | 0.4 / 0.2 | yes |
| To R&R Caliper, Add; Each | 0.2 | yes |

**Honda Crosstour 2012–2015** (base vehicles 118775, 123281, 128095, 130181).
MOTOR applies only the VSA switch. Copied from MOTOR's 2010–2011 Accord
Crosstour (same generation; VCdb renamed the model for 2012):

| Description | Hours | Add-on |
|---|---|---|
| FRONT AXLE; Crosstour | 0.9 | no |
| REAR AXLE; Crosstour | 0.9 | no |
| To R&I Or R&R Rotor, Add; Each | 0.2 | yes |
| To R&R Caliper, Add; Each | 0.2 | yes |

Source: `supabase/sql/ewt_labor_supplement_mkz_crosstour.sql`. The same MOTOR
sources also have hose, caliper overhaul, rotor refacing and parking brake
add-ons, which weren't seeded.

**Mercedes-Benz C300 2015–2016** (W205; base vehicles 131072, 136661). MOTOR
applies only four ABS speed sensor ops. Copied from MOTOR's 2017–2019 C300
(same W205 generation, identical every year). Source:
`supabase/sql/ewt_labor_supplement_c300.sql`.

| Description | Hours | Add-on |
|---|---|---|
| FRONT AXLE; C300 | 0.9 | no |
| REAR AXLE; C300 | 0.9 | no |
| To R&I Or R&R Rotor, Add; Each | 0.2 | yes |
| To R&R Caliper, Add; Each | 0.2 | yes |

MOTOR's 2017+ C300 also prices rotor and caliper as standalone jobs (1.2 hr
both sides, 1.4 for rear rotors). Those weren't used, for the same
double-counting reason as the Ram.

**Audi RS7 2014–2015** (C7; base vehicles 127035, 130085). MOTOR applies only
the vacuum pump. Copied from MOTOR's 2016–2018 RS7. Source:
`supabase/sql/ewt_labor_supplement_rs7.sql`.

| Description | Hours | Add-on |
|---|---|---|
| FRONT AXLE; RS7 | 0.9 | no |
| REAR AXLE; RS7 | 0.9 | no |
| To R&I Or R&R Rotor, Add; Each | 0.2 | yes |
| To R&R Caliper, Add; Each | 0.2 | yes |

MOTOR's C7 sources disagree on the rotor add-on: the 2016–18 RS7 says +0.2
each, and the 2014–15 S7 and A7 say +0.3. Sean picked +0.2, the RS7's own
figure, on 2026-10-27.

**Chevrolet Traverse 2018–2023** (base vehicles 143481, 146475, 148805,
152895, 156732, 161161). MOTOR has the standalone rotor and caliper jobs but
no axle brake job, so pads couldn't be quoted. Copied from MOTOR's Buick
Enclave of the same years (same C1XX platform; MOTOR's standalone
rotor/caliper hours are identical on both, and its 2017 rows were labelled
"Enclave, Traverse"). Found by the gap audit; about 23 PCJL visits a month.
Source: `supabase/sql/ewt_labor_supplement_traverse.sql`.

| Description | Hours | Add-on |
|---|---|---|
| FRONT AXLE; Traverse | 1.1 | no |
| REAR AXLE; Traverse | 1.1 | no |
| To R&I Or R&R Rotor, Add; Each | 0.2 | yes |
| To R&R Calipers, Add; Each | 0.2 | yes |

**Platform twins** (`supabase/sql/ewt_labor_supplement_twins.sql`). In these
cases MOTOR gives one twin an operation and leaves it off the other, even
though the twins' other hours match. The rows are copied from the twin's
MOTOR rows for the same year at insert time, word for word (except a fluid
row's label, which names the target and its drive type), with MOTOR's
footnote after the `Shop estimate — not MOTOR` prefix. The script skips a
target that already has the operation, so it's safe to rerun.

| Target | Years | Copied from | What |
|---|---|---|---|
| Jeep Grand Cherokee | 2016–2021 | Dodge Durango | Ball joint add-ons on the lower control arm (0.3 one / 0.2 each additional). 2011–15 already had them. |
| Ford Escape | 2021–2025 | Ford Bronco Sport | Same, lower arm |
| Chevrolet Tahoe | 2021–2025 | GMC Yukon | Same, lower and upper arm |
| Chevrolet Suburban | 2021–2025 | GMC Yukon XL | Same, lower and upper arm |
| Chevrolet Equinox | 2025 | GMC Terrain | Same, lower arm |
| Hyundai Sonata | 2025 | Kia K5 | Same, lower arm, plus rear differential fluid 0.5 |
| ~~Kia Telluride~~ | 2020–2025 | Hyundai Palisade | Transfer case fluid 0.5. **Deactivated 2026-10-27**: MOTOR already has it, filed under section TRANSFER CASE, which the first audit didn't read |
| Ford Edge | 2016–2018 | Lincoln MKX | Rear differential fluid 0.7 |

**Gap audit batch 2** (`supabase/sql/ewt_labor_supplement_gap_batch2.sql`,
56 rows). These were the audit's open questions; Sean approved each one on
2026-10-27.

| Target | Years | Source | What | PCJL visits/mo |
|---|---|---|---|---|
| Toyota 4Runner | 2010–2023 | Lexus GX460, same year (J150) | Transfer case fluid 0.6, "4Runner; 4WD" | ~41 |
| Ford Escape | 2013–2019 | The Escape's own MOTOR front rotor add-on, mirrored to the rear | Rear rotor add-on 0.3 one side / 0.6 both | ~30 |
| Toyota Camry | 2012–2014 | 2015 Camry (same XV50 generation) | Inner tie rod 1.0 both / 0.7 one | ~28 |
| Toyota Highlander | 2008–2013 | Lexus RX350, same year | Transfer case fluid 0.6, "Highlander; AWD" | ~17 |
| Honda Pilot | 2009–2015 | Honda Ridgeline knuckle add-on (2015 uses 2014) | Lower ball joint add-on 0.4 one / 0.6 both | ~15 |
| Ford Escape | 2020 | 2021 Ford Bronco Sport (no 2020 exists) | Ball joint add-on 0.3 / 0.2 | ~6 |

Two of these aren't verbatim same-year copies, which is why they went to Sean:

- **Escape rear rotor.** No twin has it, MKC included.
- **Camry inner tie rod.** MOTOR's own 2012–14 outer tie rod is 1.4 hr,
  against 0.7 for 2015–17.

Deliberately not filled:

- **RAV4 2006–12 inner tie rod.** The neighbouring years only price it as an
  add-on to an 8.5 hr rack R&I.
- **Highlander 2008–13 rear sway link.** The RX and Highlander sway bar hours
  differ.

**Toyota Tundra 2007–2021 and Sequoia 2008–2022: transfer case fluid**
(`supabase/sql/ewt_labor_supplement_tundra_sequoia.sql`, 30 rows). MOTOR has
no figure for these generations. Its only ones are the 2022–23 Tundra (0.8)
and 2023 Sequoia (0.5). Sean chose "0.8 from 2022+ Tundra" for both models.
- **Shape:** a base op, copied from MOTOR's 2022 Tundra row, so it sits in
  MOTOR's own section TRANSFER CASE › TRANSFER CASE › FLUID › CHANGE.
- **Label:** "Tundra; 4WD" / "Sequoia; 4WD", so a 2WD vehicle_id doesn't see it.
- **Source:** the footnote and `source_note` say "2022 Tundra (next gen)".

**Ford Escape 2022–2025: transfer case fluid, 0.8 h**
(`supabase/sql/ewt_labor_supplement_escape_tc.sql`, 4 rows). MOTOR has the
Escape's transfer case R&R and actuator jobs, but no fluid change in any
section.
- **The twins disagree:** Bronco Sport 1.0, Maverick 0.8, and the Corsair has
  none. The Escape's own rear diff fluid (0.6) matches the Maverick's, so Sean
  picked "0.8 hr from Maverick" on 2026-10-27.
- **Shape:** copied from the same-year Maverick into section TRANSFER CASE.
- **Label:** "Escape; AWD", so FWD vehicle_ids don't see it.

### Find gaps

`supabase/sql/ewt_labor_gap_audit.sql` (migrations `ewt_gap_audit_1…4`,
applied 2026-10-27) is a saved audit of MOTOR's holes, ranked by how often
PCJL sees the car. **Rerun it after every EWT load** and after adding
supplement rows:

```sql
set statement_timeout = '15min';
select public.ewt_gap_audit_refresh();   -- about a minute; returns "N base vehicles audited, M gaps"
select * from public.ewt_gap_audit;       -- busiest first
```

Run it from the SQL editor or the Supabase MCP, not through PostgREST,
because it takes longer than the API's 8-second limit.
`ewt_gap_audit_refresh(p_months, p_min_visits_12m)` defaults to the last 12
months and 36 visits a year (3 a month).

How it works:

- **Traffic.** Last 12 months of non-void PCJL invoices, counted by VIN, not
  `invoices.vehicle_id` (that is 0 on about a third of rows). DDS's
  year/make/model text ("FORD TRUCKS" / "F250 HD PICKUP") is mapped to a VCdb
  `base_vehicle_id`. About 99.5% of visits map. The result is stored in
  `ewt_gap_traffic`.
- **Core jobs, not raw operations.** `ewt_gap_job_codes()` maps each MOTOR (and
  supplement) row to the ~35 jobs a CSA actually quotes in BRAKES, STEERING,
  FRONT/REAR SUSPENSION, COOLING SYSTEM and MAINTENANCE & LUBRICATION:
  front brake job, rear rotors, inner tie rod, front ball joint, water pump,
  transfer case fluid, and so on. A job counts whether MOTOR prices it
  standalone or as an add-on (e.g. "To R&R Inner Tie Rods, Add" on rack R&I,
  or "To R&R Ball Joint, Add" on the control arm). A raw operation-by-operation
  diff was tried first, and it was mostly noise from real generation changes.
- **Evidence.** A missing job is flagged when either the same VCdb model has
  it within 6 years on **both** sides, or a same-year platform twin has it.
  Twins are listed by hand in `ewt_gap_platform_twins` (Sierra/Silverado,
  Yukon/Tahoe, Durango/Grand Cherokee, MKZ/Fusion, Palisade/Telluride …),
  because MOTOR's operation ids are per make and can't reveal cross-make
  twins. Add a row there when you find a new pair. Keep the year range to
  the years the two really share a platform: Tundra/Sequoia, CR-V/RDX and
  Accord/Crosstour after 2012 were taken out because they differ.
- **Fluids filed elsewhere.** MOTOR files some fluid changes outside
  MAINTENANCE & LUBRICATION: TRANSFER CASE › TRANSFER CASE › FLUID (Telluride,
  2022+ Tundra, many PTUs) and UNIVERSALS & REAR AXLE › DIFFERENTIAL. The
  refresh folds any FLUID op in a TRANSFER CASE, DIFFERENTIAL or REAR AXLE
  subgroup back into M&L (migration `ewt_gap_audit_5_fluid_sections`). The
  first version missed this, so the Telluride was filled with a duplicate. When
  you check "does MOTOR already have it", search every section.
- **Equipment.** Rear rotor/caliper gaps need VCdb rear disc brakes.
  Differential and transfer case gaps need a VCdb RWD/4WD/AWD drive type. A
  base vehicle with no MOTOR rows at all is a coverage problem (newest model
  year, see the Jetta), not a hole, so it's left out.
- **Supplement rows count as coverage**, so a filled gap drops off on the
  next refresh. (Side effect: a supplemented year can serve as "adjacent year"
  evidence for its neighbour, e.g. 2020 Escape after 2021 was filled.)

Each row is a lead, not a verdict. Before filling one, look at what MOTOR has
for the car in that section. Most leftovers are one of three things:

- **Equipment the car doesn't have.** VCdb doesn't record it, e.g. a 2016+
  Tacoma or a 2014–18 Corolla has no rear sway bar links.
- **Hours that disagree within the generation.** MOTOR's 2012–14 Camry outer
  tie rod is 1.4 hr, but 2015–17 is 0.7.
- **No same-platform source.** For example, Escape 2013–19 rear rotors (Sean
  chose a mirror of the front add-on).

Those go to Sean. Fill without asking only when the hours come verbatim from
MOTOR for the same platform: same generation of the same model, or a twin
in the same year whose other hours in that section match.

Work tables (`ewt_gap_traffic`, `ewt_gap_jobs`, `ewt_gap_sisters`,
`ewt_gap_refresh_log`) are rebuilt on every refresh. They have RLS on and
no API grants, like `ewt_labor`.

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
3. Hours copied verbatim from MOTOR for the same platform and generation
   can go in without asking (Sean, 2026-10-27). If a figure has no
   same-platform MOTOR source, or the sources disagree, get Sean's call first.
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
   Name the car the row is attached to in `motor_db_description`, not the
   source car (see the rule above).
5. Verify:
   `select * from ewt_labor_search(<base_vehicle_id>, '<SECTION>', null, null, null, <engine_config_id>, <vehicle_id>) where mechanical_estimating_id < 0;`
   The rows should come back with `labor_price`.

### Retire a row

Once MOTOR covers an operation, set `active = false` on the supplement row.
Don't delete it, because saved quotes may still carry its negative id.
