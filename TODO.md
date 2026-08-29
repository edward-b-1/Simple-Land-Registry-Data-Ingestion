# TODO

Open items that need a decision or a separate study before they can be
implemented.

## Data cleaning (`main_pp_transactions.py`)

- **Price plausibility thresholds.** `is_plausible_price` currently uses
  provisional defaults (`PLAUSIBLE_PRICE_MIN = 10_000`,
  `PLAUSIBLE_PRICE_MAX = 50_000_000`). A separate study should look at the
  price distribution by category, property type and year (e.g. category A has
  1,041 sales under £1,000 and 24,773 under £10,000) and decide the rules,
  possibly relative to the local price level rather than fixed amounts.
- **Reverse-engineer missing postcodes.** 50,369 rows have no postcode:
  ~36k are category B type O (land / commercial parcels, often with no
  street either — these drive the 2016–2025 spike), ~14k are category A
  homes spread over 1995–2026 (~450/year; rural named properties, `PLOT n`
  new builds sold before a postcode was allocated, 2,037 with no street).
  The new-build cases are old, not recent: they peak in 2000–2004
  (128–267/year), fall away after 2011 and are essentially gone since 2015
  (0–6/year; in the 12 months to June 2026 only 162 of 598,909 category A
  sales lack a postcode, one of them a new build). The Land Registry does not
  backfill: 2001–2003 `PLOT n` sales still have no postcode 20+ years on,
  and only 19 of the 1,743 no-postcode new-build rows with a street reappear
  later under the same address with a postcode. So recovery has to be done
  here, and the yield will be modest. A first pass on the 12,086 category A
  rows with a street found:
  370 have another sale of the exact same address with a single postcode
  (recoverable directly), 2,751 more sit on a street+town that maps to a
  single postcode, 7,678 are on a street with several postcodes (need the
  house number sequence or NSPL to pick one), and the rest have no match.
  Implement the unambiguous cases, then look at nearest-number matching and
  NSPL street lookups for the rest; record the source of any inferred
  postcode in a separate column rather than overwriting.
- **PAON / SAON normalisation — first pass done, refinements open.**
  `lib_land_registry_data/lib_address.py` splits PAON / SAON into
  `building_number`, `building_name`, `flat_number`, `flat_description`
  (flats only), `unit_description` (non-flats) and `plot_number`, records
  the rule in `address_pattern`, and builds `property_key_normalised` =
  `postcode|number-or-name|flat` (a number beats a name, so `MILNER COURT, 9`
  and `9` on the same postcode are one building). Results on
  `pp_market_transactions` versus the raw `postcode|PAON|SAON` key:
  consecutive pairs 13.77M → 13.81M; pairs whose property type changes
  between sales (a collision signal) for flats with a SAON 0.69% → 0.05%
  and for flats with numeric PAON + SAON 1.36% → 0.55%; implausible pairs
  (>3× or <⅓ within 5 years) unchanged at 0.59% overall (houses with numeric
  PAON 0.58%, flats without SAON 0.47%, flats with SAON 0.31%, name-only
  houses 1.20%, houses with a SAON 2.77%); keys with more than one property
  type 353k → 349k; keys with 15+ sales 37 → 92 (merging the `NAME, n` and
  `n` forms also merges flats that have no flat number — check these).
  A distinct-value audit of the new columns then led to a second pass:
  `NUMBER NAME, NUMBER` PAONs (`3 ALDHURST ROW, 19` → building 19, unit 3),
  generic sub-unit words (`UNIT 16`, `GARAGE 14`, `BLOCK 1 ...`, bare
  `UNITS`/`FLAT`) moved out of `building_name` into `unit_description`,
  trailing `AT` stripped and a lone `FLAT` nulled in descriptions, an empty
  PAON with a numeric SAON treated as the house number, leading zeros and
  reversed ranges normalised, and — the one with real effect — an
  `is_flat_like` flag (property type F *or* the address says flat) that now
  drives the flat rules, because ~62k sales typed T/S/D carry `FLAT n`
  addresses (11k of them appear elsewhere as type F; concentrated in
  1995–2004). Side effect to remember: pairs whose `property_type` changes
  between sales are now partly *correct* merges of mistyped flats, so that
  metric (2.75% → 2.8%) no longer reads as a pure collision signal.
  Remaining problems, in order of size:
  - 2.36M of 5.71M flat-like sales have no flat identifier in the source at
    all (PAON is a bare number, SAON empty). Parsing cannot fix this; it
    needs an external address key (OS Open UPRN / AddressBase) or a
    heuristic (e.g. treat each such sale as a distinct unit when the
    building has known flats).
  - `P_NAME/S_NUMBER` on non-flats (a bare-number SAON with a name PAON) is
    assumed to be the house number; `P_NAME_NUMBER/S_NUMBER` on non-flats
    is left as `unit_description` — both are guesses worth checking against
    NSPL / UPRN.
  - `NAME n` without a comma (`CRWYS COURT 11`, 16k) is ambiguous with real
    names (`ASPECT 14`) and is left as a name; a rule keyed on a known
    building-word suffix (COURT, HOUSE, ...) would catch most.
  - `P_OTHER` and `S_OTHER` rows are stored unparsed (`building_name` = raw
    PAON, description = raw SAON); `ROOM n`, `G102 GROSVENOR HOUSE` etc.
    could get rules if they matter.
  - Ranges (`17-19`) are kept as one building; a sale of `17` and a sale of
    `17-19` on the same postcode are different keys.
  - Floor-coded flat numbers (`0501`) are kept verbatim; `001` → `1` only
    for values of three digits or fewer.
  - The build now takes ~16 minutes (was 9) because of the regex work; fine
    for a nightly refresh, but worth caching the parsed parts if the rules
    keep growing.
- **First repeat-sales index build** should exclude flats without a SAON
  and pairs where `property_type` changes, and drop pairs outside ±3× per
  5 years, rather than wait for the normalisation.
- **Validate address and geography fields against the postcode.** The NSPL
  is now loaded (`ons.nspl_postcode`, May 2026 release): 1,335,003 of the
  1,335,540 distinct postcodes in `pp_transactions` match (31,359,977 of
  31,361,167 sales; 53,258 sales are on postcodes that have since been
  terminated, so always join without a `doterm is null` filter). The 537
  unmatched postcodes (1,190 sales, e.g. `TW8 0YY`, `SW8 4EF`) look like
  new-build postcodes registered before the NSPL picked them up — check
  against the next release. Comparing the PPD `district` with the NSPL local
  authority name: 4.7% of 2024–2026 market sales differ, almost all naming
  convention (`CITY OF BRISTOL` vs `Bristol, City of`, `WREKIN` vs `Telford
  and Wrekin`, `RHONDDA CYNON TAFF` vs `... Taf`); the rate rises to ~20% for
  1995–2009 sales because the PPD records the authority as it was at the
  time (pre-reorganisation). Next steps: add `lad_code` / `rgn_code` /
  `lsoa_code` to `pp_transactions` (or a view) from the NSPL and treat them
  as the canonical geography; build a name-normalisation map for the
  convention differences so genuine mismatches can be flagged; do the same
  for `county` and `town_city`.
- **Category B before October 2013.** The Land Registry only collected
  category B from October 2013, but the data has category B rows back to
  1995. Understand what these are before using category B counts over time.

## Analysis pipeline

- ~~ONS NSPL ingestion~~ — done (`main_nspl.py`, `ons` schema, latest
  quarterly release resolved automatically). Remaining: join the NSPL
  geography into `pp_transactions` (see the validation item above).
- Repeat-sales pairs table and the relative price index, see
  `PRICE_INDEX.md`.
- Replacement Bank of England series for effective rates on new advances
  after December 2015 (`CFMBJ42`–`CFMBJ45` stop there), see
  `MORTGAGE_RATES.md`.
