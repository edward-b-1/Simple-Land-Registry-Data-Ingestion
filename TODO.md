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
  homes spread evenly over 1995–2026 (~450/year; rural named properties,
  `PLOT n` new builds sold before a postcode was allocated, 2,037 with no
  street). A first pass on the 12,086 category A rows with a street found:
  370 have another sale of the exact same address with a single postcode
  (recoverable directly), 2,751 more sit on a street+town that maps to a
  single postcode, 7,678 are on a street with several postcodes (need the
  house number sequence or NSPL to pick one), and the rest have no match.
  Implement the unambiguous cases, then look at nearest-number matching and
  NSPL street lookups for the rest; record the source of any inferred
  postcode in a separate column rather than overwriting.
- **PAON / SAON normalisation into separate fields.** The raw fields mix
  conventions: PAON is a plain number (85.6% of category A), a name only
  (10.6%), or `NAME, NUMBER` (3.1%, e.g. `MILNER COURT, 9` where `9` is the
  street number of the block and SAON `FLAT 1` is the flat); SAON is empty
  (88.5%), `FLAT n` / `APARTMENT n` (7.9%), a bare number (2.8% — sometimes a
  flat number, sometimes the street number when PAON is a building name, as
  in PAON `MILNER COURT` / SAON `2`), or a floor description. 2.24M category
  A flat sales have no SAON at all, so their `property_key` collides with
  other flats in the same building. Proposed target columns:
  `building_number` (street number, incl. suffix e.g. `12A`),
  `building_name`, `flat_number` (NULL for non-flats; from SAON or from a
  bare-number SAON when PAON is a name), `flat_description` (floor etc.),
  `plot_number`, plus an `address_pattern` code recording which rule
  produced them. Then rebuild `property_key` from the normalised parts and
  compare against OS Open UPRN / AddressBase where possible.
- **Baseline for the address study.** Current `property_key` quality on
  `pp_market_transactions` (13.8M consecutive pairs): share of pairs with a
  >3× or <⅓ price move within 5 years — houses with numeric PAON 0.58%,
  flats without SAON 0.47%, flats with SAON 0.32%, name-only PAON houses
  1.21%, houses with a SAON 3.07%; 353k keys carry more than one property
  type; worst keys have 20+ "sales". Re-measure after normalisation.
- **First repeat-sales index build** should exclude flats without a SAON
  and pairs where `property_type` changes, and drop pairs outside ±3× per
  5 years, rather than wait for the normalisation.
- **Validate address and geography fields against the postcode.** Once the
  ONS Postcode Directory / NSPL is loaded, check `town_city`, `district` and
  `county` against the postcode's official geography, flag inconsistent rows,
  and use the postcode-derived geography as the canonical one.
- **Category B before October 2013.** The Land Registry only collected
  category B from October 2013, but the data has category B rows back to
  1995. Understand what these are before using category B counts over time.

## Analysis pipeline

- ONS Postcode Directory / NSPL ingestion (postcode → lat/long, LSOA, local
  authority, region), see `ANALYSIS_IDEAS.md`.
- Repeat-sales pairs table and the relative price index, see
  `PRICE_INDEX.md`.
- Replacement Bank of England series for effective rates on new advances
  after December 2015 (`CFMBJ42`–`CFMBJ45` stop there), see
  `MORTGAGE_RATES.md`.
