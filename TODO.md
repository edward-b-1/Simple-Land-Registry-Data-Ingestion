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
- **PAON / SAON quality.** Detect and analyse rows with "bad" address data
  (e.g. 1.12M PAONs containing commas such as `WM MORRISON SUPERMARKETS PLC, 1`
  = building name + number, 4,182 empty PAONs, 500k empty streets) and design
  a normalisation so that `property_key` matches the same property across
  sales more reliably. Candidates: split PAON into name and number parts,
  strip flat prefixes in SAON, compare against OS Open UPRN / AddressBase.
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
