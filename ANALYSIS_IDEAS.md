# Analysis ideas for the Price Paid dataset

The `land_registry.pp_complete_data` table holds ~31.4M transactions
(1995 → June 2026) with: price, date, postcode, property type (D/S/T/F/O),
new-build flag, freehold/leasehold, address components (PAON/SAON/street/
locality/town/district/county), PPD category (A = standard, B = non-market /
repossession etc.) and record op (A/C/D — add/change/delete).

## Analysis with the data as-is

### Repeat-sales price index

Because the data has full addresses, a house-level key (postcode + PAON +
SAON) can be built and successive sales of the same property paired up. This
gives a Case–Shiller-style index that controls for property mix far better
than median price, and allows computing holding periods, annualised returns
per property, and "flipping" activity (resold within 12–24 months). This is
the single most valuable derived dataset — most of the other ideas build on
it.

### Data hygiene first

- `record_op` — the file contains change/delete records as well as additions,
  so these need resolving (apply C/D against A by `transaction_unique_id`)
  before any counts are trustworthy.
- Filter `ppd_cat = 'A'` for market analysis.
- Decide how to treat the "O" (other) property type — it is mostly
  commercial/odd lots and skews averages massively.

### Other quick wins

- Regional price and volume trends by month; identify the 2008 crash, the
  2016 stamp-duty surge (March 2016 cliff), the 2020–21 SDLT holiday and the
  2022 rate shock.
- New-build premium: new vs existing price gap by district and year, and how
  fast it decays on resale.
- Leasehold discount, and the growth of leasehold houses (a live policy
  issue).
- Transaction volume as a leading indicator versus prices.
- Price seasonality and day-of-month clustering (completions bunch at
  month/quarter end).
- Street-level: most expensive/cheapest streets, streets with fastest
  appreciation.
- Anomaly detection: sales far below the repeat-sales implied value
  (potential non-market sales miscoded as category A).

## Augmentation ideas (all free/open)

| Dataset | Join key | What it unlocks |
|---|---|---|
| ONS Postcode Directory / NSPL | postcode | Lat/long, LSOA/MSOA, ward, local authority, region, rural/urban class. Foundational — nearly everything else joins through this. |
| EPC register (Energy Performance Certificates, open data) | address / UPRN | Floor area, bedrooms, build age, energy rating. Turns price into price per m², the biggest missing dimension in PPD. Also allows testing the "green premium". |
| Index of Multiple Deprivation | LSOA | Price vs deprivation; gentrification detection (areas whose relative price rank rises fastest). |
| Census 2021 | LSOA/MSOA | Tenure, household composition, occupation, age — explain price variance. |
| Bank of England base rate / mortgage rates | date | Affordability models, rate sensitivity of volumes. |
| ONS earnings (ASHE) by region / local authority | district + year | Price-to-earnings ratios over time — affordability maps. Also usable as a deflator: dividing prices by a regional wage index de-trends against local earnings rather than CPI (see `PRICE_INDEX.md`). |
| ONS earnings distributions (ASHE percentiles: 10th/25th/median/75th/90th, by residence or workplace, full-/part-time) | region or district + year | Quantile-to-quantile affordability: lower-quartile house price vs lower-quartile earnings, share of local earners who can afford the median local home at a given income multiple, and how the gap between price and wage distributions has widened by region. ASHE is individual employee pay, so pair with household income estimates (ONS small-area income estimates by MSOA) for household-level affordability. |
| Ordnance Survey Open UPRN / Open Names / AddressBase (partial) | address | Canonical property identifier; better dedup than address string matching. |
| School data (Ofsted ratings, DfE performance tables) | lat/long / catchment | The classic "good school premium" study. |
| Rail/tube stations (NaPTAN), Crossrail opening dates | lat/long distance | Transport premium; before/after event studies (Elizabeth line 2022). |
| Flood risk zones (Environment Agency) | polygon | Flood discount, and whether it grew after the 2013/2015 floods. |
| Planning applications (Planning Data / LandInsight) | postcode | New development effect on neighbours. |
| CPI / RPI | date | Real vs nominal prices — most headline charts get this wrong. |
| Land Registry INSPIRE polygons / Title data | UPRN / polygon | Plot size (freehold houses) — price per plot area. |

## Suggested first project

1. Apply `record_op` resolution and dedup.
2. Join NSPL for geography.
3. Build the repeat-sales pairs table.
4. Join EPC for floor area.

From there it is possible to produce a per-m² regional index, a
gentrification map, and holding-period/return distributions — each a
substantive analysis rather than another "average price by region" chart.
