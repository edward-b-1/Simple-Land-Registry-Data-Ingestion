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

- `record_op` — in `pp-complete.txt` every row is an addition (`A`); the
  change/delete records only appear in the monthly update files. The
  cleaning step asserts this rather than resolving it.
- Filter `ppd_cat = 'A'` for market analysis.
- The "O" (other) property type is entirely within category B, so the
  category A filter removes it — it is mostly commercial/odd lots and skews
  averages massively.
- Exact duplicate rows (same everything except the id, typically one row
  per title in a portfolio sale) and same-property-same-day multi sales are
  flagged and excluded from market transactions.

These steps are implemented in `main_pp_transactions.py`, which builds
`land_registry.pp_transactions` and the `pp_market_transactions` view (see
the README); open questions are in `TODO.md`.

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
| ONS National Statistics Postcode Lookup (NSPL) — **loaded, `ons.nspl_postcode` / `ons.nspl_code_lookup`** | postcode | Lat/long, LSOA/MSOA, ward, local authority, region, rural/urban class, IMD rank. Foundational — nearly everything else joins through this. |
| EPC register (Energy Performance Certificates, open data) | address / UPRN | Floor area, bedrooms, build age, energy rating. Turns price into price per m², the biggest missing dimension in PPD. Also allows testing the "green premium". |
| Index of Multiple Deprivation | LSOA | Price vs deprivation; gentrification detection (areas whose relative price rank rises fastest). |
| Census 2021 | LSOA/MSOA | Tenure, household composition, occupation, age — explain price variance. |
| Bank of England base rate (IUDBEDR daily, IUMABEDR monthly average) — **loaded, one table per series in schema `bank_of_england`** | date | Rate sensitivity of prices and volumes; spread between base rate and mortgage rates. |
| Bank of England quoted mortgage rates (2y/3y/5y fixed and 2y variable at 75%/90% LTV, lifetime tracker, SVR — series IUMBV34, IUMB482, IUMBV37, IUMBV42, IUMBV48, IUMB479, IUMBV24, IUMTLMV; from 1995) and effective rates actually paid on the outstanding stock (CFMHSDE, from 1999) and new advances by fixation period (CFMBJ39/42/43/44/45, from 2004) — **loaded, one table per series in schema `bank_of_england`** | date (lag ~3 months to completion) | What buyers actually paid to borrow. Payment-to-income affordability (median price, 75% LTV, 25-year term at that month's rate ÷ median earnings). See `MORTGAGE_RATES.md`. |
| MLAR (BoE/FCA Mortgage Lenders & Administrators Statistics, quarterly from 2007) | quarter | New advances by LTV band, loan-to-income band, purpose, borrower type (FTB / mover / BTL) and rate type; outstanding stock by product (fixed/variable, repayment vs interest-only); arrears and possessions by LTV. See `MORTGAGE_PRODUCTS.md`. |
| BoE effective rates volumes (Bankstats G1.4, monthly from 2004) | month | Share of new lending that is floating, fixed ≤1y, 1–5y, 5–10y, >10y — the fixed-rate share and the shift from 2-year to 5-year fixes; rate-shock exposure by vintage. |
| UK Finance (ex-CML) Regulated Mortgage Survey (monthly from 2005, some annual series from the 1970s) | month + region | FTB / mover / remortgage loan counts, average advance, median LTV, median income multiple, share of income on payments, by region. Cash-buyer share (PPD transactions − mortgaged purchases); deposit-to-income for first-time buyers. |
| BoE mortgage approvals (LPMVTVX, monthly) | month | 2–3 month leading indicator of transaction volumes. |
| BoE Financial Stability Report data (share of lending at ≥4.5× LTI, from PSD) | quarter | High-LTI lending series for the FPC limit. |
| NMG household survey microdata (BoE, annual from 2004) | year + region | Borrower-level mortgage type, rate paid, balance and house value (→ LTV), income, payment difficulty. |
| English Housing Survey / Survey of English Housing (UKDS, from 1993) | year + region | Owner-occupier mortgage type (repayment / interest-only / endowment, fixed vs variable), purchase year and price, deposit, income, FTB status. |
| Wealth and Assets Survey (ONS via UKDS, waves from 2006) | wave + region | Mortgage balance vs house value — LTV distribution of the whole housing stock, not just new lending. |
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
