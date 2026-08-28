# Mortgage product and LTV datasets

Datasets giving breakdowns of what kinds of mortgage products people are on,
loan-to-value (LTV), loan-to-income (LTI) and borrower type. There are
several, at three tiers of granularity. None of them link to individual
Price Paid transactions (that would need the charges register, which is not
open data), so they all join on date and/or region — but for product mix and
LTV they are rich.

## Tier 1: Published aggregate tables (free, easy)

### MLAR — Mortgage Lenders & Administrators Statistics (BoE/FCA)

Quarterly from 2007 Q1, published as Excel tables. The closest match to a
"product mix by year" dataset:

- New advances by **LTV band** (<75%, 75–90%, 90–95%, >95%) and by
  **loan-to-income band**, and the combination of high LTV + high LTI.
- New advances by **purpose** (house purchase / remortgage / further advance)
  and **borrower type** (first-time buyer, home mover, buy-to-let).
- New advances by **rate type** (fixed vs variable) and by rate band relative
  to base rate.
- Outstanding stock by product (fixed/variable, repayment vs
  **interest-only**), plus arrears and possessions by LTV band.

### BoE effective rates release (Bankstats G1.4)

Monthly from 2004. Alongside the rates it publishes the *volumes* used to
weight them, giving the share of new lending that is floating, fixed ≤1y,
1–5y, 5–10y, >10y — i.e. the fixed-rate share and the shift from 2-year to
5-year fixes over time.

### UK Finance (ex-CML) Regulated Mortgage Survey

Monthly from 2005. Tables for first-time buyers, home movers and
remortgagers: number of loans, average advance, **median LTV, median income
multiple, share of income spent on mortgage payments** — with **regional**
breakdowns. Some of the longer annual series go back to the 1970s. Headline
tables are free; the detailed regional tables have drifted behind membership
over the years, so check what is currently open.

### Bank of England Financial Stability Report data

The FPC's LTI limit (share of lending at ≥4.5× income) is monitored from
Product Sales Data and the chart data is downloadable — a good quarterly
high-LTI series.

## Tier 2: Survey microdata (free registration, borrower-level)

### NMG household survey (BoE, annual since 2004)

Microdata is downloadable directly from the BoE. Mortgage type, rate paid,
outstanding balance, house value (→ LTV), income, region, payment
difficulty. Sample of roughly 6,000 households per year.

### English Housing Survey (MHCLG, annual; predecessor Survey of English Housing from 1993)

Owner-occupier module: repayment vs interest-only vs endowment, fixed vs
variable, year of purchase, purchase price, deposit, income, first-time-buyer
status, region. Available via the UK Data Service.

### Wealth and Assets Survey (ONS, waves from 2006)

Mortgage balance and self-reported house value, so it gives the **LTV
distribution of the whole stock**, not just new lending. Also via the UK
Data Service.

## Tier 3: Loan-level (not accessible)

FCA **Product Sales Data** (PSD001 / PSD007) — every regulated mortgage sale
with LTV, LTI, term, rate type and postcode district. The FCA publishes
occasional aggregates from it (e.g. term lengths, interest-only), but the
loan-level data is only available to the regulator and, via secure research
access, some academics.

## What these add to the analysis

- **Cash-buyer share**: PPD transaction counts minus UK Finance / MLAR
  mortgaged purchases ≈ cash purchases. By region this identifies
  retiree/investor markets, which matters because cash buyers are
  rate-insensitive.
- **Deposit-to-income**: median price × (1 − median FTB LTV) ÷ median
  earnings — the real barrier for first-time buyers, and a better regional
  divergence story than price alone.
- **Rate-shock exposure**: fixed-rate share and fixation length by year shows
  how much of the stock was insulated from 2022–23, and hence which vintages
  face the refinancing cliff.
- **Interest-only legacy**: the pre-2008 interest-only cohort in MLAR / EHS,
  useful for explaining 2004–07 price growth.

See also `MORTGAGE_RATES.md` for the rate series themselves.
