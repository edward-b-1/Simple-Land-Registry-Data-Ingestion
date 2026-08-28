# Mortgage rate data for analysis against Price Paid data

Analysis against the Bank of England base rate is possible, but what matters
to buyers is the mortgage rate they actually pay. Although those rates come
from high-street lenders, the Bank of England already collects them from the
lenders and publishes aggregates for free, so no scraping of individual banks
is needed.

## Bank of England quoted and effective rates (the main source)

The BoE Interactive Database (Bankstats tables G1.3 / G1.4) has two families
of monthly series, all compiled from lender submissions and downloadable as
CSV.

### Quoted household interest rates

The average *advertised* rate across major lenders for a given product.

| Product | Series | From |
|---|---|---|
| 2-year fixed, 75% LTV | IUMBV34 | 1995 |
| 2-year fixed, 90% LTV | IUMB482 | 1995 |
| 5-year fixed, 75% LTV | IUMBV42 | 1995 |
| 2-year variable (tracker), 75% LTV | IUMBV24 | 1995 |
| Standard variable rate | IUMTLMV | 1995 |

The start dates line up almost exactly with the Price Paid data (1995).

### Effective interest rates

The rates *actually paid*, weighted by lending volume, split by new business
vs outstanding stock and fixed vs floating (e.g. the effective rate on new
mortgages, series CFMHSDE). These run from 2004 and are the best measure of
what buyers in a given month actually signed up to.

Both families are far more useful than the base rate alone, because the
spread between base rate and mortgage rates is a story in itself: it blew out
in 2008–09 (base rate fell to 0.5% but mortgage rates barely moved), narrowed
through the 2010s, and lagged again in 2022.

## Supporting sources

- **MLAR** (BoE/FCA Mortgage Lenders & Administrators Return) — quarterly
  aggregates from 2007: new advances by LTV band, income multiple, fixed vs
  variable, first-time-buyer share. Allows modelling the *typical borrower*,
  not just the typical rate.
- **BoE mortgage approvals** (series LPMVTVX) — monthly volumes; a well-known
  leading indicator of transactions by roughly 2–3 months. Worth joining
  against monthly transaction counts from the Price Paid data.
- **Moneyfacts** — the source of "average 2-year fix hits X%" press headlines.
  Daily and granular, but commercial; only historical headline figures are
  free.
- **UK Finance** (formerly the Council of Mortgage Lenders) — lending and
  affordability statistics, partly free.

Loan-level data (FCA Product Sales Data) exists but is not public, so the
BoE aggregates are the practical ceiling.

## What it unlocks

The most interesting derived measure is **payment-to-income** rather than
price-to-income:

1. Take the regional median price for the month.
2. Assume 75% LTV and a 25-year repayment term at that month's 2-year fixed
   quoted rate.
3. Compute the monthly repayment.
4. Divide by regional median earnings (ONS ASHE).

This is how affordability actually feels to a buyer. It explains why prices
kept rising through the 2010s despite high price-to-earnings ratios (cheap
money) and why 2022–23 bit so hard.

## Modelling caveats

- Since roughly 2010 most UK borrowers are on 2- or 5-year fixes, so rate
  changes hit existing owners with a lag. The *new-business* rate is what
  matters for transaction prices; the *outstanding-stock* rate matters for
  forced-sale pressure.
- Completions lag mortgage offers by a few months, so match the rate at
  approximately `transaction_date − 3 months`.
