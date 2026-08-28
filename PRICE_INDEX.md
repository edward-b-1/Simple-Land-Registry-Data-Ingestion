# Building a price index to normalise property prices

Goal: de-trend the Price Paid data so that regional and local price
movements can be detected, rather than just "everything went up, some regions
went up more than others".

## Two different normalisations

Deflating by CPI/RPI and de-trending against the national housing market are
related but answer different questions.

### 1. Deflate by CPI/RPI → "real" prices

Divide each price by the CPI level for its month. This removes general
inflation, so a £100k house in 1995 and a £200k house in 2015 are comparable
in purchasing-power terms. But house prices have massively outpaced CPI since
1995, so a real-price chart still says "everything went up, London went up
more". This is the right tool for affordability questions ("is housing more
expensive relative to the cost of everything else?"), not for detecting
regional dynamics.

A variant is to deflate by a **regional wage index** (ONS ASHE) instead of
CPI. This gives price-to-earnings ratios over time and answers "is housing
more expensive relative to what people here earn?".

### 2. Deflate by a national house price index → "relative" prices

Divide each price by the national house price level for its month. The
national trend is then flat by construction, and what remains is purely
regional/local movement relative to the country: which areas outperformed,
when, and by how much. This is the de-trending mechanism for detecting
regional movement. It can be applied hierarchically: deflate by the regional
index to see which districts/towns moved relative to their region.

Both can be combined: deflate by CPI to get real prices, then analyse the
national real-price trend separately from regional deviations.

## Where does the national index come from?

### External: ONS UK House Price Index

Monthly, national/regional/local authority level, free, itself built from
PPD plus mortgage data using a hedonic model. Simplest, well understood, and
its regional series can be used directly.

### Internal: built from this data

Fully consistent with the sample, and the method is under your control.
Options, from crude to good:

- **Median price per month.** Mix-biased — a month with more flats sells
  "cheaper" without any price fall. Noisy at fine geography.
- **Stratified / mix-adjusted.** Median per
  (region × property type × new-build × tenure) cell, re-weighted to fixed
  weights (e.g. 2015 transaction mix). Decent and cheap to do in SQL.
- **Repeat-sales.** Regress `log(price_second) − log(price_first)` on time
  dummies for the two sale dates (Bailey–Muth–Nourse / Case–Shiller). Mix-free
  because each property is its own control. This is the recommended approach:
  the data has full addresses so a property key (postcode + PAON + SAON) can
  be built, and the resulting pairs table is reusable for many other analyses
  (holding periods, returns, flipping).

## The elegant formulation

Once repeat-sales pairs exist, the cleanest way to get "regional movement net
of national" is a single regression rather than dividing two separately
estimated indices:

```
Δlog(price) = Σ_t β_t · (D_t,second − D_t,first)              # national time effects
            + Σ_{r,t} γ_{r,t} · (D_r,t,second − D_r,t,first)  # regional deviations
            + ε
```

where `D_t` is a dummy for period `t` and `D_r,t` a dummy for region `r` in
period `t`. The `β` coefficients are the national index, the `γ` coefficients
are each region's deviation from it, and they are jointly estimated with
proper standard errors — so it is possible to say "the North East diverged
*significantly* from the national trend in 2005–07" rather than eyeballing a
ratio.

At district level the `γ` estimates become noisy, so aggregate to quarters or
years and shrink toward the regional estimate (or fall back to
ratio-of-indices with a rolling window).

## Practical caveats

- Use log prices throughout; ratios and percentage changes then become simple
  differences.
- "National" is transaction-weighted by default, so London dominates it.
  Decide whether that is wanted or whether an equally-weighted-by-region
  index is more appropriate — it changes what "outperformed the nation"
  means.
- Filter `ppd_cat = 'A'`, drop property type `O`, resolve `record_op`, and
  drop pairs with fewer than ~6 months between sales (renovation flips
  distort the index).
- Monthly granularity is fine nationally and for regions; use quarterly for
  districts and annual for postcode sectors.

## Suggested output

A table:

```
relative_index(
    geography_level,   -- region / district / postcode district
    geography_id,
    period,
    index_value,       -- local index
    national_index,
    relative_index,    -- index_value / national_index
    n_transactions
)
```

at region → district → postcode district granularity. From that:

- a map of `relative_index` change over any window (e.g. 2019Q4 → 2024Q4)
  shows which places moved against the national tide;
- a time series of `relative_index` per district shows *when* it happened.

## First concrete steps

1. Resolve `record_op` and dedup; filter `ppd_cat = 'A'`; drop type `O`.
2. Build the repeat-sales pairs table: property key → consecutive sale pairs
   with dates, prices, log-return and months held.
3. Build a mix-adjusted monthly index as the quick baseline.
4. Decide on the regression step after inspecting the pairs.
