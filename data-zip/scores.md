# Scores (Metrics)

These are the "scores" (metrics) calculated for a plan.
To make them easier to work with, metrics are stored in separate files by category: 

* **General** &ndash; population deviation
* **Partisan** &ndash; measures of partisan bias (fairness) & competitiveness (responsiveness)
* **Minority** &ndash; measures of the opportunity for minority representation 
* **Compactness** &ndash; measures of compactness
* **Splitting** &ndash; measures of county-district splitting
* **Global** &ndash; measures that depend on other measures, e.g., MOD districts

For each of those categories, there is a CSV with plan-level scores and a JSONL file with by-district aggregates. 
*Note: The first value in each by-district aggregate is the total for the state. The values 1-N are the values for the individual districts.*

## General

There is one general metric:

*   **population_deviation** &ndash; The population deviation of the plan.

There is one corresponding by-district aggregate:

* **pop_by_district** &ndash;  The total population by district.

## Partisan 

The measures of partisan bias (next) and competitiveness & responsiveness (following) are described in some detail in
[Advanced Measures of Bias &amp; Responsiveness](https://medium.com/dra-2020/advanced-measures-of-bias-responsiveness-c1bf182d29a9).
Many use [fractional seat probabilities](https://lipid.phys.cmu.edu/nagle/Technical/FractionalSeats2.pdf).

*   **estimated_vote_pct** &ndash; The Democratic two-party vote share. 
    This is the same for every plan for a state.

These are the plan-level partisan bias metrics:

*   **pr_deviation** &ndash; The deviation from pr_seats. Smaller is better, and zero is perfect.
*   **estimated_seats** &ndash; The estimated number of fractional Democratic seats.
*   **fptp_seats** &ndash; The estimated number of Democratic seats using "first past the post" (FPTP), all-or-nothing accounting.
*   **disproportionality** &ndash; estimated_vote_pct minus estimated_seats as a percentage of the number of districts.
*   **efficiency_gap_wasted_votes** &ndash; The efficiency gap calculated using the wasted votes formula. Smaller absolute value is better. Positive values favor Republicans; negative values favor Democrats.
*   **efficiency_gap_statewide** &ndash; The efficiency gap calculated using the statewide formula and FPTP accounting.
*   **efficiency_gap** &ndash; The efficiency gap calculated using the statewide formula and fractional seat probabilities. 
*   **seats_bias** (αₛ) &ndash; The seats bias at 50% Democratic vote share.
*   **votes_bias** (αᵥ) &ndash; The votes bias at 50% Democratic vote share.
*   **geometric_seats_bias** (β) &ndash; The seats bias at the statewide Democratic vote share, not 50% (aka "partisan bias").
*   **declination** (δ) &ndash; The declination angle (in degrees), calculated using fractional seats and votes. Smaller is better.
*   **mean_median_statewide** &ndash; The statewide Democratic two-party vote share minus the median Democratic two-party district vote share.
*   **mean_median_average_district** &ndash; The mean Democratic two-party district vote share minus the median Democratic two-party district vote share.
*   **turnout_bias** (TO) &ndash; The difference between the statewide Democratic vote share and the average their average district vote share.
*   **lopsided_outcomes** (LO) &ndash; The difference between the average two-party vote shares for the Democratic and Republican wins.
*   **proportionality** &ndash; DRA's propoprtionality rating. Integers [0-100], where bigger is better.

These are the competitiveness & responsiveness metrics:

*   **competitive_district_count** &ndash; The number of districts that fall into the 45-55% Democratic/Republican range.
*   **competitive_districts** &ndash; The estimated number of competitive districts, using fractional seat probabilities. Bigger is better.
*   **average_margin** &ndash; The average margin of victory. Smaller is better.
*   **responsiveness** (ρ) &ndash; The slope of the seats-votes curve at the statewide Democratic vote share.
*   **responsive_districts** &ndash; The likely number of responsive districts, using fractional seat probabilities.
*   **overall_responsiveness** (R) &ndash; An overall measure of responsiveness which you can think of as a winner’s bonus.
*   **competitiveness** &ndash; DRA's competitiveness rating. Integers [0-100], where bigger is better.

There are two by-district aggregates:

* **dem_by_district** &ndash;  The number of Democratic votes by district.
* **tot_by_district** &ndash; The total two-party (Democratic & Republican) votes by district.

## Minority

These are measures of the opportunity for minority representation.
See also the measures in the 'global' category below.

*   **mmd_black** &ndash; The count of Black-alone majority-minority districts (MMD).
*   **mmd_hispanic** &ndash; The count of Hispanic-alone majority-minority districts (MMD).
*   **mmd_coalition** &ndash; The count of Black & Hispanic-together coalition districts (MMD).

*   **opportunity_districts** &ndash; The estimated number of single race or ethnicity minority opportunity districts, using fractional seat probabilities (and DRA's method). Note: This revised metric means does not clip below the 37% threshold (like DRA does). Hence, the results are more continuous.
*   **proportional_opportunities** &ndash; The proportional number of single race or ethnicity minority opportunity districts, based on statewide VAP.
*   **coalition_districts** &ndash; The estimated number of all-minorities-together coalition districts, using fractional seat probabilities (and DRA's method). Note: This revised metric means does not clip below the 37% threshold (like DRA does). Hence, the results are more continuous.
*   **proportional_coalitions** &ndash; The proportional number of all-minorities-together coalition districts, based on statewide VAP.
*   **minority** &ndash; DRA's minority opportunity rating. Integers [0-100], where bigger is better. Note: This revised metric means does not clip below the 37% threshold (like DRA does). Hence, the results are more continuous.

There are mostly self-explanatory by-district aggregates for each VAP category:

* **total_vap**
* **white_vap**
* **black_vap**
* **hispanic_vap**
* **asian_vap**
* **pacific_vap**
* **native_vap**
* **minority_vap** &ndash; The total VAP minus white VAP, i.e., all minorities combined.

There are also corresponding CVAP by-district aggregates.

## Compactness

The measures of compactness:

*   **reock** &ndash; The average Reock measure of compactnes for the districts. Bigger is better.
*   **polsby_popper** &ndash; The average Polsby-Popper measure of compactness for the districts. Bigger is better.
*   **cut_score** &ndash; The number of edges between nodes (precincts) in the contiguity graph that are cut (cross district boundaries). A measure of compactness using discrete geometry. Smaller is better.
*   **spanning_tree_score** &ndash; The spanning tree scrore. Another measure of compactness using discrete geometry. Bigger is better.
*   **population_compactness** &ndash; The population compactness of the map. Lower is more *energy* compact. Smaller is better.
*   **compactness** &ndash; DRA's compactness rating. Integers [0-100], where bigger is better.

There are two sets of by-district aggregates for compactness. 
First abstracts of district shapes from which Reock & Polsby-Popper can be computed efficiently:

* **area**
* **diameter**
* **perimeter**

and the by-district measurements of them:

* **reock**
* **polsby_popper**

## Splitting

These are measures of county-district splitting:

The county and district splitting measures are described in
[Measuring County &amp; District Splitting](https://medium.com/dra-2020/measuring-county-district-splitting-48a075bcce39).

*   **county_splitting** &ndash; A measure of the degree of county splitting. Smaller is better, and 1.0 (no splitting) is the best.
*   **district_splitting** &ndash; A measure of the degree of district splitting. Smaller is better, and 1.0 (no splitting) is the best.
*   **counties_split** &ndash; The number of counties split across districts. Smaller is better.
*   **county_splits** &ndash; The number of *times* counties are split, e.g, a county may be split more than once. Smaller is better.
*   **splitting** &ndash; DRA's county-district splitting rating. Integers [0-100], where bigger is better.

There is one associated by-district aggregate:

* **district_splitting**

## Global

This category includes metrics that depend on other metrics and/or depend on by-district measurements from multiple categories. 
For the paper, this category includes:

*   **mmd_districts** &ndash; The sum of majority-minority districts (MMD) for Blacks alone, Hispanics alone, and Blacks & Hispanics together (but neither alone, i.e., coalitions).
*   **mmd_reock** &ndash; The average Reock for MMD districts.
*   **mmd_polsby_popper** &ndash; The average Polsby-Popper for MMD districts.
*   **mmd_district_splitting** &ndash; The average district-splitting for MMD districts.
