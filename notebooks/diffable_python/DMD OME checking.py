# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: all
#     notebook_metadata_filter: all,-language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Updating the opioid measure with dm+d derived figures

# We have historically used a [presentation-level BNF code-based approach](https://docs.google.com/spreadsheets/d/1IjnHbYVszZKPmVSYydtMVzbDLPOmq8bOFq45QsSu6sE/edit#gid=0) to calculating Oral Morphine Equivalence (OME) for opioid measures.  However, this requires a high level of curation, as any new drug presentation will not automatically appear.  It is also open to a higher degree of error, due to the large number of individual codes and OME values that need to be managed.  A recent review of these codes has found a few errors in the existing list.
#
# The Dictionary of Medicines and Devices (dm+d) offers a solution to this.  It provides information on the amount of drug per dose for each chemical prescribed.  This holds a number of advantages:
# - as it is linked to individual chemicals, combined products not within the usual BNF codes for these products are picked up
# - combination products will show up separately for each chemical, and allow simple calculations
# - any new products, as long as within existing chemical substances and formulations will be picked up
#
# This means that there is less need for curation, and lower chance of error for using a dm+d-based methodology.  If successful, this metholodology could be expaned to provide more accurate analyses elsewhere, e.g. when calculating Defined Daily Doses (DDD) or Average Daily Quantities (ADQ)

# ### SQL used for testing

# There have been various iterations of the SQL used to test whether this will work.  This is the currently created version:

# The first item is a subquery to manage pharmaceutical form.  The dm+d ontology describes the type of formulation.  However, there are two issues with this:
# - Injections have many routes included, e.g. an injection may show as _subcutaneous_ and _intramuscular_.  This will duplicate the the VMP code, and give two or more times the correct value.  To adjust for this a CASE statement creates a single formulation of _injection_ for these products
# - Fentanyl buccal film has a different OME to other buccal products.  A CASE statement ensures that _film_ is listed as a separate formulation to achieve this.
#
# The subquery also removes the dosage form, leaving only the route.

# ```sql
# WITH simp_form AS (
#   SELECT DISTINCT 
#     vmp, #vmp code
#     CASE WHEN descr LIKE '%injection%' THEN 'injection' --creates "injection" as route, regardless of whether injection or infusion. this also removes injection routes, e.g.
#     WHEN descr LIKE '%infusion%' THEN 'injection' --s/c, i/v etc, AS often injections have many licensed routes, e.g "solutioninjection.subcutaneous" AND solutioninjection.intramuscular"which would multiply the row
#     WHEN descr LIKE 'filmbuccal.buccal' THEN 'film' --buccal films have a different OME and so should be indentified here
#     ELSE SUBSTR(
#       form.descr, 
#       STRPOS(form.descr, ".")+ 1) --takes the dosage form out of the string (e.g. tablet.oral) TO leave route.
#     END AS simple_form 
#   FROM 
#     dmd.ont AS ont --the coded route for dosage form, includes vmp code 
#     INNER JOIN dmd.ontformroute AS form ON form.cd = ont.form --text description of route
#     )
# ```

# The next subquery normalises numerators and denominators to mg and ml.  Some products are shown in micrograms, grams, or other.  If in miligrams or grams, the CASE statement converts to miligrams.  If there is another form, it returns a NULL value.  These can then be filtered for identification if neccesary.  The same methodology is applied to denominators which are not in mililitres.

# ```sql
# ,norm_vpi AS (
#     SELECT 
#     vmp, --vmp code
#     ing, --ing code
#     strnt_nmrtr_val,--numerator strength value
#     strnt_nmrtr_uom,--numerator unit of measurement
#     unit_num.descr as num_unit, --numerator unit 
#     unit_den.descr as den_unit, --denominator unit
#     CASE WHEN unit_num.descr = 'microgram' THEN vpi.strnt_nmrtr_val / 1000 --creates miligram value from mcg value
#     WHEN unit_num.descr = 'gram' THEN vpi.strnt_nmrtr_val * 1000 --creates miligram value from gram value
#     WHEN unit_num.descr = 'mg' THEN vpi.strnt_nmrtr_val --no change if mg value
#     ELSE NULL -- will give a null value if a non-standard dosage unit - this can then be checked if neccesary
#     END AS strnt_nmrtr_val_mg, --ll listed drugs now in miligram rather than g or mcg
#     CASE WHEN unit_den.descr = 'litre' THEN vpi.strnt_dnmtr_val * 1000 --some denominators listed as litre, so create mililitre value
#     WHEN unit_den.descr = 'ml' THEN vpi.strnt_dnmtr_val --no change if mililitre value
#     ELSE NULL -- will give a null value if a non-stanard dosage unit - this can then be checked if neccesary
#     END AS strnt_dnmtr_val_ml --denominator now in ml
#     FROM 
#     dmd.vpi AS vpi 
#     LEFT JOIN dmd.unitofmeasure AS unit_num ON vpi.strnt_nmrtr_uom = unit_num.cd --join to create text value for numerator unit
#     LEFT JOIN dmd.unitofmeasure AS unit_den ON vpi.strnt_dnmtr_uom = unit_den.cd --join to create text value for denominator unit
# ```

# There is then a main query which calculates the OME dose.
# The main calculation is to multiply the quantity prescribed for each presentation by the OME conversion factor (as defined in a separate table ) and by the mg strength per dose (divided by ml when appropriate) to generate the total OME dose.
# However, there are a number of special cases which need adjustment:
# - Transdermal fentanyl are shown in mcg per hour, and therefore need to be multiplied by the 72 hour dose to get the total OME equivalence
# - Transdermal buprenorphine are shown in mcg per hour, and therefore need to be multiplied by 168 or 96 depending on the strength to get the total OME equivalence
# - Injections need to be multiplied by the ampoule/pfs size in order to get the total OME equivalence.
#
# A concatenated join is used to join all prescribing data to generic VMPs, rather than VMPs and AMPs, in order to reduce the risk of duplication.

# ```sql
# ) 
# SELECT 
#   rx.month, 
#   rx.practice, 
#   rx.pct, 
#   vpi.strnt_dnmtr_val_ml, 
#   sum(rx.quantity) as quantity, 
#   ing.id, --ingredient DM+D code. Combination products will have more than one ing code per VMP, e.g. co-codamol will have ing for paracetamoland codeine
#   ing.nm,--ingredient name
#   rx.bnf_code as bnf_code, --BNF code to link to prescribing data
#   rx.bnf_name as bnf_name, --BNF name from prescribing data
#   vpi.strnt_nmrtr_val_mg, --strength numerator in mg
#   SUM(
#     quantity * ome *(
#       CASE WHEN ing.id = 373492002 
#       AND form.simple_form = 'transdermal' THEN (vpi.strnt_nmrtr_val_mg * 72)/ coalesce(vpi.strnt_dnmtr_val_ml, 1) -- creates 72 hour dose for fentanyl transdermal patches, as doses are per hour on DM+D)
#       WHEN ing.id = 387173000 
#       AND form.simple_form = 'transdermal' 
#       AND vpi.strnt_nmrtr_val IN (5, 10, 15, 20) THEN (vpi.strnt_nmrtr_val_mg * 168)/ coalesce(vpi.strnt_dnmtr_val_ml, 1) -- creates 168 hour (7 day) dose for low-dose buprenorphine patch
#       WHEN ing.id = 387173000 
#       AND form.simple_form = 'transdermal' 
#       AND vpi.strnt_nmrtr_val IN (35, 52.5, 70) THEN (vpi.strnt_nmrtr_val_mg * 96)/ coalesce(vpi.strnt_dnmtr_val_ml, 1) -- creates 96 hour dose for higher-dose buprenorphine patch
#       WHEN form.simple_form = 'injection' THEN (vpi.strnt_nmrtr_val_mg * vmp.udfs)/ coalesce(vpi.strnt_dnmtr_val_ml, 1) -- injections need to be weighted by pack size
#       ELSE strnt_nmrtr_val_mg / coalesce(vpi.strnt_dnmtr_val_ml, 1) --all other products have usual dose - coalesce as solid dose forms do not have a denominator
#       END
#     )
#   ) AS ome_dose, 
#   opioid.ome AS ome 
# FROM 
#   norm_vpi AS vpi --VPI has both ING and VMP codes in the table
#   INNER JOIN dmd.ing AS ing ON vpi.ing = ing.id --join to ING to get ING codes and name
#   INNER JOIN dmd.vmp AS vmp ON vpi.vmp = vmp.id --join to get BNF codes for both VMPs and AMPs joined indirectly TO ING. 
#   INNER JOIN simp_form AS form ON vmp.id = form.vmp --join to subquery for simplified administration route
#   INNER JOIN richard.opioid_class AS opioid ON opioid.id = ing.id AND opioid.form = form.simple_form --join to OME table, which has OME value for ING/route pairs 
#   INNER JOIN hscic.normalised_prescribing AS rx ON CONCAT(
#     SUBSTR(rx.bnf_code, 0, 9), 
#     'AA', 
#     SUBSTR(rx.bnf_code,-2, 2)
#   ) = CONCAT(
#     SUBSTR(vmp.bnf_code, 0, 11), 
#     SUBSTR(vmp.bnf_code,-2, 2)
#   ) --uses bnf code structure to join both branded and generic prescribing data to generic VMP codes - which stops chance of duplication of VMP/AMP names
# WHERE 
#   rx.bnf_code NOT LIKE '0410%' --remove drugs used in opiate dependence
# GROUP BY 
#   rx.month, 
#   rx.practice, 
#   rx.pct, 
#   id, 
#   ing.nm, 
#   rx.bnf_code, 
#   rx.bnf_name, 
#   vpi.strnt_nmrtr_val, 
#   strnt_nmrtr_val_mg, 
#   vpi.strnt_dnmtr_val_ml, 
#   opioid.ome
# ```

# Given the difference in approaches between these two methodologies, it is important to check whether there are major deviations in the data:

##import libraries needed
import pandas as pd
import os as os
import numpy as np
from ebmdatalab import bq, maps, charts
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from IPython.display import display, HTML

# ### Getting the data
#
# Due to the number of rows created in the full analysis, a view was created using the above SQL using the same OME values as per the original methodology:

sql='''
SELECT bnf_code, bnf_name, SUM(quantity) as new_quantity, SUM(ome_dose) AS ome_dose from richard.vw__opioid_total_ome_old_class
where month between '2020-01-01' and '2020-12-01' 
group by bnf_code, bnf_name
'''
df_opioid_total_ome_old_class_dmd = bq.cached_read(sql, csv_path=os.path.join('..','data','df_opioid_total_ome_old_class_dmd.csv'))

# The data for comparison is created by the following SQL, which is a copy of the view in BigQuery currently used in the (suspended) measure.  Both of the analyses use 2020 data for comparison.

sql='''
SELECT
  presc.bnf_name as bnf_name,
  presc.bnf_code as bnf_code,
  SUM(quantity) as old_quantity,
  SUM(quantity*dose_per_unit*ome_multiplier) AS total_ome
FROM
  ebmdatalab.hscic.normalised_prescribing AS presc
JOIN
  ebmdatalab.richard.opioid_measure_revised as opioid
ON CONCAT(
    SUBSTR(presc.bnf_code,0,9),
    'AA',
    SUBSTR(presc.bnf_code,-2,2)
  ) = CONCAT(
    SUBSTR(opioid.bnf_code,0,11),
    SUBSTR(opioid.bnf_code,-2,2)
  )
WHERE month between '2020-01-01' and '2020-12-01'
GROUP BY
  bnf_name,
  bnf_code
'''
df_opioid_total_ome_old_class_measure = bq.cached_read(sql, csv_path=os.path.join('..','data','df_opioid_total_ome_old_class_measure.csv'))

# We can then merge these two dataframes in order to create a single df which can be used to identify differences.
# Differences in calculation can be found by calculating a ratio between the "old" and "new" calculations of OME.  If they are =! 1.0, then there is a difference.

merged = df_opioid_total_ome_old_class_dmd.merge(df_opioid_total_ome_old_class_measure, indicator=True, how='outer') #merge both tables
merged["difference"] = round(merged['ome_dose'],0) - round(merged['total_ome'],0) #calculate total difference of dose between two methodologies.  Rounded due to slightly differences in calculation
merged["difference_ratio"] = merged['ome_dose'] / merged['total_ome'] #calculate ratio between two OME doses
merged["difference_ratio"] = round(merged["difference_ratio"],3) #round to remove small differences

merged.head()

# ### Differences in list of drugs using both methodologies
# We can check whether there are any changes in the list of drugs in the analyses, by filtering as `left_only` for the new dm+d analyses, and `right_only` for the older presentation-based analysis.

#Show drugs which are in dm+d methodology, but not old presentation-based methodology
new_only = merged[merged['_merge'] == 'left_only']
new_only.sort_values(by='ome_dose', ascending=False)

# As can be seen, there are a number of products which have been identified which weren't included in the old methodology.  These all seem to be reasonable drugs which should be included.

#Show drugs which are in dm+d methodology, but not old presentation-based methodology
old_only = merged[merged['_merge'] == 'right_only']
old_only.sort_values(by='total_ome', ascending=False)

# The only missing drug in the new methodology that was in the old methodology is "morphine in intrasite gel", which a) is topical and b) didn't have an OME score in the old version.
#
# Therefore the new dm+d methodology includes all the previous OME presentations, and includes a number of new ones, which will give a more accurate representation of opioid use.

# ### Differences in calculation between methodologies
# It is also important to check whether the calculations are similar between the methdologies.  If presentations in the merged dataset have a `difference ratio` of less or more than 1, the calculation process is showing variance.  If the ratio is 1, then the value is the same between both methodologies.

#filter dataframe for ratios outside of 1
different = merged[merged['difference_ratio'] !=1.000]
different = different[different['_merge'] == "both"]
different = different.sort_values(by='difference_ratio', ascending=False)
display(different)

# As can be seen the only difference in calculation is in fentanyl lozenges.  This is identifable, as the old presentation-based calculation of an OME of 100, whereas the new methodology uses an OME of 130 for all buccal and oramucosal preparations (excluding films).  As the OMEs are being reviewed to take account of equivalency of a number of drugs, including oxycodone, this is not an issue.

# ### Conclusion
# The dm+d methodology provides a number of advantages over the previous methodology.  Calculations show that this methodology mainly replicate the old analyses, whilst adding a number of additional opioids.
#
# Once the current clinical review of opioid codes is finished, we should use this new methodology to reinstate the currently suspended measure.


