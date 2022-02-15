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

##import libraries needed
import pandas as pd
import os as os
import numpy as np
from ebmdatalab import bq, maps, charts
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from IPython.display import display, HTML

sql='''
WITH simp_form AS (
  SELECT 
    vmp, #vmp code
    CASE WHEN descr LIKE '%injection%' THEN 'injection' #creates "injection" as route, regardless of whether injection or infusion. this also removes injection routes, e.g.
    WHEN descr LIKE '%infusion%' THEN 'injection' #s/c, i/v etc, AS often injections have many licensed routes, e.g "solutioninjection.subcutaneous" AND solutioninjection.intramuscular"which would multiply the row
    ELSE SUBSTR(
      form.descr, 
      STRPOS(form.descr, ".")+ 1) #takes the dosage form out of the string (e.g. tablet.oral) TO leave route.
    END AS simple_form 
  FROM 
    dmd.ont AS ont #the coded route for dosage form, includes vmp code 
    INNER JOIN dmd.ontformroute AS form ON form.cd = ont.form #text description of route 
    )


#subquery to normalise strength to mg
,norm_vpi AS (
    SELECT 
    vmp, #vmp code
    ing, #ing code
    strnt_nmrtr_val,#numerator strength value
    strnt_nmrtr_uom,#numerator unit of measurement
    unit_num.descr as num_unit, #numerator unit 
    unit_den.descr as den_unit, #denominator unit
    CASE WHEN unit_num.descr = 'microgram' THEN vpi.strnt_nmrtr_val / 1000 #creates miligram value from mcg value
    WHEN unit_num.descr = 'gram' THEN vpi.strnt_nmrtr_val * 1000 #creates miligram value from gram value
    WHEN unit_num.descr = 'mg' THEN vpi.strnt_nmrtr_val #no change if mg value
    ELSE NULL # will give a null value if a non-stanard dosage unit - this can then be checked if neccesary
    END AS strnt_nmrtr_val_mg, #all listed drugs now in miligram rather than g or mcg
    CASE WHEN unit_den.descr = 'litre' THEN vpi.strnt_dnmtr_val * 1000 #some denominators listed as litre, so create mililitre value
    WHEN unit_den.descr = 'ml' THEN vpi.strnt_dnmtr_val #no change if mililitre value
    ELSE NULL # will give a null value if a non-stanard dosage unit - this can then be checked if neccesary
    END AS strnt_dnmtr_val_ml #denominator now in ml
    FROM 
    dmd.vpi AS vpi 
    LEFT JOIN dmd.unitofmeasure AS unit_num ON vpi.strnt_nmrtr_uom = unit_num.cd #join to create text value for numerator unit
    LEFT JOIN dmd.unitofmeasure AS unit_den ON vpi.strnt_dnmtr_uom = unit_den.cd #join to create text value for denominator unit
) 

#main query to calculate the OME
SELECT 
  rx.month, 
  rx.practice, 
  rx.pct, 
  vpi.strnt_dnmtr_val_ml, 
  sum(rx.quantity) as quantity, 
  ing.id, #ingredient DM+D code. Combination products will have more than one ing code per VMP, e.g. co-codamol will have ing for paracetamoland codeine
  ing.nm,#ingredient name
  vmp.bnf_code as bnf_code, #BNF code to link to prescribing data
  rx.bnf_name as bnf_name, #BNF name from prescribing data
  vpi.strnt_nmrtr_val_mg, #strength numerator in mg
  SUM(
    quantity * ome *(
      CASE WHEN ing.id = 373492002 
      AND form.simple_form = 'transdermal' THEN (vpi.strnt_nmrtr_val_mg * 72)/ coalesce(vpi.strnt_dnmtr_val_ml, 1) # creates 72 hour dose for fentanyl transdermal patches, as doses are per hour on DM+D)
      WHEN ing.id = 387173000 
      AND form.simple_form = 'transdermal' 
      AND vpi.strnt_nmrtr_val IN (5, 10, 15, 20) THEN (vpi.strnt_nmrtr_val_mg * 168)/ coalesce(vpi.strnt_dnmtr_val_ml, 1) # creates 168 hour (7 day) dose for low-dose buprenorphine patch
      WHEN ing.id = 387173000 
      AND form.simple_form = 'transdermal' 
      AND vpi.strnt_nmrtr_val IN (35, 52.5, 70) THEN (vpi.strnt_nmrtr_val_mg * 96)/ coalesce(vpi.strnt_dnmtr_val_ml, 1) # creates 96 hour dose for higher-dose buprenorphine patch
      ELSE strnt_nmrtr_val_mg / coalesce(vpi.strnt_dnmtr_val_ml, 1) #all other products have usual dose - coalesce as solid dose forms do not have a denominator
      END
    )
  ) AS ome_dose, 
  opioid.ome AS ome 
FROM 
  norm_vpi AS vpi #VPI has both ING and VMP codes in the table
  INNER JOIN dmd.ing AS ing ON vpi.ing = ing.id #join to ING to get ING codes and name
  INNER JOIN dmd.vmp AS vmp ON vpi.vmp = vmp.id #join to get BNF codes for both VMPs and AMPs joined indirectly TO ING. 
  INNER JOIN simp_form AS form ON vmp.id = form.vmp #join to subquery for simplified administration route
  INNER JOIN richard.opioid_class AS opioid ON opioid.id = ing.id AND opioid.form = form.simple_form #join to OME table, which has OME value for ING/route pairs
  INNER JOIN hscic.normalised_prescribing AS rx ON CONCAT(
    SUBSTR(rx.bnf_code, 0, 9), 
    'AA', 
    SUBSTR(rx.bnf_code,-2, 2)
  ) = CONCAT(
    SUBSTR(vmp.bnf_code, 0, 11), 
    SUBSTR(vmp.bnf_code,-2, 2)
  ) #uses bnf code structure to join both branded and generic prescribing data to generic VMP codes - which stops chance of duplication of VMP/AMP names
WHERE 
  rx.bnf_code NOT LIKE '0410%' #remove drugs used in opiate dependence
GROUP BY 
  rx.month, 
  rx.practice, 
  rx.pct, 
  id, 
  ing.nm, 
  vmp.bnf_code, 
  rx.bnf_name, 
  vpi.strnt_nmrtr_val, 
  strnt_nmrtr_val_mg, 
  vpi.strnt_dnmtr_val_ml, 
  opioid.ome
'''
df_opioid_new_2002 = bq.cached_read(sql, csv_path=os.path.join('..','data','df_opioid_new_2022.csv'))


