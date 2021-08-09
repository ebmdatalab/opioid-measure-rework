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

# blah blah blah

##import libraries needed
import pandas as pd
import os as os
import numpy as np
from ebmdatalab import bq, maps, charts
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec











# ### Code

sql='''
#subquery to create a "simple" administration route.  

WITH
  simp_form AS (
  SELECT
    DISTINCT vmp,
    CASE
      WHEN descr LIKE '%injection%' THEN 'injection' # creates "injection" as route, regardless of whether injection or infusion. This also removes injection routes, e.g. 
      WHEN descr LIKE '%infusion%' THEN 'injection'  # S/C, I/V etc, as often injections have many licensed routes, which would multiply the row
    ELSE
    SUBSTR(form.descr, STRPOS(form.descr,".")+1) #takes the dosage form out of the string (e.g. tablet.oral) to leave route.
  END
    AS simple_form
  FROM
    dmd.ont AS ont # the coded route for dosage form, includes VMP code
  INNER JOIN
    dmd.ontformroute AS form # text description of route
  ON
    form.cd=ont.form)
    
#main query to calculate the OME

SELECT
  ing.id, #ingredient DM+D code.  Combination products will have more than one ing code per VMP, e.g. co-codamol will have ing for paracetamol AND codeine
  ing.nm, #ingredient name
  vmp.nm AS vmp_nm, #VMP code 
  rx.bnf_name, #BNF name from prescribing data
  form.simple_form AS form, #simple route form from subquery above
  SUM(quantity) AS quantity, #quantity from prescribing data
  SUM((quantity)*((vpi.strnt_nmrtr_val)/COALESCE(strnt_dnmtr_val, #calculation of total mg prescribed using VPI table.  Coalesce function used for either solid dose formulation 
        1))) AS mg,                                               #and no demoninator or where there is denominator, e.g. liquid (e.g. 10mg/5ml = 2mg per ml.) Multiplied by quantity.
  SUM((quantity)*((vpi.strnt_nmrtr_val)/COALESCE(strnt_dnmtr_val, #calculation of OME from datalab-generated table.  Multiplies total mg by OME weighting.
        1))*ome.ome) AS ome
FROM
  dmd.vpi AS vpi #VPI has both ING and VMP codes in the table
INNER JOIN
  dmd.ing AS ing #join to ING to get ING codes and name
ON
  vpi.ing=ing.id
INNER JOIN
  dmd.vmp_full AS vmp #join to get BNF codes joined indirectly to ING.
ON
  vpi.vmp=vmp.id
INNER JOIN
  richard.cd_test_data AS rx #prescribing data
ON
  CONCAT(SUBSTR(rx.bnf_code,0,9),'AA',SUBSTR(rx.bnf_code,-2,2)) = CONCAT(SUBSTR(vmp.bnf_code,0,11),SUBSTR(vmp.bnf_code,-2,2)) #joins both generic and brands to VMP using generic check code
INNER JOIN
  simp_form AS form # join to subquery for administration route above
ON
  vmp.id=form.vmp
INNER JOIN
  richard.cd_test_data_ome AS ome #OME table created by datalab
ON
  ing.id=ome.ing #joins to ING for chemical
  AND form.simple_form=ome.form #joins to route
GROUP BY
  ing.id,
  ing.nm,
  vmp.nm,
  rx.bnf_name,
  simple_form
ORDER BY
  vmp_nm
'''
df_opioid = bq.cached_read(sql, csv_path=os.path.join('..','data','df_ccg.csv'))

df_opioid.head(200)


