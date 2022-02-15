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
SELECT
 presc.bnf_name, presc.bnf_code, sum(quantity) as quantity,
  SUM(quantity*dose_per_unit*ome_multiplier) AS total_ome
FROM
  ebmdatalab.hscic.normalised_prescribing AS presc
JOIN
  ebmdatalab.richard.opioid_measure as opioid
ON CONCAT(
    SUBSTR(presc.bnf_code,0,9),
    'AA',
    SUBSTR(presc.bnf_code,-2,2)
  ) = CONCAT(
    SUBSTR(opioid.bnf_code,0,11),
    SUBSTR(opioid.bnf_code,-2,2)
  )
  where presc.month between '2020-04-01' and '2021-03-01'
GROUP BY
bnf_name, bnf_code
'''
exportfile = os.path.join("..","data","df_opioid_old.csv") #set path for data cache
df_opioid_old = bq.cached_read(sql, csv_path=exportfile, use_cache=False) #save dataframe to csv

df_opioid_old.head()

sql='''
#subquery to create a "simple" administration route. 
WITH 
simp_form AS ( 
SELECT DISTINCT vmp, #vmp code
                CASE 
                    WHEN descr LIKE '%injection%' THEN 'injection' #creates "injection" as route, regardless of whether injection or infusion. this also removes injection routes, e.g.
                    WHEN descr LIKE '%infusion%' THEN 'injection'  #s/c, i/v etc, AS often injections have many licensed routes, which would multiply the row
                    ELSE SUBSTR(form.descr, STRPOS(form.descr,".")+1) #takes the dosage form out of the string (e.g. tablet.oral) TO leave route.
                END AS simple_form 
FROM dmd.ont AS ont #the coded route for dosage form, includes vmp code 
INNER JOIN dmd.ontformroute AS form ON form.cd=ont.form #text description of route 
  )

#subquery to normalise strength to mg
,norm_vpi AS (
SELECT vmp, #vmp code
       ing, #ing code
       strnt_nmrtr_val, #numerator strength value
       strnt_nmrtr_uom, #numerator unit of measurement
       unit_num.descr AS unit_num, #numerator unit 
       unit_den.descr AS unit_den, #denominator unit
       CASE
           WHEN unit_num.descr = 'microgram' THEN vpi.strnt_nmrtr_val / 1000 #creates miligram value from mcg value
           WHEN unit_num.descr = 'gram' THEN vpi.strnt_nmrtr_val * 1000 #creates miligram value from gram value
           ELSE vpi.strnt_nmrtr_val
       END AS strnt_nmrtr_val_mg, #all listed drugs now in miligram rather than g or mcg
       CASE
           WHEN unit_den.descr = 'litre' THEN vpi.strnt_dnmtr_val * 1000 #some denominators listed as litre, so create mililitre value
           ELSE vpi.strnt_dnmtr_val
       END AS strnt_dnmtr_val_ml #denominator now in ml
FROM dmd.vpi AS vpi
LEFT JOIN dmd.unitofmeasure AS unit_num ON vpi.strnt_nmrtr_uom = unit_num.cd #join to create text value for numerator unit
LEFT JOIN dmd.unitofmeasure AS unit_den ON vpi.strnt_dnmtr_uom = unit_den.cd) #join to create text value for denominator unit

#subquery to create single BNF table for AMPs and VMPs commented out to check bnf_codes
#vmp_amp AS 
#(SELECT DISTINCT id,
#                 nm,
#                 bnf_code
#FROM dmd.vmp #vmp table
#WHERE bnf_code IS NOT NULL
#UNION DISTINCT
#SELECT DISTINCT vmp,
#                nm,
#                bnf_code
#FROM dmd.amp #amp table
#WHERE bnf_code IS NOT NULL)
    
#main query to calculate the OME
SELECT vpi.strnt_dnmtr_val_ml,
       sum(rx.quantity) as quantity,
       vmp.unit_dose_uom, 
       vmp.udfs, 
       ing.id, #ingredient DM+D code. Combination products will have more than one ing code per VMP, e.g. co-codamol will have ing for paracetamoland codeine
       bnf.presentation as bnf_name, #ingredient name vmp.bnf_code AS bnf_code,
       #vmp.nm AS vmp_nm, #VMP code
       rx.bnf_code as bnf_code, #BNF code to link to prescribing data
       vpi.strnt_nmrtr_val_mg, #strength numerator in mg
       SUM(quantity*ome*(CASE
           WHEN ing.id=373492002 AND form.simple_form = 'transdermal' THEN (vpi.strnt_nmrtr_val_mg*72)/coalesce(vpi.strnt_dnmtr_val_ml, 1) # creates 72 hour dose for fentanyl transdermal patches, as doses are per hour on DM+D)
           WHEN ing.id=387173000 AND form.simple_form = 'transdermal' AND vpi.strnt_nmrtr_val IN (5, 10, 15, 20) THEN (vpi.strnt_nmrtr_val_mg*168)/coalesce(vpi.strnt_dnmtr_val_ml, 1) # creates 168 hour (7 day) dose for low-dose buprenorphine patch
           WHEN ing.id=387173000 AND form.simple_form = 'transdermal' AND vpi.strnt_nmrtr_val IN (35, 52.5, 70) THEN (vpi.strnt_nmrtr_val_mg*96)/coalesce(vpi.strnt_dnmtr_val_ml, 1) # creates 96 hour dose for higher-dose buprenorphine patch
           WHEN vmp.unit_dose_uom IN (413516001, 3318611000001103, 415818006) THEN (vmp.udfs*strnt_nmrtr_val_mg)/coalesce(vpi.strnt_dnmtr_val_ml, 1) # include unit dose size for ampoule, pre-filled injection and vial
           ELSE strnt_nmrtr_val_mg/coalesce(vpi.strnt_dnmtr_val_ml, 1) #all other products have usual dose - coalesce as solid dose forms do not have a denominator
       END)) AS total_ome, 
       opioid.ome AS ome
FROM norm_vpi AS vpi #VPI has both ING and VMP codes in the table
INNER JOIN dmd.ing AS ing ON vpi.ing=ing.id #join to ING to get ING codes and name
INNER JOIN dmd.vmp AS vmp ON vpi.vmp=vmp.id #join to get BNF codes for both VMPs and AMPs joined indirectly TO ING. 
INNER JOIN simp_form AS form ON vmp.id=form.vmp #join to subquery for simplified administration route
INNER JOIN richard.opioid_class_old AS opioid ON opioid.id=ing.id AND opioid.form=form.simple_form #join to OME table, which has OME value for ING/route pairs
#INNER JOIN hscic.normalised_prescribing AS rx ON rx.bnf_code = vmp.bnf_code
INNER JOIN hscic.normalised_prescribing AS rx ON CONCAT(SUBSTR(rx.bnf_code,0,9),'AA',SUBSTR(rx.bnf_code,-2,2)) = CONCAT(SUBSTR(vmp.bnf_code,0,11),SUBSTR(vmp.bnf_code,-2,2))
INNER JOIN hscic.bnf as bnf ON rx.bnf_code = bnf.presentation_code
WHERE rx.bnf_code NOT LIKE '0410%' #remove drugs used in opiate dependence
and month between '2020-04-01' and '2021-03-01'
#and rx.bnf_code LIKE '040702040AAABAB'
GROUP BY id,
         ing.nm,
         rx.bnf_code,
         bnf.presentation,
         vmp.udfs,
         vpi.strnt_nmrtr_val,
         strnt_nmrtr_val_mg,
         vpi.strnt_dnmtr_val_ml,
         opioid.ome,
         vmp.unit_dose_uom
'''
exportfile = os.path.join("..","data","df_opioid_new.csv") #set path for data cache
df_opioid_new = bq.cached_read(sql, csv_path=exportfile, use_cache=False) #save dataframe to csv

df_opioid_new.head(10)

df_opioid_old.head(20)

# +
df_opioid_new = df_opioid_new.drop(columns=['strnt_dnmtr_val_ml','unit_dose_uom','udfs','id','strnt_nmrtr_val_mg','ome'])


# -

def dataframe_difference(df1, df2, which=None):
    """Find rows which are different between two DataFrames."""
    comparison_df = df1.merge(
        df2,
        indicator=True,
        how='outer'
    )
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]
    #diff_df.to_csv('data/diff.csv')
    return diff_df


dataframe_difference(df_opioid_new, df_opioid_old) 

comparison_df.head(200)

new_df.reset_index()

new_df['ome_diff']=round(new_df['total_ome']-new_df['total_ome'],0)

new_df['ome_diff_multi']=new_df['total_ome']/new_df['total_ome']

new_df.to_csv('differences.csv')

diff_df = new_df[new_df.ome_diff != 0]

diff_df.head()

diff_df_

agg_df = diff_df.groupby(['bnf_name', 'bnf_code', 'ome_diff_multi'])['ome_diff'].agg('sum')

agg_df=agg_df.to_frame()

agg_df.sort_values(by='ome_diff_multi', ascending=False)

agg_df.reset_index()

# +
#agg_df = agg_df[agg_df.ome_diff <1 and >-1]
agg_df = agg_df[(agg_df['ome_diff']<1) & (agg_df['ome_diff']<-1)]
# -


display(HTML(agg_df.sort_values(by='ome_diff_multi', ascending=False).to_html()))


