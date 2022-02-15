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
#month, 
rtrim(presc.bnf_name) as bnf_name, presc.bnf_code, dose_per_unit, sum(quantity) as quantity, round(SUM(quantity*dose_per_unit*ome_multiplier),2) AS total_ome
FROM
  ebmdatalab.hscic.normalised_prescribing AS presc
JOIN
  -- data in richard.opioid_measure comes from:
  -- https://docs.google.com/spreadsheets/d/1IjnHbYVszZKPmVSYydtMVzbDLPOmq8bOFq45QsSu6sE/edit?usp=sharing
  ebmdatalab.richard.opioid_measure as opioid
ON CONCAT(
    SUBSTR(presc.bnf_code,0,9),
    'AA',
    SUBSTR(presc.bnf_code,-2,2)
  ) = CONCAT(
    SUBSTR(opioid.bnf_code,0,11),
    SUBSTR(opioid.bnf_code,-2,2)
  )
GROUP BY
#month,
presc.bnf_name, presc.bnf_code, dose_per_unit
'''
df_opioid_old = bq.cached_read(sql, csv_path=os.path.join('..','data','df_opioid_old.csv'))

df_opioid_old.head()

sql='''
#subquery to create single BNF table for AMPs and VMPs
WITH vmp_amp AS 
(SELECT DISTINCT id,
                 nm,
                 bnf_code
FROM dmd.vmp #vmp table
WHERE bnf_code IS NOT NULL
UNION DISTINCT
SELECT DISTINCT vmp,
                nm,
                bnf_code
FROM dmd.amp #amp table
WHERE bnf_code IS NOT NULL)

, ing_level_rx AS
(select distinct ##distinct needed due to case statement for simple formulation
rx.sha,
rx.regional_team,
rx.stp,
rx.pct,
rx.practice,
rx.bnf_code,
rx.bnf_name,
rx.items,
rx.quantity,

CASE
    WHEN vpi.strnt_nmrtr_uom = 258685003 THEN vpi.strnt_nmrtr_val / 1000 #creates miligram value from mcg value (uom = 258685003)
    WHEN vpi.strnt_nmrtr_uom = 258682000 THEN vpi.strnt_nmrtr_val * 1000 #creates miligram value from gram value (uom = 258682000)
    WHEN vpi.strnt_nmrtr_uom = 258684004 THEN vpi.strnt_nmrtr_val #normal miligram value (uom = 258684004)
    ELSE null
    END AS strnt_nmrtr_val_mg, #all listed drugs now in miligram rather than g or mcg
CASE
    WHEN vpi.strnt_dnmtr_uom = 258770004 THEN vpi.strnt_dnmtr_val * 1000 #some denominators listed as litre, so create mililitre value (uom = 258770004)
    WHEN ing.id=373492002 AND form.descr LIKE '%transdermal%' THEN COALESCE(vpi.strnt_dnmtr_val/72, 1/72) # creates 72 hour dose for fentanyl transdermal patches, as doses are per hour on DM+D)
    WHEN ing.id=387173000 AND form.descr LIKE '%transdermal%' AND strnt_nmrtr_val IN (5, 10, 15, 20) THEN COALESCE(vpi.strnt_dnmtr_val/168, 1/168) # creates 168 hour (7 day) dose for low-dose buprenorphine patch
    WHEN ing.id=387173000 AND form.descr LIKE '%transdermal%' AND strnt_nmrtr_val IN (35, 52.5, 70) THEN COALESCE(vpi.strnt_dnmtr_val/96, 1/96) # creates 96 hour dose for higher-dose buprenorphine patch
    ELSE vpi.strnt_dnmtr_val
    END AS strnt_dnmtr_val_ml, #denominator now in ml
rx.month, 
ing.id as ing, 
ing.nm as ing_name,
COALESCE(udfs, 1) AS unit_dose,
CASE 
    WHEN form.descr LIKE '%injection%' THEN 'injection' #creates "injection" as route, regardless of whether injection or infusion. this also removes injection routes, e.g.
    WHEN descr LIKE '%infusion%' THEN 'injection'  #s/c, i/v etc, AS often injections have many licensed routes, which would multiply the row
        ELSE SUBSTR(form.descr, STRPOS(form.descr,".")+1) #takes the dosage form out of the string (e.g. tablet.oral) TO leave route.
    END AS simple_form 
from `ebmdatalab.richard.opioids_normalised` as rx
INNER JOIN
vmp_amp
on 
rx.bnf_code = vmp_amp.bnf_code
INNER JOIN 
dmd.vpi as vpi
ON
vmp_amp.id = vpi.vmp
INNER JOIN 
dmd.ing as ing
on
vpi.ing = ing.id
INNER JOIN
dmd.ont as ont
ON
vmp_amp.id = ont.vmp
INNER JOIN 
dmd.ontformroute as form
ON
ont.form = form.cd
)

select 
#month, 
bnf_name, 
bnf_code,
round(strnt_nmrtr_val_mg/coalesce(strnt_dnmtr_val_ml,1),3) as dose_per_unit,
sum(quantity) as quantity,
round(SUM(ome*quantity*strnt_nmrtr_val_mg/coalesce(strnt_dnmtr_val_ml, 1)),2) as total_ome#all other products have usual dose - coalesce as solid dose forms do not have a denominator),2) as total_ome
from ing_level_rx as ing_rx
inner join richard.opioid_class as opioid
on
ing_rx.ing = opioid.id and ing_rx.simple_form = opioid.form
group by 
#month, 
bnf_name, bnf_code, ome, dose_per_unit
'''
df_opioid_new = bq.cached_read(sql, csv_path=os.path.join('..','data','df_opioid_new.csv'))

df_opioid_new.head(10)

df_opioid_old.head(20)

# +
#df_opioid_new['dose_per_unit','total_ome'] = pd.to_numeric(df_opioid_new['dose_per_unit','total_ome'])
#df_opioid_old["month"] = pd.to_datetime(df_opioid_old["month"]) 
#df_opioid_new["month"] = pd.to_datetime(df_opioid_new["month"])
#df_opioid_new[['quantity','dose_per_unit','total_ome']] = df_opioid_new[['quantity','dose_per_unit','total_ome']].apply(pd.to_numeric, axis = 1)
#df_opioid_old[['quantity','dose_per_unit','total_ome']] = df_opioid_old[['quantity','dose_per_unit','total_ome']].apply(pd.to_numeric, axis = 1)
# -

pd.concat([df_opioid_old,df_opioid_new]).drop_duplicates(keep=False)

new_df = pd.merge(df_opioid_old,df_opioid_new,  how='outer', on=['bnf_name','bnf_code'])

new_df['ome_diff']=round(new_df['total_ome_x']-new_df['total_ome_y'],1)

new_df['ome_diff_multi']=new_df['total_ome_x']/new_df['total_ome_y']

new_df.head()

diff_df = new_df[new_df.ome_diff != 0]

diff_df.head()

diff_df_

agg_df = diff_df.groupby(['bnf_name', 'bnf_code', 'ome_diff_multi'])['ome_diff'].agg('sum')

agg_df=agg_df.to_frame()

agg_df.sort_values(by='ome_diff_multi', ascending=False)

agg_df.reset_index()

#agg_df = agg_df[agg_df.ome_diff <1 and >-1]
agg_df = agg_df[(agg_df['ome_diff']<1) & (agg_df['ome_diff']<-1)]


display(HTML(agg_df.sort_values(by='ome_diff_multi', ascending=False).to_html()))


