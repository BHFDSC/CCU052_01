# Databricks notebook source
# MAGIC %md # CCU052_01-D05-quality_assurance
# MAGIC  
# MAGIC **Description** This notebook creates the quality assurance table, which indentifies individuals to remove from the analyses due to erroneous or conflicting data, with reference to previous work/coding by CCU002 and Spiros Denaxas.
# MAGIC  
# MAGIC **Authors** Tom Bolton, Fionna Chalmers, Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07 and subseqently CCU003_05-D05-quality_assurance
# MAGIC
# MAGIC **Data Output**
# MAGIC - 'ccu004_01_tmp_quality_assurance': table with flags for individuals meeting exclusion criteria based on each of the ten exclusion rules

# COMMAND ----------

spark.sql('CLEAR CACHE')
spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

# COMMAND ----------

# DBTITLE 1,Functions
# %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

# MAGIC %run "../../../../shds/common/functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC # 0. Parameters

# COMMAND ----------

# MAGIC %run "./CCU052_01-D01-parameters"

# COMMAND ----------

# widgets
dbutils.widgets.removeAll()
dbutils.widgets.text('1 project', project)
dbutils.widgets.text('2 subproject', subproject)
dbutils.widgets.text('3 cohort', cohort)
dbutils.widgets.text('4 version', version)
dbutils.widgets.text('5 proj', proj)

# COMMAND ----------

# MAGIC %md # 1. Data

# COMMAND ----------

# spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_codelist_quality_assurance')
codelist_qa = spark.table(f'{dbc}.{param_table_codelist_qa}')
skinny      = spark.table(f'{dbc}.{param_table_tmp_skinny}')
deaths      = spark.table(f'{dbc}.{param_table_cur_deaths_nodup}')
gdppr       = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
pmeds       = extract_batch_from_archive(parameters_df_datasets, 'pmeds')

# COMMAND ----------

# MAGIC %md # 2. Prepare

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('skinny')
print('---------------------------------------------------------------------------------')
# reduce
_skinny = skinny.select('PERSON_ID', 'DOB', 'SEX')

# check
count_var(_skinny, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('deaths')
print('---------------------------------------------------------------------------------')
# reduce
_deaths = deaths.select('PERSON_ID', 'REG_DATE', 'REG_DATE_OF_DEATH')

# check
count_var(_deaths, 'PERSON_ID'); print()
tmp1 = (
  _deaths
  .withColumn('flag_REG_DATE_OF_DEATH', f.when(f.col('REG_DATE_OF_DEATH').isNotNull(), 1).otherwise(0))
  .withColumn('flag_REG_DATE', f.when(f.col('REG_DATE').isNotNull(), 1).otherwise(0)))
tmpt = tab(tmp1, 'flag_REG_DATE_OF_DEATH', 'flag_REG_DATE', var2_unstyled=1); print()


print('---------------------------------------------------------------------------------')
print('merged')
print('---------------------------------------------------------------------------------')
# merge
_merged = (
  merge(_skinny, _deaths, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'])
  .withColumn('in_deaths', f.when(f.col('_merge') == 'both', 1).otherwise(0))\
  .drop('_merge')); print()

# check
count_var(_merged, 'PERSON_ID'); print()
print(_merged.limit(10).toPandas().to_string()); print()
tmpt = tab(_merged, 'SEX'); print()
tmpt = tabstat(_merged, 'DOB', date=1); print()


print('---------------------------------------------------------------------------------')
print('gdppr')
print('---------------------------------------------------------------------------------')
# check
count_var(gdppr, 'NHS_NUMBER_DEID'); print()

# reduce
_gdppr = (
  gdppr
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'RECORD_DATE', 'CODE')
  .where(f.col('PERSON_ID').isNotNull()))
  
# check
count_var(_gdppr, 'PERSON_ID'); print()
print(_gdppr.limit(10).toPandas().to_string()); print()


print('---------------------------------------------------------------------------------')
print('pmeds')
print('---------------------------------------------------------------------------------')
# check
count_var(pmeds, 'Person_ID_DEID'); print()

# reduce
_pmeds = (
  pmeds
  .select(f.col('Person_ID_DEID').alias('PERSON_ID'), 'ProcessingPeriodDate', 'PrescribedBNFCode')
  .where(f.col('PERSON_ID').isNotNull()))
  
# check
count_var(_pmeds, 'PERSON_ID'); print()
print(_pmeds.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md # 3. Medical conditions / medications

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

# check
tmpt = tab(codelist_qa, 'name', 'terminology', var2_unstyled=1); print()

# check
_list_terms = list(
  codelist_qa
  .select('terminology')
  .distinct()
  .toPandas()['terminology'])
assert set(_list_terms) <= set(['SNOMED', 'BNF'])

# COMMAND ----------

display(codelist_qa)

# COMMAND ----------

codelist_qa_gdppr = codelist_qa.filter(f.col('terminology') == 'SNOMED')

# COMMAND ----------

codelist_qa_pmeds = codelist_qa.filter((f.col('terminology') == 'BNF'))

# COMMAND ----------

# MAGIC %md ## 3.2 Create

# COMMAND ----------

# prepare
gdppr_hx = (
  _gdppr
  .withColumn('DATE', f.when(f.col('DATE').isNull(), f.col('RECORD_DATE')).otherwise(f.col('DATE')))
  .drop(f.col('RECORD_DATE'))
  .where(f.col('DATE').isNotNull())
  .withColumn('CENSOR_DATE_START', f.lit(None))
  .withColumn('CENSOR_DATE_END', f.lit(None)))     

pmeds_hx = (
  _pmeds
  .withColumnRenamed('ProcessingPeriodDate', 'DATE')  
  .withColumnRenamed('PrescribedBNFCode', 'CODE')
  .where(f.col('DATE').isNotNull())
  .withColumn('CENSOR_DATE_START', f.lit(None))
  .withColumn('CENSOR_DATE_END', f.lit(None)))

# dictionary - dataset, codelist, and ordering in the event of tied records
hx_in = {
    'gdppr':   ['gdppr_hx',   'codelist_qa_gdppr', 1]
  , 'pmeds':   ['pmeds_hx',   'codelist_qa_pmeds', 2]
}

# run codelist match and codelist match summary functions
hx, hx_1st, hx_1st_wide = codelist_match(hx_in, _name_prefix=f'hx_')
hx_summ_name, hx_summ_name_code = codelist_match_summ(hx_in, hx)

# prepare
hx_all = hx['all'].drop('CENSOR_DATE_START', 'CENSOR_DATE_END')
hx_1st = hx_1st.drop('CENSOR_DATE_START', 'CENSOR_DATE_END')
hx_1st_wide = hx_1st_wide.drop('CENSOR_DATE_START', 'CENSOR_DATE_END')

# temp save
hx_all = temp_save(df=hx_all, out_name=f'{proj}_tmp_qa_hx_all'); print()
hx_1st = temp_save(df=hx_1st, out_name=f'{proj}_tmp_qa_hx_1st'); print()
hx_1st_wide = temp_save(df=hx_1st_wide, out_name=f'{proj}_tmp_qa_hx_1st_wide'); print()

# COMMAND ----------

# MAGIC %md ## 3.3 Check

# COMMAND ----------

tmpt = tab(hx_1st, 'name'); print()
tmpt = tab(hx_1st_wide, 'hx_pregnancy_flag', 'hx_prostate_cancer_flag'); print()
tmpt = tab(hx_1st_wide, 'hx_hrt_flag','hx_cocp_flag'); print()

# check
# count_var(_hx_1st_wide, 'PERSON_ID'); print()
# count_var(_hx_1st_wide, '_pregnancy'); print()
# count_var(_hx_1st_wide, '_prostate_cancer'); print()
# tmpt = tab(_hx_1st_wide, '_pregnancy_ind', '_prostate_cancer_ind', var2_unstyled=1); print()

# COMMAND ----------

# check
display(hx_1st_wide)

# COMMAND ----------

# check codelist match summary by name
display(hx_summ_name)

# COMMAND ----------

# check codelist match summary by name and code
display(hx_summ_name_code)

# COMMAND ----------

# MAGIC %md ## 3.4 Merge

# COMMAND ----------

_merged = merge(_merged, hx_1st_wide.select('PERSON_ID', 'hx_pregnancy_flag', 'hx_prostate_cancer_flag', 'hx_cocp_flag','hx_hrt_flag'), ['PERSON_ID'], 
                validate='1:1', 
#               Asserting_result compares the merge for the left and both to be equal, where this fails it will be due to patients only occuring in one of the datasets
#               In this specific case it will be patients that exist in pmeds, but not in gdppr
                
#               assert_results=['both', 'left_only'], 
                assert_results=None,
                
#               TB suggested to use keep_results parameter. This parameter will keep only the specified merge indicator, i.e. using 'both' and 'left_only' will filter out 
#               Patients with the merge indicatior of 'right only'
                keep_results = ['both','left_only'],
                indicator=0); print()

# COMMAND ----------

# check
display(_merged)

# COMMAND ----------

# check
tmpt = tab(_merged, 'SEX', 'hx_pregnancy_flag', var2_unstyled=1); print()
tmpt = tab(_merged, 'SEX', 'hx_prostate_cancer_flag', var2_unstyled=1); print()
tmpt = tab(_merged, 'SEX', 'hx_hrt_flag', var2_unstyled=1); print()
tmpt = tab(_merged, 'SEX', 'hx_cocp_flag', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md # 4. Rules

# COMMAND ----------

# MAGIC %md ## 4.1 Prepare

# COMMAND ----------

# ------------------------------------------------------------------------------
# preparation: rule 10 (Patients have all missing record_dates and dates)
# ------------------------------------------------------------------------------
# identify records with null date
_gdppr_null = (
  _gdppr
  .select('PERSON_ID', 'DATE', 'RECORD_DATE')
  .withColumn('_null', f.when((f.col('DATE').isNull()) & (f.col('RECORD_DATE').isNull()), 1).otherwise(0))
)

# check
tmpt = tab(_gdppr_null, '_null'); print()
# 7k / 10bn

# summarise per individual
_gdppr_null_summ = (
  _gdppr_null
  .groupBy('PERSON_ID')
  .agg(
    f.sum(f.when(f.col('_null') == 0, 1).otherwise(0)).alias('_n_gdppr_notnull')
    , f.sum(f.col('_null')).alias('_n_gdppr_null')
  )
  .where(f.col('_n_gdppr_null') > 0)
)

# cache
_gdppr_null_summ.cache().count()

# check
print(_gdppr_null_summ.toPandas().to_string()); print()

# check
tmp = (
  _gdppr_null_summ
  .select('_n_gdppr_null')
  .groupBy()
  .sum()
  .collect()[0][0]
)
print(tmp); print()

# merge
_merged = merge(_merged, _gdppr_null_summ, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); 

# COMMAND ----------

# check
display(_merged)

# COMMAND ----------

# MAGIC %md ## 4.2 Create

# COMMAND ----------

# Rule 1: Year of birth is after the year of death
# Rule 2: Patient does not have mandatory fields completed (nhs_number, sex, Date of birth)
# Rule 3: Year of Birth Predates NHS Established Year or Year is over the Current Date
# Rule 4: Remove those with only null/invalid dates of death
# Rule 5: Remove those where registered date of death before the actual date of death
# Rule 6: Pregnancy/birth codes for men
# Rule 7: HRT codes codes for men
# Rule 8: COCP codes codes for men
# Rule 9: Prostate cancer codes for women
# Rule 10: Patients have all missing record_dates and dates

#IMPORTANT: REMOVED f.trim from line20

_qax = _merged\
  .withColumn('YOB', f.year(f.col('DOB')))\
  .withColumn('YOD', f.year(f.col('REG_DATE_OF_DEATH')))\
  .withColumn('_rule_1', f.when(f.col('YOB') > f.col('YOD'), 1).otherwise(0))\
  .withColumn('_rule_2',\
    f.when(\
      (f.col('SEX').isNull()) | (~f.col('SEX').isin([1,2]))\
      | (f.col('DOB').isNull())\
      | (f.col('PERSON_ID').isNull()) | (f.trim(f.col('PERSON_ID')) == '')\
           #| (f.col('PERSON_ID').isNull()) | (f.col('PERSON_ID') == '') | (f.col('PERSON_ID') == ' ')\
    , 1).otherwise(0)\
  )\
  .withColumn('_rule_3',\
    f.when(\
      (f.col('YOB') < 1793) | (f.col('YOB') > datetime.datetime.today().year)\
    , 1).otherwise(0)\
  )\
  .withColumn('_rule_4',\
    f.when(\
      (f.col('in_deaths') == 1)\
      & (\
        (f.col('REG_DATE_OF_DEATH').isNull())\
        | (f.col('REG_DATE_OF_DEATH') <= f.to_date(f.lit('1900-01-01')))\
        | (f.col('REG_DATE_OF_DEATH') > f.current_date())\
      )\
    , 1).otherwise(0)\
  )\
  .withColumn('_rule_5', f.when(f.col('REG_DATE_OF_DEATH') > f.col('REG_DATE'), 1).otherwise(0))\
  .withColumn('_rule_6', f.when((f.col('SEX') == 1) & (f.col('hx_pregnancy_flag') == 1) , 1).otherwise(0))\
  .withColumn('_rule_7', f.when((f.col('SEX') == 1) & (f.col('hx_hrt_flag') == 1) , 1).otherwise(0))\
  .withColumn('_rule_8', f.when((f.col('SEX') == 1) & (f.col('hx_cocp_flag') == 1) , 1).otherwise(0))\
  .withColumn('_rule_9', f.when((f.col('SEX') == 2) & (f.col('hx_prostate_cancer_flag') == 1) , 1).otherwise(0))\
  .withColumn('_rule_10', f.when((f.col('_n_gdppr_null') > 0) & (f.col('_n_gdppr_notnull') == 0) , 1).otherwise(0))


# row total and concat
_qax = _qax\
  .withColumn('_rule_total', sum([f.col(col) for col in _qax.columns if re.match('^_rule_.*$', col)]))\
  .withColumn('_rule_concat', f.concat(*[f'_rule_{i}' for i in list(range(1, 11))]))

display(_qax)

# COMMAND ----------

# temp save
outName = f'{proj}_tmp_quality_assurance_qax'
_qax.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
_qax = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md ## 4.3 Checks

# COMMAND ----------

# check
count_var(_qax, 'PERSON_ID'); print()
tmpt = tab(_qax, '_rule_total'); print()
tmpt = tab(_qax, '_rule_concat'); print()

# check rule frequency
for i in list(range(1, 11)):
  tmpt = tab(_qax, f'_rule_{i}'); print()

# incase many patterns, can sort by desc below
# tmp = _qax\
#   .groupBy('_rule_concat')\
#   .agg(f.count(f.col('_rule_concat')).alias('n'))\
#   .orderBy(f.desc('n'))
# display(tmp)

# Rule 1: Year of birth is after the year of death
# Rule 2: Patient does not have mandatory fields completed (nhs_number, sex, Date of birth)
# Rule 3: Year of Birth Predates NHS Established Year or Year is over the Current Date
# Rule 4: Remove those with only null/invalid dates of death
# Rule 5: Remove those where registered date of death before the actual date of death
# Rule 6: Pregnancy/birth codes for men
# Rule 7: HRT codes codes for men
# Rule 8: COCP codes codes for men
# Rule 9: Prostate Cancer Codes for women
# Rule 10: Patients have all missing record_dates and dates

# COMMAND ----------

display(_qax)

# COMMAND ----------

# MAGIC %md # 5. Save 

# COMMAND ----------

# reduce columns
tmp1 = (_qax.select(['PERSON_ID'] + [col for col in _qax.columns if col.startswith('_rule_')]))

# recode 0 to null (for purpose of summary table)
# for v in [col for col in tmp1.columns if re.match('^_rule_.*$', col)]:
for v in [f'_rule_{i}' for i in list(range(1, 11))]:
  tmp1 = (tmp1
          .withColumn(v, f.when(f.col(v) == 0, f.lit(None)).otherwise(f.col(v))))

# check
count_var(tmp1, 'PERSON_ID'); print()
print(_merged.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# save
save_table(df=tmp1, out_name=f'{param_table_tmp_qa}', save_previous=True, dbc=dbc)

# repoint
tmp1 = spark.table(f'{dbc}.{param_table_tmp_qa}')
