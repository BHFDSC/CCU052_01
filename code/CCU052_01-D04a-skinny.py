# Databricks notebook source
# MAGIC %md # CCU052_01-D04-skinny
# MAGIC  
# MAGIC **Description** This notebook creates the skinny patient table, which includes key patient characteristics.
# MAGIC  
# MAGIC **Authors** Tom Bolton, John Nolan
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07
# MAGIC
# MAGIC **Notes**

# COMMAND ----------

# MAGIC %md # 0. Setup 

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

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

# %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/functions"

# COMMAND ----------

# MAGIC %run "../../../../shds/common/functions"

# COMMAND ----------

# %run "/Workspaces/dars_nic_391419_j3w9t_collab/SHDS/common/skinny_20221113"

# COMMAND ----------

# MAGIC %run "../../../../shds/common/skinny_20221113"

# COMMAND ----------

# MAGIC %md # 1. Parameters

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

# MAGIC %md # 2. Data

# COMMAND ----------

gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
hes_ae  = extract_batch_from_archive(parameters_df_datasets, 'hes_ae')
hes_op  = extract_batch_from_archive(parameters_df_datasets, 'hes_op')

# COMMAND ----------

# MAGIC %md # 3. Create

# COMMAND ----------

# MAGIC %md ## 3.1. Harmonised (unassembled)

# COMMAND ----------

kpc_harmonised = key_patient_characteristics_harmonise(gdppr=gdppr, hes_apc=hes_apc, hes_ae=hes_ae, hes_op=hes_op)

# temp save (~15 minutes)
outName = f'{proj}_tmp_kpc_harmonised'.lower()
kpc_harmonised.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
# spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
kpc_harmonised = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md ## 3.2. Selected (assembled)

# COMMAND ----------

kpc_selected = key_patient_characteristics_select(harmonised=kpc_harmonised)

# COMMAND ----------

# MAGIC %md # 4. Save

# COMMAND ----------

# save
save_table(df=kpc_selected, out_name=f'{param_table_tmp_skinny}', save_previous=True, dbc=dbc)

# repoint
kpc_selected = spark.table(f'{dbc}.{param_table_tmp_skinny}')

# COMMAND ----------

# MAGIC %md # 5. Check

# COMMAND ----------

count_var(kpc_selected, 'PERSON_ID')

# COMMAND ----------

display(kpc_selected)

# COMMAND ----------

# see .\data_checks\"skinny" - for further checks
