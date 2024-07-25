# Databricks notebook source
files = dbutils.fs.ls("/mnt/mimicdatalake/healthcareproject")
for file in files:
    print(file.path)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the tables into spark dataframes**

# COMMAND ----------

df_adm= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/mimicdatalake/healthcareproject/admissions")
df_call= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/mimicdatalake/healthcareproject/callout")
df_icu= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/mimicdatalake/healthcareproject/icustays")
df_transfers= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/mimicdatalake/healthcareproject/transfers")

# COMMAND ----------

# MAGIC %md
# MAGIC **Dataframe 1: Admissions table**

# COMMAND ----------

display(df_adm)

# COMMAND ----------

# MAGIC %md
# MAGIC Selecting required columns

# COMMAND ----------

df_adm=df_adm.select("subject_id","hadm_id","admittime","dischtime","deathtime","admission_type","admission_location","discharge_location","hospital_expire_flag")

# COMMAND ----------

# MAGIC %md
# MAGIC Checking the datatypes

# COMMAND ----------

df_adm.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Creating dataframe to store entires of death

# COMMAND ----------

df_adm_death=df_adm.filter(df_adm.deathtime.isNotNull())
# display(df_adm_death)
df_adm=df_adm.select("subject_id","hadm_id","admittime","dischtime","admission_type","admission_location","discharge_location","hospital_expire_flag")
display(df_adm)

# COMMAND ----------

from pyspark.sql.functions import col,when,to_timestamp
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for null values

# COMMAND ----------

df=df_adm.filter(df_adm.admittime.isNull())
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for inconsistent data

# COMMAND ----------

value_counts = df_adm.groupBy(col("admission_type")).count()
value_counts.show()

# COMMAND ----------

value_counts = df_adm.groupBy(col("admission_type")).count()
value_counts.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Changing abbreviated terms

# COMMAND ----------

df_adm = df_adm.withColumn(
    "discharge_location",
    when(col("discharge_location") == "SNF", "SKILLED NURSING FACILITY").otherwise(col("discharge_location"))
)
df_adm = df_adm.withColumn(
    "discharge_location",
    when(col("discharge_location") == "ICF", "INTERNATIONAL CLASSIFICATION OF FUNCTIONING").otherwise(col("discharge_location"))
)
display(df_adm)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking timestamp format

# COMMAND ----------

def check_timestamp_adm(df, timestamp_col):
    return df.withColumn(
        "is_valid_timestamp",
        when(to_timestamp(col(timestamp_col), "yyyy-MM-dd HH:mm:ss").isNotNull(), True).otherwise(False)
    )
df_call_checked = check_timestamp_adm(df_adm, "dischtime")
df_filter=df_call_checked.filter(df_call_checked["is_valid_timestamp"]==False)
display(df_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for outliers

# COMMAND ----------

import plotly.express as px
df_pandas = df_adm.toPandas()
fig = px.box(df_pandas, y="hadm_id", title='Boxplot of hadm_id')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dataframe2: Callout table**

# COMMAND ----------

display(df_call)

# COMMAND ----------

# MAGIC %md
# MAGIC selecting required columns

# COMMAND ----------

df_call=df_call.select("subject_id","hadm_id","submit_wardid","curr_wardid","discharge_wardid","callout_outcome","createtime","updatetime","acknowledgetime","outcometime")

# COMMAND ----------

# MAGIC %md
# MAGIC Checking datatypes

# COMMAND ----------

df_call.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for null values

# COMMAND ----------

df=df_call.filter(df_call.discharge_wardid.isNull())
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Filling null values of acknowledgetime

# COMMAND ----------

df_call = df_call.withColumn(
    'acknowledgetime',
    when(col('acknowledgetime').isNull(), col('outcometime'))
    .otherwise(col('acknowledgetime'))
)
display(df_call)

# COMMAND ----------

# MAGIC %md
# MAGIC Filling null values of discharge_wardid

# COMMAND ----------

df_call = df_call.withColumn(
    'discharge_wardid',
    when(col('discharge_wardid').isNull(), col('curr_wardid'))
    .otherwise(col('discharge_wardid'))
)
display(df_call)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for inconsistent data

# COMMAND ----------

value_counts = df_call.groupBy(col("callout_outcome")).count()
display(value_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking timestamp format

# COMMAND ----------

def check_timestamp_format(df, timestamp_col):
    return df.withColumn(
        "is_valid_timestamp",
        when(to_timestamp(col(timestamp_col), "yyyy-MM-dd HH:mm:ss").isNotNull(), True).otherwise(False)
    )
df_call_checked = check_timestamp_format(df_call, "updatetime")
df_filter=df_call_checked.filter(df_call_checked["is_valid_timestamp"]==False)
display(df_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for outliers

# COMMAND ----------

df_pandas = df_call.toPandas()
fig = px.box(df_pandas, y="subject_id", title='Boxplot of subject_id')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dataframe 3: Icustays**

# COMMAND ----------

display(df_icu)

# COMMAND ----------

# MAGIC %md
# MAGIC selecting required columns

# COMMAND ----------

df_icu=df_icu.select("subject_id","hadm_id","icustay_id","first_wardid","last_wardid","intime","outtime","los")

# COMMAND ----------

# MAGIC %md
# MAGIC Checking datatypes

# COMMAND ----------

df_icu.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for null values

# COMMAND ----------

df=df_icu.filter(df_icu.los.isNull())
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking timestamp format

# COMMAND ----------

def check_timestamp_format(df, timestamp_col):
    return df.withColumn(
        "is_valid_timestamp",
        when(to_timestamp(col(timestamp_col), "yyyy-MM-dd HH:mm:ss").isNotNull(), True).otherwise(False)
    )
df_icu_checked = check_timestamp_format(df_icu, "intime")
df_filter=df_icu_checked.filter(df_icu_checked["is_valid_timestamp"]==False)
display(df_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for outliers

# COMMAND ----------

df_pandas = df_icu.toPandas()
fig = px.box(df_pandas, y="hadm_id", title='Boxplot of hadm_id')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dataframe 4: Transfers table**

# COMMAND ----------

display(df_transfers)

# COMMAND ----------

# MAGIC %md
# MAGIC selecting required columns

# COMMAND ----------

df_transfers=df_transfers.select("subject_id","hadm_id","icustay_id","eventtype","prev_wardid","curr_wardid","intime","outtime","los")
display(df_transfers)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking datatypes

# COMMAND ----------

df_transfers.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for null values

# COMMAND ----------

df=df_transfers.filter(df_transfers.outtime.isNull())
display(df)

# COMMAND ----------

df_transfers = df_transfers.withColumnRenamed("intime", "ward_intime")
df_transfers= df_transfers.withColumnRenamed("outtime","ward_outtime")

# COMMAND ----------

# MAGIC %md
# MAGIC Dealing with null values of wards

# COMMAND ----------

def add_ward_column(df):
    return df.withColumn(
        "wardid",
        when(col("eventtype") == "admit", col("curr_wardid"))
        .when(col("eventtype") == "discharge", col("prev_wardid"))
        .when(col("eventtype") == "transfer", col("curr_wardid"))
        .otherwise(None)
    )
df_final=add_ward_column(df_transfers)
display(df_final)

# COMMAND ----------

df_transfers=df_final.select("subject_id","hadm_id","icustay_id","eventtype","wardid","ward_intime","ward_outtime","los")

# COMMAND ----------

# MAGIC %md
# MAGIC Dealing with null values of ward_outtime

# COMMAND ----------

df_transfers = df_transfers.withColumn(
    'ward_outtime',
    when(col('ward_outtime').isNull(), col('ward_intime'))
    .otherwise(col('ward_outtime'))
)
display(df_transfers)

# COMMAND ----------

# MAGIC %md
# MAGIC Null values of los

# COMMAND ----------

df_transfers = df_transfers.withColumn(
    'los',
    when(col('los').isNull(), 0)
    .otherwise(col('los'))
)
display(df_transfers)

# COMMAND ----------

# MAGIC %md
# MAGIC Dealing with null values of icustay_id

# COMMAND ----------

df_fil=df_transfers.filter(df_transfers['icustay_id']=='111111')
df_fil.show()

# COMMAND ----------

df_transfers = df_transfers.withColumn(
    'icustay_id',
    when(col('icustay_id').isNull(), 111111)
    .otherwise(col('icustay_id'))
)
display(df_transfers)

# COMMAND ----------

# MAGIC %md
# MAGIC Checking for inconsistent data

# COMMAND ----------

value_counts = df_transfers.groupBy(col("eventtype")).count()
display(value_counts)

# COMMAND ----------

value_counts = df_transfers.groupBy(col("wardid")).count()
display(value_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC **Mounting the cleaned data into datalake storage**

# COMMAND ----------

# Define your storage account and container
storage_account_name = "mimicdatawarehouse"
container_name = "healthcarecd"
mount_point = "/mnt/healthcarecd"

# Define your service principal credentials
client_id = "4a105edf-840d-4345-af39-b778cb5f42d5"
tenant_id = "3fb2dc83-a18d-427d-a9bc-889aea1e89d2"
client_secret = "3Za8Q~Cc_~vPJY3QDfM4wmeM4ESoInOi69NX4b~2"

# Create the mount point
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "4a105edf-840d-4345-af39-b778cb5f42d5",
  "fs.azure.account.oauth2.client.secret": "3Za8Q~Cc_~vPJY3QDfM4wmeM4ESoInOi69NX4b~2",
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/3fb2dc83-a18d-427d-a9bc-889aea1e89d2/oauth2/token"
}

# Mount the ADLS container to Databricks
dbutils.fs.mount(
  source = f"abfss://healthcarecd@mimicdatawarehouse.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs
)


# COMMAND ----------

file_names = ["adm_table.csv", "callout_table.csv", "icustays_table.csv", "transfers_table.csv"]
dataframes = [df_adm, df_call, df_icu, df_transfers]
output_path = f"{mount_point}/cleaned_data"
for df, file_name in zip(dataframes, file_names):
    df.write.mode("overwrite").csv(f"{output_path}/{file_name}", header=True)



# COMMAND ----------


