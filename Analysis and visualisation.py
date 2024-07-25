# Databricks notebook source
# MAGIC %md
# MAGIC **Mounting datalake storage**

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "4a105edf-840d-4345-af39-b778cb5f42d5",
           "fs.azure.account.oauth2.client.secret": "3Za8Q~Cc_~vPJY3QDfM4wmeM4ESoInOi69NX4b~2",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/3fb2dc83-a18d-427d-a9bc-889aea1e89d2/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://healthcarecd@mimicdatawarehouse.dfs.core.windows.net/",
  mount_point = "/mnt/mimicdatawarehouse/healthcarecd",
  extra_configs = configs)

# COMMAND ----------

storage_account_name = "mimicdatawarehouse"
container_name = "healthcarecd"
mount_point = "/mnt/healthcarecd"

# COMMAND ----------

# List the files in the cleaned_data directory
cleaned_data_path = f"{mount_point}/cleaned_data"
display(dbutils.fs.ls(cleaned_data_path))

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading files into the dataframes**

# COMMAND ----------

# Read the CSV files into DataFrames
df_adm = spark.read.csv(f"{cleaned_data_path}/adm_table.csv", header=True, inferSchema=True)
df_call = spark.read.csv(f"{cleaned_data_path}/callout_table.csv", header=True, inferSchema=True)
df_icu = spark.read.csv(f"{cleaned_data_path}/icustays_table.csv", header=True, inferSchema=True)
df_transfers = spark.read.csv(f"{cleaned_data_path}/transfers_table.csv", header=True, inferSchema=True)


# COMMAND ----------

display(df_adm)

# COMMAND ----------

display(df_call)

# COMMAND ----------

display(df_icu)

# COMMAND ----------

display(df_transfers)

# COMMAND ----------

from pyspark.sql.functions import col,when,to_timestamp,concat_ws,first,year,month,dayofmonth,weekofyear,count
from pyspark.sql.functions import col, to_date, sum as _sum, expr,count,round,countDistinct
from pyspark.sql import functions as F
import pandas as pd
import plotly.express as px

# COMMAND ----------

# MAGIC %md
# MAGIC **Total length of stay according to the admission type**

# COMMAND ----------

df_adm = df_adm.withColumn("length_of_stay_days", 
                           (F.unix_timestamp(F.col("dischtime")) - F.unix_timestamp(F.col("admittime"))) / (60 * 60 * 24))

# Group by admission_type and calculate average length of stay
df_grouped = df_adm.groupBy("admission_type").avg("length_of_stay_days").withColumnRenamed("avg(length_of_stay_days)", "avg_length_of_stay_days")
display(df_grouped)


# COMMAND ----------

df_pandas = df_grouped.toPandas()
fig = px.bar(df_pandas, x='admission_type', y='avg_length_of_stay_days',
             labels={'admission_type': 'Admission Type', 'avg_length_of_stay_days': 'Average Length of Stay (days)'},
             title='Average Length of Stay by Admission Type')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **ICU Stay Duration**

# COMMAND ----------

df_pandas = df_icu.select("subject_id", "los").toPandas()
df_pandas['subject_id'] = df_pandas['subject_id'].astype(str)

# Create the bar chart
fig = px.bar(df_pandas, x='subject_id', y='los', 
             labels={'subject_id': 'Subject ID', 'los': 'Length of Stay (days)'},
             title='ICU stay duration')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Ward utilization**

# COMMAND ----------

# MAGIC %md
# MAGIC Ward utilization by time

# COMMAND ----------

df_transfers = df_transfers.withColumn("duration_hours", (col("ward_outtime").cast("long") - col("ward_intime").cast("long")) / 3600)
df_transfers = df_transfers.withColumn("date", to_date(col("ward_intime")))

# Group by id and ward to find the duration
df_time_spent_per_ward = df_transfers.groupBy("wardid").agg(
    round(_sum("duration_hours"), 2).alias("total_hours_spent")
)
display(df_time_spent_per_ward)

# COMMAND ----------

import matplotlib.pyplot as plt
df_pandas = df_time_spent_per_ward.toPandas()
df_pandas = df_pandas.sort_values(by='wardid')
plt.figure(figsize=(10, 6))
plt.bar(df_pandas['wardid'], df_pandas['total_hours_spent'], color='skyblue')
plt.xlabel('Ward ID')
plt.ylabel('Total Hours Spent')
plt.title('Total Hours Spent in Each Ward')
plt.xticks(df_pandas['wardid'], rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Ward utlization rate

# COMMAND ----------

df_ward_usage = df_transfers.groupBy("wardid").agg(
    count("*").alias("ward_usage_count")
)
display(df_ward_usage)

# COMMAND ----------

total_wards = df_ward_usage.count() 
df_utilization_rate = df_ward_usage.withColumn(
    "utilization_rate",
    col("ward_usage_count") / total_wards
)
#Dataframe showing ward utilization
display(df_utilization_rate)


# COMMAND ----------

import plotly.graph_objs as go
df_utilization_rate = df_utilization_rate.orderBy("wardid")

df_pandas = df_utilization_rate.select("wardid", "utilization_rate").toPandas()
fig = go.Figure()
fig.add_trace(go.Bar(x=df_pandas['wardid'], y=df_pandas['utilization_rate'], name='Utilization Rate'))
fig.update_layout(title='Ward Utilization Rates',
                  xaxis_title='Ward ID',
                  yaxis_title='Utilization Rate',
                  yaxis_tickformat=".2%",
                  bargap=0.2,
                  xaxis={'type': 'category', 'categoryorder': 'array', 'categoryarray': df_pandas['wardid']})

fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC **Time taken by hospital to respond to callout**

# COMMAND ----------

# convert to pandas dataframe and change to datetime
df_call_pandas = df_call.toPandas()
df_call_pandas['createtime'] = pd.to_datetime(df_call_pandas['createtime']).dt.date
df_call_pandas['outcometime'] = pd.to_datetime(df_call_pandas['outcometime']).dt.date

# COMMAND ----------

df_call = df_call.withColumn(
    "time_diff_minutes", 
    (F.unix_timestamp(F.col("outcometime")) - F.unix_timestamp(F.col("createtime"))) / (60*60)
)

df_pandas = df_call.select("subject_id", "time_diff_minutes").toPandas()
df_pandas['subject_id'] = df_pandas['subject_id'].astype(str)

# Create the bar chart
fig = px.bar(df_pandas, x='subject_id', y='time_diff_minutes', 
             labels={'subject_id': 'SUBJECT ID', 'time_diff_minutes': 'Time Difference (hours)'},
             title='Time Taken to respond to a callout')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Callout duration representation

# COMMAND ----------

df_filter=df_call.select("subject_id","hadm_id","createtime","updatetime","acknowledgetime","outcometime")
df_filter=df_filter.limit(5)
display(df_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC Function to extract rows and plot gantt chart

# COMMAND ----------

def generate_gantt_chart_callout(id_value):
    df_filtered = df_call.filter(col('hadm_id') == id_value)
    df_filtered = df_filtered.select('createtime', 'updatetime', 'acknowledgetime', 'outcometime')
    df_pandas = df_filtered.toPandas()

    fig = go.Figure()

    for idx, col_name in enumerate(['createtime', 'updatetime', 'acknowledgetime', 'outcometime']):
        fig.add_trace(go.Bar(x=df_pandas[col_name], y=[f'{col_name}'], orientation='h', name=f'{col_name}'))

    fig.update_layout(title=f'Gantt Chart for ID: {id_value}', barmode='stack', xaxis_title='Time')
    fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Using query system to show chart for specified patient id

# COMMAND ----------

dbutils.widgets.text("hadm_id_callout", "", "Enter hadm_id for callout")
hadm_id1 = dbutils.widgets.get("hadm_id_callout")
if hadm_id1:
    generate_gantt_chart_callout(int(hadm_id1))
else:
    print("Please enter a valid hadm_id.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Time spent in different stages of stay**

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

window_spec = Window.partitionBy("hadm_id").orderBy("ward_intime")
df_transfers = df_transfers.withColumn("row_num", row_number().over(window_spec))

# COMMAND ----------

df_transfers = df_transfers.withColumn("ward_info", 
    concat_ws(";", col("wardid"), col("ward_intime"), col("ward_outtime")))
df_transfers_pivot = df_transfers.groupBy("hadm_id").pivot("row_num").agg(
    first("ward_info")
)

# COMMAND ----------

for i in range(1, df_transfers_pivot.columns.count("hadm_id") - 1):
    df_transfers_pivot = df_transfers_pivot.withColumn(f"wardid_{i}", split(col(f"{i}"), ";").getItem(0)) \
                                           .withColumn(f"intime_{i}", split(col(f"{i}"), ";").getItem(1)) \
                                           .withColumn(f"outtime_{i}", split(col(f"{i}"), ";").getItem(2))

# COMMAND ----------

for i in range(1, df_transfers_pivot.columns.count("hadm_id") - 1):
    df_transfers_pivot = df_transfers_pivot.drop(f"{i}")
df_result = df_adm.join(df_transfers_pivot, on="hadm_id", how="left")

# COMMAND ----------

# The transfers are represented as columns with wardno;intime;outtime format
df_final=df_result.select("subject_id","hadm_id","admittime","1","2","3","4","5","6","7","8","9","dischtime")
display(df_final)

# COMMAND ----------

import pandas as pd
import plotly.express as px
from pyspark.sql.functions import col

def generate_gantt_chart(hadm_id):
    # Filter data for the given hadm_id
    filtered_df = df_final.filter(col("hadm_id") == hadm_id)

    # Select relevant columns and convert to Pandas DataFrame
    columns_to_select = ["subject_id", "hadm_id"] + [str(i) for i in range(1, 10)]
    filtered_df = filtered_df.select(*columns_to_select)
    filtered_pd_df = filtered_df.toPandas()

    # Process ward transfer data and extract timestamps
    def process_ward_data(row):
        data = []
        for i in range(1, 10):
            ward_info = row[str(i)]
            if pd.notnull(ward_info):
                ward_no, ward_intime, ward_outtime = ward_info.split(";")
                ward_intime = pd.to_datetime(ward_intime)
                ward_outtime = pd.to_datetime(ward_outtime)
                data.append({
                    "hadm_id": str(row["hadm_id"]),
                    "Ward": ward_no,
                    "Start": ward_intime,
                    "Finish": ward_outtime,
                    "Start Display": ward_intime.strftime('%Y-%m-%d %H:%M:%S'),
                    "Finish Display": ward_outtime.strftime('%Y-%m-%d %H:%M:%S')
                })
        return data

    # Generate ward data and collect all timestamps
    ward_data = []
    for index, row in filtered_pd_df.iterrows():
        ward_entries = process_ward_data(row)
        ward_data.extend(ward_entries)

    # Convert to DataFrame
    ward_df = pd.DataFrame(ward_data)

    # Print the transformed data for verification
    # print("Transformed Ward Data:")
    # print(ward_df.head())

    # Define distinct wards and assign colors
    distinct_wards = ward_df["Ward"].unique().tolist()
    color_map = {ward: px.colors.qualitative.Plotly[i % len(px.colors.qualitative.Plotly)] for i, ward in enumerate(distinct_wards)}

    # Plot the Gantt chart using Plotly
    fig = px.timeline(
        ward_df,
        x_start="Start",
        x_end="Finish",
        y="hadm_id",
        color="Ward",
        category_orders={"Ward": distinct_wards},
        color_discrete_map=color_map,
        title=f"Patient Ward Transfers for HADM_ID = {hadm_id}"
    )

    # Update layout to show x-axis with datetime formatting
    fig.update_layout(
    xaxis_title='Time',
    yaxis_title='Admission ID',
    xaxis_tickformat='%Y-%m-%d %H:%M:%S',
    xaxis=dict(
        tickvals=[row['Start'] for idx, row in ward_df.iterrows()] + [row['Finish'] for idx, row in ward_df.iterrows()],
        ticktext=[row['Start Display'] for idx, row in ward_df.iterrows()] + [row['Finish Display'] for idx, row in ward_df.iterrows()]
    )
)
    fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Using query system to show time flow of specified patient id

# COMMAND ----------

dbutils.widgets.text("hadm_id_timeflow", "", "Enter hadm_id for timeflow")
hadm_id_input = dbutils.widgets.get("hadm_id_timeflow")

# Check provided input
if hadm_id_input:
    hadm_id = int(hadm_id_input)
    generate_gantt_chart(hadm_id)
else:
    print("Please enter a valid HADM_ID.")

# COMMAND ----------

# MAGIC %md
# MAGIC Showing the careunit names of the wards

# COMMAND ----------

df_wards= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/mimicdatalake/healthcareproject/transfers")
df_wards=df_wards.select("curr_careunit","curr_wardid")
result_df = df_wards.select(
    col("curr_wardid").alias("ward_id"),
    when(col("curr_careunit").isNull(), "general ward").otherwise(col("curr_careunit")).alias("careunit_name")
).filter(
    col("ward_id").isNotNull()
).distinct()
display(result_df)

# COMMAND ----------


