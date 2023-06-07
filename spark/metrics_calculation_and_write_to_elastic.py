import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.types import *
import argparse


os.environ['PYSPARK_PYTHON'] = 'python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3.9'


PATH_COVID_DATASET = '../dataset/covid_dataset.csv'
PATH_STOCKS_DATASET = '../dataset/history/*'
PATH_COVID_SCHEMA = 'configs/covid_dataset_schema.json'
PATH_STOCKS_SCHEMA = 'configs/stocks_dataset_schema.json'

parser = argparse.ArgumentParser()
parser.add_argument("--host")
parser.add_argument("--port")
parser.add_argument("--one_node")
parser.add_argument("--elastic_username")
parser.add_argument("--elastic_password")

args = vars(parser.parse_args())

host = args['host']
port = args['port']
one_node = args["one_node"]
username = args["elastic_username"]
password = args["elastic_password"]

with open(PATH_COVID_SCHEMA) as file:
    schema_covid = StructType.fromJson(json.load(file))

with open(PATH_STOCKS_SCHEMA) as file:
    schema_stocks = StructType.fromJson(json.load(file))

spark = SparkSession.builder.master("local[*]").getOrCreate()

data_for_checking = spark.read.csv(
    './data/datafile.csv',
    inferSchema=True, header=True, sep='|')


data_for_checking = data_for_checking \
    .withColumn("etf", f.when(data_for_checking.ETF == "Y", "etfs")
                .when(data_for_checking.ETF == "N", "stocks")
                .otherwise(data_for_checking.ETF))

df_stocks = spark.read.schema(schema_stocks) \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(PATH_STOCKS_DATASET)

df_covid = spark.read.schema(schema_covid) \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(PATH_COVID_DATASET)

df_covid_to_load = df_covid.drop( "conf_cases", "prob_cases", "pnew_case",
                                  "conf_death", "prob_death", "pnew_death",
                                  "consent_cases", "consent_deaths" )

df_covid_to_load.printSchema()

raw_data_to_load = data_for_checking \
    .join(df_stocks, df_stocks['Ticker'] == data_for_checking['Symbol']) \
    .select(df_stocks["*"], "etf")


# LOADING RAW DATA DATAFRAMES TO TYPES OF INDEXES
raw_data_to_load.write.format("es") \
    .mode('append') \
    .option("es.mapping.date.rich", False) \
    .option('es.resource', '{etf}/_doc') \
    .option("es.nodes", host) \
    .option("es.port", port) \
    .option("es.nodes.wan.only", one_node) \
    .option("es.net.http.auth.user", username) \
    .option("es.net.http.auth.pass", password) \
    .option("es.mapping.exclude", 'etf') \
    .save()


df_covid = df_covid.select("submission_date", "new_case")

df_covid = df_covid \
    .withColumn("submission_date", f.to_date(f.col("submission_date"), "MM/dd/yyyy")) \
    .withColumn("week", f.weekofyear(f.col("submission_date"))) \
    .withColumn("year", f.year(f.col("submission_date")))

# filter by first pandemic week and last week in covid dataset
df_covid = df_covid.where(
    (f.col("submission_date") >= "2020-03-02") &
    (f.col("submission_date") <= '2022-02-16')
)

# calculate incidents_by_week, first by day
df_covid = df_covid.withColumn(
    "incidents_by_week", f.mean(f.col("new_case")).over(
        Window.partitionBy("submission_date", "week", "year"))
)
# second for week with groupBy and agg
df_covid = df_covid \
    .groupBy("week", "year") \
    .agg(f.max("incidents_by_week").alias("week_incidents"))

"""
Add column "ticker" with path to file, transform to name of stock in str format
Filter by first pandemic week and last week in covid dataset
Add cols int(week) and int(year)
Calculate avg stock price by divide highest and lowest price by day
Aggregate avg price and find mean price by week
"""
df_stocks = df_stocks \
    .filter((f.col("Date") >= "2020-03-02") &
            (f.col("Date") <= '2022-02-16')) \
    .withColumn("week", f.weekofyear(f.col("Date"))) \
    .withColumn("year", f.year(f.col("Date"))) \
    .withColumn("avgPrice", f.col("Close")) \
    .groupBy("ticker", "week", "year") \
    .agg(f.mean("avgPrice").alias("avgPrice"))

# join stocks and covid dataframes on the same columns year and week
metrics = df_stocks.join(df_covid, ["year", "week"])

# order by stock price for mean values and order by date for find necessary week
price_window = Window.partitionBy("ticker").orderBy("avgPrice") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
week_window = Window.partitionBy("ticker").orderBy("year", "week") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

"""
Find metrics, use f.first or f.last for first and last row
Use week_window for find first and last weeks calculations
Use price_window for weeks with max and min prices
"""
metrics = metrics.select("ticker",
                         f.first("week").over(week_window).alias("first_week"),
                         f.first("year").over(week_window).alias("first_week_year"),
                         f.first("avgPrice").over(week_window).alias("first_week_value"),
                         f.first("week_incidents").over(week_window).alias("first_week_covid_incidents"),
                         f.last("week").over(price_window).alias("max_week"),
                         f.last("year").over(price_window).alias("max_week_year"),
                         f.last("avgPrice").over(price_window).alias("max_week_value"),
                         f.last("week_incidents").over(price_window).alias("max_week_covid_incidents"),
                         f.first("week").over(price_window).alias("min_week"),
                         f.first("year").over(price_window).alias("min_week_year"),
                         f.first("avgPrice").over(price_window).alias("min_week_value"),
                         f.first("week_incidents").over(price_window).alias("min_week_covid_incidents"),
                         f.last("week").over(week_window).alias("last_week"),
                         f.last("year").over(week_window).alias("last_week_year"),
                         f.last("avgPrice").over(week_window).alias("last_week_value"),
                         f.last("week_incidents").over(week_window).alias("last_week_covid_incidents")
                         )
# get distinct rows with groupBy
metrics = metrics.groupby("ticker").agg(
    f.first("first_week").alias("first_week"),
    f.first("first_week_year").alias("first_week_year"),
    f.first("first_week_value").alias("first_week_value"),
    f.first("first_week_covid_incidents").alias("first_week_covid_incidents"),
    f.first("max_week").alias("max_week"),
    f.first("max_week_year").alias("max_week_year"),
    f.first("max_week_value").alias("max_week_value"),
    f.first("max_week_covid_incidents").alias("max_week_covid_incidents"),
    f.first("min_week").alias("min_week"),
    f.first("min_week_year").alias("min_week_year"),
    f.first("min_week_value").alias("min_week_value"),
    f.first("min_week_covid_incidents").alias("min_week_covid_incidents"),
    f.first("last_week").alias("last_week"),
    f.first("last_week_year").alias("last_week_year"),
    f.first("last_week_value").alias("last_week_value"),
    f.first("last_week_covid_incidents").alias("last_week_covid_incidents")
)

# concat int(week) and int(year) to str(week/year)
metrics = metrics \
    .withColumn("first_week_date", f.concat_ws("/", "first_week", "first_week_year")) \
    .withColumn("max_week_date", f.concat_ws("/", "max_week", "max_week_year")) \
    .withColumn("min_week_date", f.concat_ws("/", "min_week", "min_week_year")) \
    .withColumn("last_week_date", f.concat_ws("/", "last_week", "last_week_year"))
metrics = metrics.select(
    "ticker",
    "first_week_date", "first_week_value", "first_week_covid_incidents",
    "max_week_date", "max_week_value", "max_week_covid_incidents",
    "min_week_date", "min_week_value", "min_week_covid_incidents",
    "last_week_date", "last_week_value", "last_week_covid_incidents"
)

# need for working w/yyyy format date
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# convert str(week/year) to date
metrics = metrics \
    .withColumn("first_week_date",
                f.next_day(f.to_date(f.col("first_week_date"), "ww/YYYY"), "MONDAY")) \
    .withColumn("max_week_date",
                f.next_day(f.to_date(f.col("max_week_date"), "ww/YYYY"), "MONDAY")) \
    .withColumn("min_week_date",
                f.next_day(f.to_date(f.col("min_week_date"), "ww/YYYY"), "MONDAY")) \
    .withColumn("last_week_date",
                f.next_day(f.to_date(f.col("last_week_date"), "ww/YYYY"), "MONDAY"))

# round calculations
metrics = metrics \
    .withColumn("first_week_value",
                f.round(f.col("first_week_value"), 2)) \
    .withColumn("first_week_covid_incidents",
                f.round(f.col("first_week_covid_incidents"), 2)) \
    .withColumn("max_week_value",
                f.round(f.col("max_week_value"), 2)) \
    .withColumn("max_week_covid_incidents",
                f.round(f.col("max_week_covid_incidents"), 2)) \
    .withColumn("min_week_value",
                f.round(f.col("min_week_value"), 2)) \
    .withColumn("min_week_covid_incidents",
                f.round(f.col("min_week_covid_incidents"), 2)) \
    .withColumn("last_week_value",
                f.round(f.col("last_week_value"), 2)) \
    .withColumn("last_week_covid_incidents",
                f.round(f.col("last_week_covid_incidents"), 2))

agg_data_to_load = data_for_checking \
    .join(metrics, metrics['ticker'] == data_for_checking['Symbol']) \
    .select(metrics["*"], "etf")

# converting data type to string type
agg_data_to_load = agg_data_to_load\
    .withColumn("last_week_date", agg_data_to_load["last_week_date"].cast(StringType()))
agg_data_to_load = agg_data_to_load.\
    withColumn("max_week_date", agg_data_to_load["max_week_date"].cast(StringType()))
agg_data_to_load = agg_data_to_load.\
    withColumn("first_week_date", agg_data_to_load["first_week_date"].cast(StringType()))
agg_data_to_load = agg_data_to_load.\
    withColumn("min_week_date", agg_data_to_load["min_week_date"].cast(StringType()))


# DIFFERENCE BETWEEN PRICES IN PERCENTAGE
agg_data_to_load = agg_data_to_load \
    .withColumn('diff_max_first_price', agg_data_to_load.max_week_value * 100 / agg_data_to_load.first_week_value - 100)
agg_data_to_load = agg_data_to_load \
    .withColumn('diff_min_first_price', 100 - agg_data_to_load.min_week_value * 100 / agg_data_to_load.first_week_value)


# LOADING DATAFRAMES TO TYPES OF INDEXES
agg_data_to_load.write.format("es") \
    .mode('append') \
    .option('es.resource', '{etf}_week_agg/_doc') \
    .option("es.mapping.date.rich", "false") \
    .option("timestampFormat", "yyyy-MM-dd") \
    .option("es.nodes", host) \
    .option("es.port", port) \
    .option("es.nodes.wan.only", one_node) \
    .option("es.net.http.auth.user", username) \
    .option("es.net.http.auth.pass", password) \
    .option("es.mapping.exclude", 'etf') \
    .save()


# CHANGING COVID DATA TO LOAD IN INDEX
df_covid_to_load = df_covid_to_load \
    .withColumn('created_at', f.split(df_covid_to_load['created_at'], ' ').getItem(0))
df_covid_to_load = df_covid_to_load \
    .withColumn("submission_date", f.date_format(f.to_date(f.col("submission_date"), "MM/dd/yyyy"), "yyyy-MM-dd"))
df_covid_to_load = df_covid_to_load \
    .withColumn("created_at", f.date_format(f.to_date(f.col("created_at"), "MM/dd/yyyy"), "yyyy-MM-dd"))

# print(agg_data_to_load.loc[agg_data_to_load['ticker'] == 'ALPP'])
agg_data_to_load.where(agg_data_to_load.ticker=='ALPP').show()
agg_data_to_load.where(agg_data_to_load.ticker=='OAS').show()
agg_data_to_load.where(agg_data_to_load.ticker=='MOBQ').show()

# LOADING COVID DATAFRAME TO INDEX
df_covid_to_load.write.format("es") \
    .mode('append') \
    .option('es.resource', 'covid/_doc') \
    .option("es.mapping.date.rich", False) \
    .option("timestampFormat", "yyyy-MM-dd") \
    .option("es.nodes", host) \
    .option("es.port", port) \
    .option("es.nodes.wan.only", one_node) \
    .option("es.net.http.auth.user", username) \
    .option("es.net.http.auth.pass", password) \
    .save()
