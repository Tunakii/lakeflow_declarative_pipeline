from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, IntegerType
from pyspark.sql.functions import col, explode, current_date

bronze_path = spark.conf.get('bronze_path')

@dp.table(comment="This table contains raw JSON data along with the datatypes")
def bronze_countries_metadata():
    schema = StructType([
        StructField("capital", ArrayType(StringType()), False),
        StructField("currencies", MapType(StringType(), StructType([
            StructField("name", StringType(), False),
            StructField("symbol", StringType(), False)
        ])), False),
        StructField("flag", StringType(), False),
        StructField("name", StructType([
            StructField("common", StringType(), False),
            StructField("official", StringType(), False),
            StructField("nativeName", MapType(StringType(), StructType([
                StructField("official", StringType(), False),
                StructField("common", StringType(), False)
            ])), False)
        ]), False),
        StructField("region", StringType(), True),
        StructField("population", IntegerType(), True),
        StructField("_rescued_data", StringType(), True)
    ])
    return (
        spark.readStream.format("cloudFiles")
        .schema(schema)
        .option("cloudFiles.format", "json")
        .load(f"{bronze_path}")
        .select(
            col("name.common").alias("country_name"),
            col("name.official").alias("country_official"),
            explode(col("name.nativeName")).alias("language_code", "native_struct"),
            col("capital")[0].alias("capital"),
            col("flag"),
            col("region"),
            explode(col("currencies")).alias("currency_code", "currency_struct"),
            col("population"),
            col("_rescued_data"),
        )
        .select(
            col("country_name"),
            col("country_official"),
            col("language_code"),
            col("native_struct.common").alias("native_common"),
            col("native_struct.official").alias("native_official"),
            col("capital"),
            col("flag"),
            col("region"),
            col("currency_code"),
            col("currency_struct.name").alias("currency_name"),
            col("currency_struct.symbol").alias("currency_symbol"),
            col("population"),
            col("_rescued_data"),
            current_date().alias("loading_date")
        )
    )