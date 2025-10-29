from pyspark import pipelines as dp
from pyspark.sql.functions import md5, concat_ws

@dp.expect_or_drop("country_name_not_null", "country_name IS NOT NULL")
@dp.table
def silver_cleaned_countries():
    cleaned_df = spark.read.table("bronze_countries_metadata")
    return(
        cleaned_df.withColumn(
            "country_key",
            md5(concat_ws("||", *cleaned_df.columns))
        )
        .drop("_rescued_data")
    )
