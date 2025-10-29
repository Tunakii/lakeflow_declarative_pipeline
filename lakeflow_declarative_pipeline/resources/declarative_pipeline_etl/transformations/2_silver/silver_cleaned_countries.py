from pyspark import pipelines as dp
from pyspark.sql.functions import md5, concat_ws, col, expr

@dp.expect_or_drop("country_name_not_null", "country_name IS NOT NULL")
@dp.table
def silver_cleaned_countries():
    cleaned_df = spark.read.table("bronze_countries_metadata")
    return (
        cleaned_df.withColumn(
            "country_key",
            md5(concat_ws("||", *cleaned_df.columns))
        )
        .drop("_rescued_data")
    )

dp.create_streaming_table(name = "silver_cleaned_countries_history")

dp.create_auto_cdc_flow(
    target="silver_cleaned_countries_history",
    source="silver_cleaned_countries",
    keys=["country_key"],
    sequence_by=col("loading_date"),
    except_column_list=["loading_date"],
    stored_as_scd_type="2"
)