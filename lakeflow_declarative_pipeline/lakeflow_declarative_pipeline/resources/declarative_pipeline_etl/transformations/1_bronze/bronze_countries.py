from pyspark import pipelines as dp

bronze_path = spark.conf.get('bronze_path')

@dp.table(
    comment="This table contains all countries"
)
def bronze_countries():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("inferColumnTypes", "true")
        .option("mergerSchema", "true")
        .option("multiline", "true")
        .load(f"{bronze_path}")
    )
