CREATE MATERIALIZED VIEW gold_population AS
SELECT
    country_key,
    population
FROM silver_cleaned_countries;