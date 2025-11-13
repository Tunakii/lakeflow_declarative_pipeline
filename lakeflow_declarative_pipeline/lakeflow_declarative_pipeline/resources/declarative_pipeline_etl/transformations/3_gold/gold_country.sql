CREATE MATERIALIZED VIEW gold_countries AS
WITH country_language_counts AS (
  SELECT
    language_code,
    COUNT(language_code) AS language_count
  FROM silver_cleaned_countries
  GROUP BY language_code
  ORDER BY language_count DESC
)
SELECT
  s.country_key,
  s.country_name,
  s.country_official,
  s.language_code,
  s.capital,
  s.currency_code,
  c.language_count
FROM silver_cleaned_countries_history s
JOIN country_language_counts c
  ON s.language_code = c.language_code
AND s.population IS NOT NULL;