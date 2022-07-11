{{ config(materialized='table') }}

with Dim_Date as(
SELECT 
  Date(d) AS Date_Key,
  Date(FORMAT_DATE('%F', d)) as full_date,
  EXTRACT(YEAR FROM d) AS year,
  EXTRACT(WEEK FROM d) AS week_of_year,
  EXTRACT(DAY FROM d) AS year_day,
  EXTRACT(YEAR FROM d) AS fiscal_year,
  FORMAT_DATE('%Q', d) as fiscal_qtr,
  EXTRACT(MONTH FROM d) AS month,
  FORMAT_DATE("%Y-%m",  d) as year_month,
  FORMAT_DATE("%Y-%U",  d) as year_week,
  FORMAT_DATE('%B', d) as month_name,
  FORMAT_DATE('%w', d) AS week_day,
  FORMAT_DATE('%A', d) AS day_name,
  (CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN 0 ELSE 1 END) AS day_is_weekday,
FROM (
  SELECT
    *
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2050-01-01', INTERVAL 1 DAY)) AS d )
    )
select * from Dim_Date