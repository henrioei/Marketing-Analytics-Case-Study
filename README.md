# Case Study #1 - Marketing Campaign

#### Requirement #1 Top 2 Movie Categories for Each Customer
* Top 2 categories based on rental count
#### Requirement #2 Film Recommendations for Top 2 Categories
* Max 3 films per category
* Must not have watched recommended films before
#### Requirement #3 Individual Customer Top Category Insights
* Number of films watched
* Comparison to DVD Rental Company Average
* Top X% Ranking
#### Requirement #4 Individual Customer 2nd Category Insights
* Number of films watched
* Percentage of viewing history
#### Requirement #5 Favorite Actor Insights and Recommendations
* Number of films watched
* Max 3 Film Recommendations
* Previously watched & top 2 category recommendations must not be included
----------------------------------------------------------------------------
## Combining the Datasets
#### Complete Joint Dataset (INNER JOIN)
```sql
DROP TABLE IF EXISTS complete_joint_dataset_with_rental_date;
CREATE TEMP TABLE complete_joint_dataset_with_rental_date AS
SELECT
  rental.inventory_id AS inventory_id,
  rental.rental_id AS rental_id,
  rental.customer_id AS customer_id,
  rental.rental_date,
  inventory.film_id AS film_id,
  film.title AS title,
  film_category.category_id AS category_id,
  category.name AS category_name
FROM
  dvd_rentals.rental
  INNER JOIN dvd_rentals.inventory ON rental.inventory_id = inventory.inventory_id
  INNER JOIN dvd_rentals.film ON inventory.film_id = film.film_id
  INNER JOIN dvd_rentals.film_category ON film.film_id = film_category.film_id
  INNER JOIN dvd_rentals.category ON film_category.category_id = category.category_id;
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/inner-join-dataset.jpg)
#### Complete Joint Dataset (LEFT JOIN)
```sql
DROP TABLE IF EXISTS complete_left_join_dataset;
CREATE TEMP TABLE complete_left_join_dataset AS
SELECT
  rental.customer_id,
  inventory.film_id,
  film.title,
  category.name AS category_name
FROM
  dvd_rentals.rental
  LEFT JOIN dvd_rentals.inventory ON rental.inventory_id = inventory.inventory_id
  LEFT JOIN dvd_rentals.film ON inventory.film_id = film.film_id
  LEFT JOIN dvd_rentals.film_category ON film.film_id = film_category.film_id
  LEFT JOIN dvd_rentals.category ON film_category.category_id = category.category_id;
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/left-join-dataset.jpg)
#### Combining tables and checking to see  if our joined dataset matches between INNER and LEFT JOIN
```sql
(
    SELECT
      'left join' AS join_type,
      COUNT(*) AS final_record_count
    FROM
      complete_joint_dataset_with_rental_date
  )
UNION
  (
    SELECT
      'inner join' AS join_type,
      COUNT(*) AS final_record_count
    FROM
      complete_joint_dataset_with_rental_date
  );
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/inner-left-comparison.jpg)
----------------------------------------------------------------------------
## Identifying the Top 2 Categories Per User
#### Customer Rental Count (t1)
```sql
DROP TABLE IF EXISTS category_rental_counts;
CREATE TEMP TABLE category_rental_counts AS
SELECT
  customer_id,
  category_name,
  COUNT(*) AS rental_count,
  MAX(rental_date) AS latest_rental_date
FROM complete_joint_dataset_with_rental_date
GROUP BY
  customer_id,
  category_name;
 ```
#### Total Customer Rentals (t2)
```sql
DROP TABLE IF EXISTS customer_total_rentals;
CREATE TEMP TABLE customer_total_rentals AS
SELECT
  customer_id,
  SUM(rental_count) AS total_rental_count
FROM category_rental_counts
GROUP BY customer_id;
 ```
#### Average Category Rental Counts (t3)
```sql
DROP TABLE IF EXISTS average_category_rental_counts;
CREATE TEMP TABLE average_category_rental_counts AS
SELECT
  category_name,
  FLOOR(AVG(rental_count)) AS avg_rental_count
FROM category_rental_counts
GROUP BY
  category_name;
 ```
#### Percentile Rank Window Function (t4)
```sql
DROP TABLE IF EXISTS customer_category_percentiles;
CREATE TEMP TABLE customer_category_percentiles AS
SELECT
  customer_id,
  category_name,
  CEILING(
    100 * PERCENT_RANK() OVER (
      PARTITION BY category_name
      ORDER BY rental_count DESC
    )
  ) AS percentile
FROM category_rental_counts;
 ```
#### Combining the Temp Tables
```sql
DROP TABLE IF EXISTS customer_category_joint_table;
CREATE TEMP TABLE customer_category_joint_table AS
SELECT
  t1.customer_id,
  t1.category_name,
  t1.rental_count,
  t1.latest_rental_date,
  t2.total_rental_count,
  t3.avg_rental_count,
  t4.percentile,
  t1.rental_count - t3.avg_rental_count AS average_comparison,
  ROUND(100 * t1.rental_count / t2.total_rental_count) AS category_percentage
FROM category_rental_counts AS t1
INNER JOIN customer_total_rentals AS t2
  ON t1.customer_id = t2.customer_id
INNER JOIN average_category_rental_counts AS t3
  ON t1.category_name = t3.category_name
INNER JOIN customer_category_percentiles AS t4
  ON t1.customer_id = t4.customer_id
  AND t1.category_name = t4.category_name;
 ```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/combined-temp-tables.jpg)
#### Filter Out the Top 2 Categories Per User
```sql
DROP TABLE IF EXISTS top_categories_information;
CREATE TEMP TABLE top_categories_information AS (
-- use a CTE with the ROW_NUMBER() window function implemented
WITH ordered_customer_category_joint_table AS (
  SELECT
    customer_id,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY rental_count DESC, latest_rental_date DESC
    ) AS category_ranking,
    category_name,
    rental_count,
    average_comparison,
    percentile,
    category_percentage
  FROM customer_category_joint_table
)
-- filter out top 2 rows from the CTE for final output
SELECT *
FROM ordered_customer_category_joint_table
WHERE category_ranking <= 2
);
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/filter-out-top-2-categories.jpg)
#### Identifying the Top Category Percentile Per User For User-Category Insights
```sql
DROP TABLE IF EXISTS top_category_percentile;
CREATE TEMP TABLE top_category_percentile AS
WITH calculated_cte AS (
SELECT
  top_categories_information.customer_id,
  top_categories_information.category_name AS top_category_name,
  top_categories_information.rental_count,
  category_rental_counts.category_name,
  top_categories_information.category_ranking,
  PERCENT_RANK() OVER (
    PARTITION BY category_rental_counts.category_name
    ORDER BY category_rental_counts.rental_count DESC
  ) AS raw_percentile_value
FROM category_rental_counts
LEFT JOIN top_categories_information
  ON category_rental_counts.customer_id = top_categories_information.customer_id
)
SELECT
  customer_id,
  category_name,
  rental_count,
  category_ranking,
  CASE
    WHEN ROUND(100 * raw_percentile_value) = 0 THEN 1
    ELSE ROUND(100 * raw_percentile_value)
  END AS percentile
FROM calculated_cte
WHERE
  category_ranking = 1
  AND top_category_name = category_name;
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/insights-top-percentile.jpg)
#### First Category Insights
We are preparing this temp table to provide the following information:
1. Number of times a user rented a certain category of film
2. Comparison to the average number of times a certain category of film has been rented
3. Identifying the user percentile from the comparison
```sql
DROP TABLE IF EXISTS first_category_insights;
CREATE TEMP TABLE first_category_insights AS
SELECT
  base.customer_id,
  base.category_name,
  base.rental_count,
  base.rental_count - average.avg_rental_count AS average_comparison,
  base.percentile
FROM top_category_percentile AS base
LEFT JOIN average_category_rental_counts AS average
  ON base.category_name = average.category_name;
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/first-cat-insights-top-percentile.jpg)
#### Second Category Insights
We are preparing this temp table to provide the following information:
1. Number of times a user rented a certain category of film
2. Comparing the second favorite category of films to a users entire viewing history
```sql
DROP TABLE IF EXISTS second_category_insights;
CREATE TEMP TABLE second_category_insights AS
SELECT
  top_categories_information.customer_id,
  top_categories_information.category_name,
  top_categories_information.rental_count,
  top_categories_information.category_ranking,
  -- need to cast as NUMERIC to avoid INTEGER floor division!
  ROUND(
    100 * top_categories_information.rental_count::NUMERIC / customer_total_rentals.total_rental_count
  ) AS total_percentage
FROM top_categories_information
LEFT JOIN customer_total_rentals
  ON top_categories_information.customer_id = customer_total_rentals.customer_id
WHERE category_ranking = 2;
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/second-cat-insights-top-percentile.jpg)