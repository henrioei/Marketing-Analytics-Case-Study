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
* Complete Joint Dataset(INNER JOIN)
* Complete Joint Dataset (LEFT JOIN)
* Checking to see if our joined dataset matches between INNER and LEFT JOIN

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
  LEFT JOIN dvd_rentals.inventory ON rental.inventory_id = inventory.inventory_id
  LEFT JOIN dvd_rentals.film ON inventory.film_id = film.film_id
  LEFT JOIN dvd_rentals.film_category ON film.film_id = film_category.film_id
  LEFT JOIN dvd_rentals.category ON film_category.category_id = category.category_id;
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/left-join-dataset.jpg)
#### Checking to see if our joined dataset matches between INNER and LEFT JOIN
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
## Creating the Category Recommendations
* Customer Rental Count (t1)
* Total Customer Rentals (t2)
* Average Category Rental Counts (t3)
* Percentile Rank Window Function (t4)
* Combining the Temp Tables
* Filter Out the Top 2 Categories Per User
* Identifying the Top Category Percentile Per User For User-Category Insights
* First Category Insights
* Second Category Insights
* Create Temp Tables to Filter Out Films That Has Been Watched
* Final Category Recommendations

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
* Number of times a user rented a certain category of film
* Comparison to the average number of times a certain category of film has been rented
* Identifying the user percentile from the comparison
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
* Number of times a user rented a certain category of film
* Comparing the second favorite category of films to a users entire viewing history
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
#### Create Temp Tables to Filter Out Films That Has Been Watched
```sql
-- Film Counts

DROP TABLE IF EXISTS film_counts;
CREATE TEMP TABLE film_counts AS
SELECT DISTINCT
  film_id,
  title,
  category_name,
  COUNT(*) OVER (
    PARTITION BY film_id
  ) AS rental_count
FROM complete_joint_dataset_with_rental_date;

-- Category Film Exclusions

DROP TABLE IF EXISTS category_film_exclusions;
CREATE TEMP TABLE category_film_exclusions AS
SELECT DISTINCT
  customer_id,
  film_id
FROM complete_joint_dataset_with_rental_date;
```
#### Final Category Recommendations
* ranked_films_cte is created to provide a recommendation ranking for the titles every customer have not watched per top 2 category for each customer
* Since the requirement only asks for the top 3 film recommendations then we will only select the reco rank of 3 and below for each customer
```sql
DROP TABLE IF EXISTS category_recommendations;
CREATE TEMP TABLE category_recommendations AS
WITH ranked_films_cte AS (
  SELECT
    top_categories_information.customer_id,
    top_categories_information.category_name,
    top_categories_information.category_ranking,
    film_counts.film_id,
    film_counts.title,
    film_counts.rental_count,
    DENSE_RANK() OVER (
      PARTITION BY
        top_categories_information.customer_id,
        top_categories_information.category_ranking
      ORDER BY
        film_counts.rental_count DESC,
        film_counts.title
    ) AS reco_rank
  FROM top_categories_information
  INNER JOIN film_counts
    ON top_categories_information.category_name = film_counts.category_name
  WHERE NOT EXISTS (
    SELECT 1
    FROM category_film_exclusions
    WHERE
      category_film_exclusions.customer_id = top_categories_information.customer_id AND
      category_film_exclusions.film_id = film_counts.film_id
  )
)
SELECT * FROM ranked_films_cte
WHERE reco_rank <= 3;

SELECT *
FROM category_recommendations
WHERE customer_id = 1
ORDER BY category_ranking, reco_rank;
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/final-category-recommendations.jpg)
## Creating a Dataset for Actors and Film
* Actor Joint Table
* Top Actor Counts
* Actor Film Counts
* Actor Film Exclusions
* Final Actor Recommendations

#### Actor Joint Table
```sql
DROP TABLE IF EXISTS actor_joint_dataset;
CREATE TEMP TABLE actor_joint_dataset AS
SELECT
  rental.customer_id,
  rental.rental_id,
  rental.rental_date,
  film.film_id,
  film.title,
  actor.actor_id,
  actor.first_name,
  actor.last_name
FROM dvd_rentals.rental
INNER JOIN dvd_rentals.inventory
  ON rental.inventory_id = inventory.inventory_id
INNER JOIN dvd_rentals.film
  ON inventory.film_id = film.film_id
INNER JOIN dvd_rentals.film_actor
  ON film.film_id = film_actor.film_id
INNER JOIN dvd_rentals.actor
  ON film_actor.actor_id = actor.actor_id;
```
#### Top Actor Counts
* actor_count CTE is to add up the number of times a customer have rented a film that stared a certain actor
* ranked_actor_counts CTE is to add in a ranking of an actor based on how often a customer has rented a film that stared an actor
```sql
DROP TABLE IF EXISTS top_actor_counts;
CREATE TEMP TABLE top_actor_counts AS
WITH actor_counts AS (
  SELECT
    customer_id,
    actor_id,
    first_name,
    last_name,
    COUNT(*) AS rental_count,
    MAX(rental_date) AS latest_rental_date
  FROM actor_joint_dataset
  GROUP BY
    customer_id,
    actor_id,
    first_name,
    last_name
),
ranked_actor_counts AS (
  SELECT
    actor_counts.*,
    DENSE_RANK() OVER (
      PARTITION BY customer_id
      ORDER BY
        rental_count DESC,
        latest_rental_date DESC,
        first_name,
        last_name
    ) AS actor_rank
  FROM actor_counts
)
SELECT
  customer_id,
  actor_id,
  first_name,
  last_name,
  rental_count
FROM ranked_actor_counts
WHERE actor_rank = 1;
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/customer-most-favorite-actor.jpg)
#### Actor Film Counts
```sql
DROP TABLE IF EXISTS actor_film_counts;
CREATE TEMP TABLE actor_film_counts AS
WITH film_counts AS (
  SELECT
    film_id,
    COUNT(DISTINCT rental_id) AS rental_count
  FROM actor_joint_dataset
  GROUP BY film_id
)
SELECT DISTINCT
  actor_joint_dataset.film_id,
  actor_joint_dataset.actor_id,
  actor_joint_dataset.title,
  film_counts.rental_count
FROM actor_joint_dataset
LEFT JOIN film_counts
  ON actor_joint_dataset.film_id = film_counts.film_id;
```
#### Actor Film Exclusions
```sql
DROP TABLE IF EXISTS actor_film_exclusions;
CREATE TEMP TABLE actor_film_exclusions AS
(
  SELECT DISTINCT
    customer_id,
    film_id
  FROM complete_joint_dataset_with_rental_date
)
-- we use a UNION to combine the previously watched and the recommended films!
UNION
(
  SELECT DISTINCT
    customer_id,
    film_id
  FROM category_recommendations
);
```
#### Final Actor Recommendations
```sql
DROP TABLE IF EXISTS actor_recommendations;
CREATE TEMP TABLE actor_recommendations AS
WITH ranked_actor_films_cte AS (
  SELECT
    top_actor_counts.customer_id,
    top_actor_counts.first_name,
    top_actor_counts.last_name,
    top_actor_counts.rental_count,
    actor_film_counts.title,
    actor_film_counts.film_id,
    actor_film_counts.actor_id,
    DENSE_RANK() OVER (
      PARTITION BY
        top_actor_counts.customer_id
      ORDER BY
        actor_film_counts.rental_count DESC,
        actor_film_counts.title
    ) AS reco_rank
  FROM top_actor_counts
  INNER JOIN actor_film_counts
    ON top_actor_counts.actor_id = actor_film_counts.actor_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM actor_film_exclusions
    WHERE
      actor_film_exclusions.customer_id = top_actor_counts.customer_id AND
      actor_film_exclusions.film_id = actor_film_counts.film_id
  )
)
SELECT * FROM ranked_actor_films_cte
WHERE reco_rank <= 3;
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/final-actor-recommendations.jpg)
## Final Data Asset
```sql
DROP TABLE IF EXISTS final_data_asset;
CREATE TEMP TABLE final_data_asset AS
WITH first_category AS (
  SELECT
    customer_id,
    category_name,
    CONCAT(
      'You''ve watched ', rental_count, ' ', category_name,
      ' films, that''s ', average_comparison,
      ' more than the DVD Rental Co average and puts you in the top ',
      percentile, '% of ', category_name, ' gurus!'
    ) AS insight
  FROM first_category_insights
),
second_category AS (
  SELECT
    customer_id,
    category_name,
    CONCAT(
      'You''ve watched ', rental_count, ' ', category_name,
      ' films making up ', total_percentage,
      '% of your entire viewing history!'
    ) AS insight
  FROM second_category_insights
),
top_actor AS (
  SELECT
    customer_id,
    -- use INITCAP to transform names into Title case
    CONCAT(INITCAP(first_name), ' ', INITCAP(last_name)) AS actor_name,
    CONCAT(
      'You''ve watched ', rental_count, ' films featuring ',
      INITCAP(first_name), ' ', INITCAP(last_name),
      '! Here are some other films ', INITCAP(first_name),
      ' stars in that might interest you!'
    ) AS insight
  FROM top_actor_counts
),
adjusted_title_case_category_recommendations AS (
  SELECT
    customer_id,
    INITCAP(title) AS title,
    category_ranking,
    reco_rank
  FROM category_recommendations
),
wide_category_recommendations AS (
  SELECT
    customer_id,
    MAX(CASE WHEN category_ranking = 1  AND reco_rank = 1
      THEN title END) AS cat_1_reco_1,
    MAX(CASE WHEN category_ranking = 1  AND reco_rank = 2
      THEN title END) AS cat_1_reco_2,
    MAX(CASE WHEN category_ranking = 1  AND reco_rank = 3
      THEN title END) AS cat_1_reco_3,
    MAX(CASE WHEN category_ranking = 2  AND reco_rank = 1
      THEN title END) AS cat_2_reco_1,
    MAX(CASE WHEN category_ranking = 2  AND reco_rank = 2
      THEN title END) AS cat_2_reco_2,
    MAX(CASE WHEN category_ranking = 2  AND reco_rank = 3
      THEN title END) AS cat_2_reco_3
  FROM adjusted_title_case_category_recommendations
  GROUP BY customer_id
),
adjusted_title_case_actor_recommendations AS (
  SELECT
    customer_id,
    INITCAP(title) AS title,
    reco_rank
  FROM actor_recommendations
),
wide_actor_recommendations AS (
  SELECT
    customer_id,
    MAX(CASE WHEN reco_rank = 1 THEN title END) AS actor_reco_1,
    MAX(CASE WHEN reco_rank = 2 THEN title END) AS actor_reco_2,
    MAX(CASE WHEN reco_rank = 3 THEN title END) AS actor_reco_3
  FROM adjusted_title_case_actor_recommendations
  GROUP BY customer_id
),
final_output AS (
  SELECT
    t1.customer_id,
    t1.category_name AS cat_1,
    t4.cat_1_reco_1,
    t4.cat_1_reco_2,
    t4.cat_1_reco_3,
    t2.category_name AS cat_2,
    t4.cat_2_reco_1,
    t4.cat_2_reco_2,
    t4.cat_2_reco_3,
    t3.actor_name AS actor,
    t5.actor_reco_1,
    t5.actor_reco_2,
    t5.actor_reco_3,
    t1.insight AS insight_cat_1,
    t2.insight AS insight_cat_2,
    t3.insight AS insight_actor
FROM first_category AS t1
INNER JOIN second_category AS t2
  ON t1.customer_id = t2.customer_id
INNER JOIN top_actor t3
  ON t1.customer_id = t3.customer_id
INNER JOIN wide_category_recommendations AS t4
  ON t1.customer_id = t4.customer_id
INNER JOIN wide_actor_recommendations AS t5
  ON t1.customer_id = t5.customer_id
)
SELECT * FROM final_output;
```
![](https://github.com/henrioei/Marketing-Analytics-Case-Study/blob/main/tables/final-data-asset.jpg)
