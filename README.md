# Case Study #1 - Marketing Campaign

*Requirement #1 Top 2 Movie Categories for Each Customer*
*Requirement #2 Film Recommendations for Top 2 Categories*
* Max 3 films per category
* Must not have watched recommended films before
*Requirement #3 Individual Customer Top Category Insights*
* Number of films watched
* Comparison to DVD Rental Company Average
* Top X% Ranking
*Requirement #4 Individual Customer 2nd Category Insights*
* Number of films watched
* Percentage of viewing history
*Requirement #5 Favorite Actor Insights and Recommendations*
* Number of films watched
* Max 3 Film Recommendations
* Previously watched & top 2 category recommendations must not be included
----------------------------------------------------------------------------
## Combining the Datasets
----------------------------------------------------------------------------
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
#### Combining tables and checking to see  if our joined dataset matches between INNER and LEFT JOIN*
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
----------------------------------------------------------------------------
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
