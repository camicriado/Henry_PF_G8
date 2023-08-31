DROP TABLE IF EXISTS datawarehouse.dim_business;
CREATE TABLE IF NOT EXISTS datawarehouse.dim_business (
  	`business_id`			STRING,
  	`name`					STRING,
  	`address`				STRING,
  	`city`					STRING,
  	`state`					STRING,
  	`postal_code`			STRING,
  	`latitude`				FLOAT64,
  	`longitude`				FLOAT64,
  	`stars`					FLOAT64,
  	`review_count`			INT64,
  	`categories`			STRING,
  	`categoria`				STRING
);	

DROP TABLE IF EXISTS datawarehouse.review;
CREATE TABLE IF NOT EXISTS datawarehouse.review (
  	`user_id`				STRING,
  	`business_id`			STRING,
  	`stars`					FLOAT64,
  	`useful`				INT64,
  	`funny`					INT64,
  	`cool`					INT64,
  	`text`					STRING,
  	`date`					DATE,
  	`sentimiento_score`		FLOAT64
);	

DROP TABLE IF EXISTS datawarehouse.dim_user;
CREATE TABLE IF NOT EXISTS datawarehouse.dim_user (
  	`user_id`				STRING,
  	`name`					STRING,
  	`review_count`			INT64,
  	`useful`				INT64,
  	`funny`					INT64,
  	`cool`					INT64,
  	`average_stars`			FLOAT64
);	

DROP TABLE IF EXISTS datawarehouse.tip;
CREATE TABLE IF NOT EXISTS datawarehouse.tip (
  	`user_id`				STRING,
  	`business_id`			STRING,
  	`text`					STRING,
  	`date`					DATE,
  	`sentimiento_score`		FLOAT64
);	

DROP TABLE IF EXISTS datawarehouse.checkin;
CREATE TABLE IF NOT EXISTS datawarehouse.checkin (
  	`business_id`			STRING,
  	`num_visitas`			INT64
);	

DROP TABLE IF EXISTS datawarehouse.dim_sitios_google;
CREATE TABLE IF NOT EXISTS datawarehouse.dim_sitios_google (
  	`name`					STRING,
  	`address`				STRING,
  	`gmap_id`				STRING,
  	`latitude`				FLOAT64,
  	`longitude`				FLOAT64,
  	`categories`			STRING,
  	`avg_rating`			FLOAT64,
  	`num_of_reviews`		INT64,
  	`categoria`				STRING
);	

DROP TABLE IF EXISTS datawarehouse.review_google;
CREATE TABLE IF NOT EXISTS datawarehouse.review_google (
  	`user_id`				FLOAT64,
  	`name`					STRING,
  	`rating`				INT64,
  	`text`					STRING,
  	`gmap_id`				STRING,
  	`date`					DATE,
  	`estado`				STRING,
  	`sentimiento_score`		FLOAT64
);	

ALTER TABLE datawarehouse.dim_business ADD PRIMARY KEY (`business_id`) NOT ENFORCED;

ALTER TABLE datawarehouse.dim_user ADD PRIMARY KEY (`user_id`) NOT ENFORCED;



