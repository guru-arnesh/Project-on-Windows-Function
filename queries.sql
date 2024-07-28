-- CREATING TABLE
CREATE TABLE bookings (
	booking_id INT PRIMARY KEY,
	listing_name VARCHAR,
	host_id INT,
	host_name VARCHAR(50),
	neighbourhood_group VARCHAR(30),
	neighbourhood VARCHAR(30),	
	latitude DECIMAL(11,8),
	longitude DECIMAL(11,8),
	room_type VARCHAR(30),
	price INT,
	minimum_nights INT,
	num_of_reviews INT,
	last_review DATE,
	reviews_per_month DECIMAL(4,2),
	calculated_host_listings_count INT,
	availability_365 INT
);
----------------------------------------------------------------------------

SELECT * FROM bookings

----------------------------------------------------------------------------

-- Connected to Database using SQL Shell(psql)
-- \COPY bookings FROM 'C:/Users/moche/Downloads/AB_NYC_2019.csv' DELIMITER ',' CSV HEADER ENCODING 'utf8'

----------------------------------------------------------------------------

--Average Price with OVER()
SELECT
booking_id,
bookings.listing_name,
bookings.neighbourhood_group,
AVG (price) OVER ()
FROM bookings;

--Average, minimum, and maximum  price with OVER()
SELECT
booking_id,
bookings.listing_name,
bookings.neighbourhood_group,
AVG (price) OVER (),
MIN (price) OVER (),
MAX (price) OVER ()
FROM bookings;

--Difference from average price with OVER()
SELECT 
booking_id,
listing_name,
neighbourhood_group,
price,
ROUND((AVG(price)OVER()),2) AS avg,
ROUND((price-AVG(price)OVER()),2) AS diff_from_avg
FROM bookings;

--Percent of average price with OVER()
SELECT 
booking_id,
listing_name,
neighbourhood_group,
price,
ROUND((AVG(price)OVER()),2) AS avg_price,
ROUND(((price / AVG(price) OVER())*100),2) AS percent_of_avg_price	
FROM bookings;

----------------------------------------------------------------------------

--PARTITION BY

--PARTITION BY neighbourhood group
SELECT 
booking_id,
listing_name,
price,
neighbourhood_group,
neighbourhood,
ROUND(AVG(price)OVER(PARTITION BY(neighbourhood_group) ORDER BY(neighbourhood_group)),2) AS avg_price_by_neigh_group
FROM bookings;

SELECT DISTINCT neighbourhood_group
FROM bookings
ORDER BY neighbourhood_group;

--PARTITION BY neighbourhood group and neighbourhood
SELECT 
booking_id,
listing_name,
price,
neighbourhood_group,
neighbourhood,
ROUND(AVG(price)OVER(PARTITION BY neighbourhood_group, neighbourhood),2) AS avg_price_by_grp_and_neigh
FROM bookings;

--PARTITION BY neighbourhood delta
SELECT 
booking_id,
listing_name,
price,
neighbourhood_group,
neighbourhood,
ROUND(AVG(price)OVER(PARTITION BY neighbourhood_group, neighbourhood),2) AS avg_price_by_grp_and_neigh,
ROUND((price-AVG(price)OVER (PARTITION BY neighbourhood_group, neighbourhood )),2) AS group_and_neigh_delta
FROM bookings;

----------------------------------------------------------------------------

--ROW NUMBER

--Overall price rank
SELECT 
booking_id,
listing_name,
neighbourhood_group,
neighbourhood,
price,
ROW_NUMBER() OVER(ORDER BY price DESC) AS overall_price_rank
FROM bookings;

--neighbourhood price rank
SELECT 
booking_id,
listing_name,
neighbourhood_group,
neighbourhood,
price,
ROW_NUMBER() OVER(ORDER BY price DESC) AS overall_price_rank,
ROW_NUMBER() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC)
FROM bookings;

--TOP 3 booking prices (by using CASE statement)
SELECT 
booking_id,
listing_name,
neighbourhood_group,
neighbourhood,
price,
ROW_NUMBER() OVER(ORDER BY price DESC) AS overall_price_rank,
ROW_NUMBER() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC),
CASE 
	WHEN ROW_NUMBER() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC) <= 3 THEN 'Yes'
	ELSE 'No'
	END AS TOP3_Flag
FROM bookings;

----------------------------------------------------------------------------

--RANK
SELECT 
booking_id,
listing_name,
neighbourhood_group,
neighbourhood,
price,
ROW_NUMBER() OVER(ORDER BY price DESC) AS overall_price_rank,
RANK() OVER (ORDER BY price DESC) AS overall_price_rank_with_rank,
ROW_NUMBER() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC) AS neigh_group_price_rank,
RANK() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC) AS neigh_group_price_rank_with_rank
FROM bookings;

--DENSE RANK
--HERE IS THE DIFFERENCE BETWEEN ROW_NUMBER, RANK & DENSE_RANK 
SELECT 
booking_id,
listing_name,
neighbourhood_group,
neighbourhood,
price,
ROW_NUMBER() OVER(ORDER BY price DESC) AS overall_price_rank,
RANK() OVER (ORDER BY price DESC) AS overall_price_rank_with_rank,
DENSE_RANK() OVER(ORDER BY price DESC) overall_price_rank_with_dense_rank 
FROM bookings;

----------------------------------------------------------------------------


--LAG(By default its 1 period)
SELECT
booking_id,
listing_name,
host_name,
price,
last_review,
LAG(price) OVER(PARTITION BY host_name ORDER BY last_review)
FROM bookings

--LAG BY 2 periods
SELECT
booking_id,
listing_name,
host_name,
price,
last_review,
LAG(price,2) OVER(PARTITION BY host_name ORDER BY last_review)
FROM bookings

--LEAD by 1 period
SELECT
	booking_id,
	listing_name,
	host_name,
	price,
	last_review,
	LEAD(price) OVER(PARTITION BY host_name ORDER BY last_review)
FROM bookings;

--LEAD by 2 periods
SELECT
	booking_id,
	listing_name,
	host_name,
	price,
	last_review,
	LEAD(price, 2) OVER(PARTITION BY host_name ORDER BY last_review)
FROM bookings;


----------------------------------------------------------------------------

-- Top 3 with subquery to select only the 'Yes' values in the top3_flag column
SELECT * FROM (
	SELECT
		booking_id,
		listing_name,
		neighbourhood_group,
		neighbourhood,
		price,
		ROW_NUMBER() OVER(ORDER BY price DESC) AS overall_price_rank,
		ROW_NUMBER() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC) AS neigh_group_price_rank,
		CASE
			WHEN ROW_NUMBER() OVER(PARTITION BY neighbourhood_group ORDER BY price DESC) <= 3 THEN 'Yes'
			ELSE 'No'
		END AS top3_flag
	FROM bookings
	) a
WHERE top3_flag = 'Yes'