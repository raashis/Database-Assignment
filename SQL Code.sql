#Q1 - sql (Raashi)
SELECT category
FROM customer_support
WHERE BINARY category = UPPER(category)  -- only need rows where the category is uppercase
  AND category NOT LIKE '%[^A-Z ]%'  -- filters out anything else too which is not A-Z
GROUP BY category;



#Q2 - sql (Angelina)
SELECT category, COUNT(*) as NumOfRecords FROM customer_support
WHERE category in ( -- filter rows based on specific categories that meet the criteria defined below
	SELECT DISTINCT(category) FROM customer_support -- select distinct categories to avoid duplicates in the result
	WHERE category = BINARY UPPER(category) -- ensure categories are stored in uppercase, case-sensitive comparison
) AND (flags LIKE '%Q%' AND flags LIKE '%W%') -- filter records where the 'flags' column contains both 'Q' and 'W' as it will contain
											  -- records that contained colloquial variation and offensive language.
GROUP BY category; -- group the results by category to count records per category
 
 
 
# Qn 3 - sql (Esmond)
select distinct(Cancelled)
from flight_delay;

select count(distinct(Airline))
from flight_delay;

with 
southwest as 
(
  select *
  from flight_delay
  where Airline = 'Southwest Airlines Co.' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
),
skywest as 
(
  select *
  from flight_delay
  where Airline = 'Skywest Airlines Inc.' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
),
united as 
(
  select *
  from flight_delay
  where Airline = 'United Air Lines Inc.' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
),
us as 
(
  select *
  from flight_delay
  where Airline = 'US Airways Inc.' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
),
delta as 
(
  select *
  from flight_delay
  where Airline = 'Delta Air Lines Inc.' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
),
atlantic as 
(
  select *
  from flight_delay
  where Airline = 'Atlantic Southeast Airlines' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
),
frontier as 
(
  select *
  from flight_delay
  where Airline = 'Frontier Airlines Inc.' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
),
hawaiian as 
(
  select *
  from flight_delay
  where Airline = 'Hawaiian Airlines Inc.' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
),
american_eagle as 
(
  select *
  from flight_delay
  where Airline = 'American Eagle Airlines Inc.' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
),
american as 
(
  select *
  from flight_delay
  where Airline = 'American Airlines Inc.' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
),
alaska as 
(
  select *
  from flight_delay
  where Airline = 'Alaska Airlines Inc.' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
),
jetblue as 
(
  select *
  from flight_delay
  where Airline = 'JetBlue Airways' and (ArrDelay > 0 or DepDelay > 0) and Cancelled = 0
  #limit 2
)
select * from southwest 
union 
select * from skywest
union
select * from united
union
select * from us
union
select * from delta
union
select * from atlantic
union
select * from frontier
union
select * from hawaiian
union
select * from american_eagle
union
select * from american
union
select * from alaska
union
select * from jetblue;

## final method
with
cancel as (
 select 'Cancelation' as Type, flight_delay.*
    from flight_delay
    where Cancelled = 1
),
delay as (
 select 'Delay' as Type, flight_delay.*
    from flight_delay
    where ArrDelay > 0 or DepDelay > 0
)
select * from cancel
union
select * from delay;

## random workings
## notice that there are duplicates, union allows us to automatically removes duplicates
select DayOfWeek, Date, DepTime, ArrTime, UniqueCarrier, Airline, FlightNum, ArrDelay, DepDelay, Cancelled
from flight_delay
group by DayOfWeek, Date, DepTime, ArrTime, UniqueCarrier, Airline, FlightNum, ArrDelay, DepDelay, Cancelled
having count(*) > 1;

select *
from flight_delay
where Date = '28-02-2019' and DepTime = 1854 and ArrTime = 1946 and UniqueCarrier = 'F9';

select *
from flight_delay
where Date = '28-02-2019' and DepTime = 2027 and ArrTime = 2314 and UniqueCarrier = 'F9';




# Q4 - sql (Aditi)

-- Step 1: Create a Common Table Expression (CTE) to calculate monthly delays
WITH MonthlyDelays AS (
    SELECT 
        -- Extract the numeric month (1-12) from the Date column
        MONTH(STR_TO_DATE(Date, '%d-%m-%y')) AS MonthNum,
        
        -- Extract the full name of the month (e.g., "January") from the Date column
        DATE_FORMAT(STR_TO_DATE(Date, '%d-%m-%y'), '%M') AS MonthName,
        
        -- Combine the origin and destination airports into a route string (e.g., "JFK to LAX")
        CONCAT(Origin, ' to ', Dest) AS Route,
        
        -- Count the number of delayed flights for each route
        COUNT(*) AS DelayCount
    FROM 
        flight_delay  -- Source table containing flight delay data
    WHERE 
        DepDelay > 0  -- Only include records where there was a departure delay (DepDelay > 0)
    GROUP BY 
        MonthNum,       -- Group data by numeric month
        MonthName,      -- Group data by month name for readability
        Route           -- Group data by flight route
),

-- Step 2: Create another CTE to rank routes by the number of delays within each month
MaxDelays AS (
    SELECT 
        MonthNum,       -- Include the numeric month for sorting and partitioning
        MonthName,      -- Include the full month name for context in the results
        Route,          -- Include the flight route (e.g., "JFK to LAX")
        DelayCount,     -- Include the total number of delays for this route
        -- Rank the routes based on the number of delays within each month
        RANK() OVER (
            PARTITION BY MonthNum               -- Reset ranking for each month
            ORDER BY DelayCount DESC            -- Rank routes in descending order of delays
        ) AS RouteRank
    FROM 
        MonthlyDelays  -- Use the results of the first CTE
)

-- Step 3: Retrieve the routes with the highest delays for each month
SELECT 
    MonthName,          -- Display the full month name for clarity
    Route,              -- Display the flight route
    DelayCount          -- Display the total number of delays for the top route
FROM 
    MaxDelays           -- Use the ranked data from the second CTE
WHERE 
    RouteRank = 1       -- Select only the top-ranked route for each month
ORDER BY 
    MonthNum;           -- Order the results by the numeric month to preserve chronological order





#Q5 - sql (Chloe)
#This was based on our previous assumption that quarter-on-quarter refers to the changes in the quarters of the same year. (ie Q1 to Q2 represents one quarter-on-quarter change)

WITH quarterly_data AS 
(
    SELECT 
        YEAR(STR_TO_DATE(StockDate,'%m/%d/%Y')) AS Year,
        QUARTER(STR_TO_DATE (StockDate,'%m/%d/%Y')) AS Quarter,
        MAX(High) AS Max_High,
        MIN(Low) AS Min_Low,
        AVG(Price)AS Avg_Price
    FROM sia_stock
    WHERE StockDate LIKE '%2023%'
    GROUP BY YEAR(STR_TO_DATE(StockDate,'%m/%d/%Y')), QUARTER(STR_TO_DATE(StockDate,'%m/%d/%Y'))
)
SELECT 
    q1.Year,q1.Quarter,q1.Max_High,q1.Min_Low,q1.Avg_price,
    (q1.Max_High-COALESCE(q2.Max_High,0))/ NULLIF(COALESCE(q2.Max_High, 0),0)*100 AS Change_Percent_High,
    (q1.Min_Low-COALESCE(q2.Min_Low, 0))/ NULLIF(COALESCE(q2.Min_Low,0),0)*100 AS Change_Percent_Low,
    (q1.Avg_Price-COALESCE(q2.Avg_Price,0))/NULLIF(COALESCE(q2.Avg_Price, 0),0)* 100 AS Change_Percent_Avg
FROM quarterly_data q1
LEFT JOIN quarterly_data q2 ON q1.Year=q2.Year AND q1.Quarter=q2.Quarter+1
ORDER BY q1.Year,q1.Quarter;

#This was based on our current assumption that quarter-on-quarter refers to the changes in the corresponding quarters of the previous year. (ie Q1 of the first year to Q1 of the second year represents one quarter-on-quarter change)

WITH quarterly_data AS 
(
    SELECT 
        YEAR(STR_TO_DATE(StockDate,'%m/%d/%Y')) AS Year,
        QUARTER(STR_TO_DATE(StockDate,'%m/%d/%Y')) AS Quarter,
        MAX(High) AS Max_High,
        MIN(Low) AS Min_Low,
        AVG(Price) AS Avg_Price
    FROM sia_stock
    WHERE StockDate LIKE '%2023%' OR StockDate LIKE '%2022%'
    GROUP BY YEAR(STR_TO_DATE(StockDate,'%m/%d/%Y')), QUARTER(STR_TO_DATE(StockDate,'%m/%d/%Y'))
)
SELECT 
    q1.Year,q1.Quarter,q1.Max_High AS Current_Max_High,q1.Min_Low AS Current_Min_Low,q1.Avg_Price AS Current_Avg_Price,
    (q1.Max_High - COALESCE(q2.Max_High, 0)) / NULLIF(COALESCE(q2.Max_High, 0), 0) * 100 AS Change_Percent_High,
    (q1.Min_Low - COALESCE(q2.Min_Low, 0)) / NULLIF(COALESCE(q2.Min_Low, 0), 0) * 100 AS Change_Percent_Low,
    (q1.Avg_Price - COALESCE(q2.Avg_Price, 0)) / NULLIF(COALESCE(q2.Avg_Price, 0), 0) * 100 AS Change_Percent_Avg
FROM quarterly_data q1
LEFT JOIN quarterly_data q2 
    ON q1.Quarter = q2.Quarter AND q1.Year = 2023 AND q2.Year = 2022
WHERE q1.Year = 2023
ORDER BY q1.Quarter;





#Q6 - sql (Chloe)
SELECT 
    sales_channel,route,count(route) as num_flights,
    AVG(length_of_stay)/AVG(flight_duration) AS avg_los_per_flight_hour,
    AVG(wants_extra_baggage)/AVG(flight_duration) AS avg_baggage_per_flight_hour,
    AVG(wants_preferred_seat)/AVG(flight_duration) AS avg_seat_per_flight_hour,
    AVG(wants_in_flight_meals)/AVG(flight_duration) AS avg_meals_per_flight_hour
FROM customer_booking
GROUP BY sales_channel,route
ORDER BY sales_channel,-num_flights;



# Q7 - sql (Aditi)

-- Step 1: Select and process data
SELECT 
    -- Create a new column `Season` to classify the time of travel into 'Seasonal' or 'Non-Seasonal'
    CASE 
        -- Use the first three characters of the `MonthFlown` column (month abbreviation)
        WHEN SUBSTRING(MonthFlown, 1, 3) IN ('Jun', 'Jul', 'Aug', 'Sep') THEN 'Seasonal'
        -- If the month is not within the specified range, classify it as 'Non-Seasonal'
        ELSE 'Non-Seasonal'
    END AS Season,  -- Alias the result as 'Season' for use in the output

    -- Include the airline name to allow grouping and analysis per airline
    Airline,

    -- Include the class of travel (e.g., Economy, Business, First) for more granular insights
    Class,

    -- Calculate the average seat comfort rating for each group of data
    AVG(SeatComfort) AS AvgSeatComfort,

    -- Calculate the average rating for food and beverages for each group
    AVG(FoodnBeverages) AS AvgFoodnBeverages,

    -- Calculate the average inflight entertainment rating for each group
    AVG(InflightEntertainment) AS AvgInflightEntertainment,

    -- Calculate the average rating for value for money for each group
    AVG(ValueForMoney) AS AvgValueForMoney,

    -- Calculate the overall average rating for the travel experience for each group
    AVG(OverallRating) AS AvgOverallRating

-- Specify the source table that contains airline reviews data
FROM 
    airlines_reviews

-- Step 2: Group the data for aggregation
GROUP BY 
    -- Group by the calculated `Season` column to separate 'Seasonal' and 'Non-Seasonal' data
    Season,  
    
    -- Group by airline to calculate and display metrics specific to each airline
    Airline,  
    
    -- Group by class of travel to differentiate experiences by travel class (e.g., Economy, Business)
    Class  

-- Step 3: Order the results for better readability and analysis
ORDER BY 
    -- Use a `CASE` statement to prioritize 'Seasonal' records over 'Non-Seasonal' records
    CASE 
        -- Assign a sorting value of 1 for 'Seasonal'
        WHEN Season = 'Seasonal' THEN 1  
        -- Assign a sorting value of 2 for 'Non-Seasonal'
        ELSE 2  
    END,
    
    -- Within each season, sort the results alphabetically by airline name
    Airline;






# Q8 - sql (Angelina)

# To look at the specifc aspects of dissatisfaction, we do not need to filter out entries where Recommended = 'no' because even when Recommended = 'yes', there may be certain ratings that were given low scores, indicating dissatisfaction. 
# An example is S Han's review on 20/2/2024
-- SELECT * FROM airlines_reviews WHERE Name = 'S Han' AND ReviewDate = '20/2/2024';

## Looking at Robert Watson's review, it was stated that "The seats on this aircraft are dreadful" and yet SeatComfort was given a rating of 5/5.
-- SELECT * FROM airlines_reviews WHERE name = 'Robert Watson';
## Also, looking at J Hoek's review on 14/9/2023, InflightEntertainment was given a rating of 1 even though nothing about it was stated in the review itself. Therefore, there is some ambiguity seen here.
-- SELECT * FROM airlines_reviews WHERE name = 'J Hoek';
## This means that we should look cross-check the "Reviews" with the respective ratings columns and exclude entries that have contradiction within it or is ambiguous in our analysis.
## The goal is to streamline the complaint detection process by focusing on strong signals of dissatisfaction and minimising noise from ambiguous data.
## This can be done by having the criteria for including the data as a complaint as (1) ratings less than 3, and (2) having associated keywords in the review.

## An exception to this would be the need to cross validate the column for "ValueForMoney". This is because it may be difficult to find keywords to determine from their reviews whether it is value for money or not. 
## Normally, whether it is value for money or not would be inferred from the other complaints they may have.
## For example, a review about a bad seat may not necessarily include the keyword "value" or any similar keywords, but it can be inferred that it is not value for money.
## Nonetheless, it is still worth looking at the value complaints as it takes into account the monetary aspect.

-- SELECT DISTINCT SeatComfort FROM airlines_reviews; 
-- SELECT DISTINCT StaffService FROM airlines_reviews; 
-- SELECT DISTINCT FoodnBeverages FROM airlines_reviews; 
-- SELECT DISTINCT InflightEntertainment FROM airlines_reviews; 
-- SELECT DISTINCT ValueForMoney FROM airlines_reviews; 
## The above ratings are all out of 5

-- Define a Common Table Expression (CTE) to analyse seat comfort complaints
WITH ComfortComplaints AS (
	SELECT Airline, TypeofTraveller, 'Seat Complaints' AS Issue, COUNT(*) AS Frequency
	FROM airlines_reviews
    WHERE SeatComfort < 3 -- this sets a strict criteria for looking at dissatisfaction, avoiding ambiguity. "<=3" was not chose as including a rating of 3 may indicate neutral feelings instead.
    AND (Reviews LIKE '%comfort%' OR Reviews LIKE '%seat%' -- search for keywords related to comfort or seats
        OR SOUNDEX(Reviews) = SOUNDEX('comfort') OR SOUNDEX(Reviews) = SOUNDEX('seat')) -- use SOUNDEX for phonetic matches, ensuring typos in the Reviews column are being considered.
    GROUP BY Airline, TypeofTraveller
),
-- Define a CTE for service-related complaints
ServiceComplaints AS (
	SELECT Airline, TypeofTraveller, 'Service Complaints' AS Issue, COUNT(*) AS Frequency
	FROM airlines_reviews
    WHERE StaffService < 3
    AND (Reviews LIKE '%service%' OR Reviews LIKE '%staff%'
		OR SOUNDEX(Reviews) = SOUNDEX('service') OR SOUNDEX(Reviews) = SOUNDEX('staff')) -- Service complaints were assumed to include staff related services.
    GROUP BY Airline, TypeofTraveller
),
-- Define a CTE for food-related complaints
FoodComplaints AS (
	SELECT Airline, TypeofTraveller, 'Food Complaints' AS Issue, COUNT(*) AS Frequency
	FROM airlines_reviews
    WHERE FoodnBeverages < 3
    AND (Reviews LIKE '%food%' OR Reviews LIKE '%drink%'
		OR SOUNDEX(Reviews) = SOUNDEX('food') OR SOUNDEX(Reviews) = SOUNDEX('drink'))
    GROUP BY Airline, TypeofTraveller
),
-- Define a CTE for inflight entertainment complaints
EntertainmentComplaints AS (
	SELECT Airline, TypeofTraveller, 'Entertainment Complaints' AS Issue, COUNT(*) AS Frequency
	FROM airlines_reviews
    WHERE InflightEntertainment < 3
    AND (Reviews LIKE '%entertainment%' OR SOUNDEX(Reviews) = SOUNDEX('entertainment'))
    GROUP BY Airline, TypeofTraveller
),
-- Define a CTE for complaints about value for money
ValueComplaints AS (
	SELECT Airline, TypeofTraveller, 'Value Complaints' AS Issue, COUNT(*) AS Frequency 
	FROM airlines_reviews
    WHERE ValueForMoney < 3
    GROUP BY Airline, TypeofTraveller
),
-- Define a CTE for complaints about flight delay or cancellation. 
-- Note: Although this was not a rated aspect, it is a common issue in airlines that should be analysed too.
DelayComplaints AS (
	SELECT Airline, TypeofTraveller, 'Delayed or Cancelled Flights Complaints' AS Issue, COUNT(*) AS Frequency
	FROM airlines_reviews
    WHERE (Reviews LIKE '%delay%' OR Reviews LIKE '%cancelled%' OR Reviews LIKE '%postponed%' OR Reviews LIKE '%late%' 
		  OR SOUNDEX(Reviews) = SOUNDEX('delay') OR SOUNDEX(Reviews) = SOUNDEX('cancelled') OR SOUNDEX(Reviews) = SOUNDEX('postponed') OR SOUNDEX(Reviews) = SOUNDEX('late'))
    GROUP BY Airline, TypeofTraveller
)
-- Combine the results of all complaint types using UNION
SELECT * FROM ComfortComplaints
UNION
SELECT * FROM ServiceComplaints
UNION
SELECT * FROM FoodComplaints
UNION    
SELECT * FROM EntertainmentComplaints
UNION
SELECT * FROM ValueComplaints
UNION
SELECT * FROM DelayComplaints
ORDER BY Airline, TypeofTraveller, Frequency DESC; -- By arranging from frequency from highest to lowest frequency, we can highlight top complaints.

   



#Q9 - sql (Raashi) 
-- analysing overall trends where we also made sure to look at if it is verified (cause as explained in our report, unverified are reviews which we are not sure if those passengers have flown w sia or not 
-- also make sure we're only selecting sia
WITH verified_reviews AS (
    SELECT *
    FROM airlines_reviews
    WHERE 
		verified = 'TRUE' AND
		Airline = 'Singapore Airlines'
), 
covid_reviews AS ( -- had to first convert the string to date for the dates, and we also defined our pre-covid and post-covid which
-- we ended up using for the rest of our qn. 
    SELECT 
        *,
        CASE 
        WHEN STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') < '2020-01-01' THEN 'Pre-COVID'
        WHEN STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') >= '2023-01-01' THEN 'Post-COVID'
        ELSE 'During'
    END AS covid_period
    FROM 
        verified_reviews
    WHERE
        STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') < '2020-01-01' OR STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') >= '2023-01-01'
),
Recommended_reviews AS ( -- calculating the proportion of reviews here (sthg we ended up using a lot thruout the qn)
    SELECT
        covid_period,
        SUM(CASE WHEN Recommended = 'yes' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS proportion_Recommended
    FROM
        covid_reviews
    GROUP BY
        covid_period
),
overall_ratings AS ( -- overall ratings is sthg we also used thruout our analysis, gives a broad idea of the avg ratings
    SELECT
        covid_period,
        AVG(OverallRating) AS avg_overall_rating,
        AVG(ValueForMoney) AS avg_value_for_money
    FROM
        covid_reviews
    GROUP BY
        covid_period
), 
average_ratings AS (
    SELECT
        covid_period,
        AVG(SeatComfort) AS avg_seat_comfort,
        AVG(StaffService) AS avg_staff_service,
        AVG(FoodnBeverages) AS avg_food_n_beverages,
        AVG(InflightEntertainment) AS avg_inflight_entertainment
    FROM
        covid_reviews
    GROUP BY
        covid_period
)
SELECT 
    cr.covid_period,
    rr.proportion_Recommended,
    oratings.avg_overall_rating,
    oratings.avg_value_for_money,
    ar.avg_seat_comfort,
    ar.avg_staff_service,
    ar.avg_food_n_beverages,
    ar.avg_inflight_entertainment
FROM 
    covid_reviews cr
JOIN
    Recommended_reviews rr ON rr.covid_period = cr.covid_period
JOIN 
    overall_ratings oratings ON cr.covid_period = oratings.covid_period
JOIN 
    average_ratings ar ON cr.covid_period = ar.covid_period
GROUP BY 
    cr.covid_period
ORDER BY 
    cr.covid_period;

-- further categorising by class here

WITH verified_reviews AS (
    SELECT *
    FROM airlines_reviews
    WHERE verified = 'TRUE' AND
		Airline = 'Singapore Airlines'
), 
covid_reviews AS (
    SELECT 
        *,
         CASE 
			WHEN STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') < '2020-01-01' THEN 'Pre-COVID'
			WHEN STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') >= '2023-01-01' THEN 'Post-COVID'
			ELSE 'During'
		END AS covid_period
    FROM 
        verified_reviews
    WHERE
        STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') < '2020-01-01' OR STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') >= '2023-01-01'
),
Recommended_reviews AS (
    SELECT
        Class,
        covid_period,
        SUM(CASE WHEN Recommended = 'yes' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS proportion_Recommended
    FROM
        covid_reviews
    GROUP BY
        Class,
        covid_period
),
overall_ratings AS (
    SELECT
        Class,
        covid_period,
        AVG(OverallRating) AS avg_overall_rating,
        AVG(ValueForMoney) AS avg_value_for_money
    FROM
        covid_reviews
    GROUP BY
        Class,
        covid_period
), 
average_ratings AS (
    SELECT
        Class,
        covid_period,
        AVG(SeatComfort) AS avg_seat_comfort,
        AVG(StaffService) AS avg_staff_service,
        AVG(FoodnBeverages) AS avg_food_n_beverages,
        AVG(InflightEntertainment) AS avg_inflight_entertainment
    FROM
        covid_reviews
    GROUP BY
        Class,
        covid_period
)
SELECT 
    cr.Class,
    cr.covid_period,
    rr.proportion_Recommended,
    oratings.avg_overall_rating,
    oratings.avg_value_for_money,
    ar.avg_seat_comfort,
    ar.avg_staff_service,
    ar.avg_food_n_beverages,
    ar.avg_inflight_entertainment
FROM 
    covid_reviews cr
JOIN
    Recommended_reviews rr ON rr.covid_period = cr.covid_period AND rr.Class = cr.Class
JOIN 
    overall_ratings oratings ON oratings.covid_period = cr.covid_period AND oratings.Class = cr.Class
JOIN 
    average_ratings ar ON ar.covid_period = cr.covid_period AND ar.Class = cr.Class
GROUP BY 
    cr.Class,
    cr.covid_period
ORDER BY 
    cr.Class,
    cr.covid_period;

-- categorising economy and business Class travellers by TypeOfTraveller (since theyre the bulk of the travellers in sia)

WITH verified_reviews AS (
    SELECT *
    FROM airlines_reviews
    WHERE 
		verified = 'TRUE' AND
        Class IN ('Business Class', 'Economy Class') AND
        Airline = 'Singapore Airlines'
), 
covid_reviews AS (
    SELECT 
        *,
         CASE 
			WHEN STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') < '2020-01-01' THEN 'Pre-COVID'
			WHEN STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') >= '2023-01-01' THEN 'Post-COVID'
			ELSE 'During'
        END AS covid_period
    FROM 
        verified_reviews
    WHERE
       STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') < '2020-01-01' OR STR_TO_DATE(CONCAT(MonthFlown, '-01'), '%b-%y-%d') >= '2023-01-01'
),
Recommended_reviews AS (
    SELECT
        TypeofTraveller,
        covid_period,
        SUM(CASE WHEN Recommended = 'yes' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS proportion_Recommended
    FROM
        covid_reviews
    GROUP BY
        TypeofTraveller,
        covid_period
),
overall_ratings AS (
    SELECT
        TypeofTraveller,
        covid_period,
        AVG(OverallRating) AS avg_overall_rating,
        AVG(ValueForMoney) AS avg_value_for_money
    FROM
        covid_reviews
    GROUP BY
        TypeofTraveller,
        covid_period
), 
average_ratings AS (
    SELECT
        TypeofTraveller,
        covid_period,
        AVG(SeatComfort) AS avg_seat_comfort,
        AVG(StaffService) AS avg_staff_service,
        AVG(FoodnBeverages) AS avg_food_n_beverages,
        AVG(InflightEntertainment) AS avg_inflight_entertainment
    FROM
        covid_reviews
    GROUP BY
        TypeofTraveller,
        covid_period
)
SELECT 
    cr.TypeofTraveller,
    cr.covid_period,
    rr.proportion_Recommended,
    oratings.avg_overall_rating,
    oratings.avg_value_for_money,
    ar.avg_seat_comfort,
    ar.avg_staff_service,
    ar.avg_food_n_beverages,
    ar.avg_inflight_entertainment
FROM 
    covid_reviews cr
JOIN
    Recommended_reviews rr ON rr.covid_period = cr.covid_period AND rr.TypeofTraveller = cr.TypeofTraveller
JOIN 
    overall_ratings oratings ON oratings.covid_period = cr.covid_period AND oratings.TypeofTraveller = cr.TypeofTraveller
JOIN 
    average_ratings ar ON ar.covid_period = cr.covid_period AND ar.TypeofTraveller = cr.TypeofTraveller
GROUP BY 
    cr.TypeofTraveller,
    cr.covid_period
ORDER BY 
    cr.TypeofTraveller, 
    cr.covid_period; -- sorts type of traveller by pre covid and post covid






# Q 10 - SQL (Esmond)
select * from airlines_reviews;
## did not filter out such that its only SIA so that we can learn from other airlines as well

## categorise the reviews by identifying certain keywords, so to filter out the specific columns we will be analysing 
select 
    case 
        when Reviews like '%compensation%' or Reviews like '%compensate%' or Reviews like '%refund%' then 'Compensation Related'
        when Reviews like '%safe%' or Reviews like '%safety%' or Reviews like '%emergency%' then 'Safety Related'
        when Reviews like '%turbulence%' then 'Turbulence Related'
        when Reviews like '%seat%' or Reviews like '%toilet%' or Reviews like '%comfort%' then 'Facility Related'
        when Reviews like '%service%' or Reviews like '%serve%' or Reviews like '%staff%' or Reviews like '%crew%' then 'Service Related'
        when Reviews like '%delay%' or Reviews like '%late%' or Reviews like '%wait%' then 'Delay Related'
        when Reviews like '%baggage%' or Reviews like '%luggage%' then 'Baggage Related'
        else 'Other'
    end as Issue,
    COUNT(*) as Frequency
from airlines_reviews
group by Issue
order by Frequency desc;

## scan through the reviews under 'other', notice that it is mainly good reviews / short reviews that do not consist the keywords i used to filter
select Issue, Reviews
from (
    select 
        Reviews,
        case 
            when Reviews like '%compensation%' or Reviews like '%compensate%' or Reviews like '%refund%' then 'Compensation Related'
   when Reviews like '%safe%' or Reviews like '%safety%' or Reviews like '%emergency%' then 'Safety Related'
   when Reviews like '%turbulence%' then 'Turbulence Related'
   when Reviews like '%seat%' or Reviews like '%toilet%' or Reviews like '%comfort%' then 'Facility Related'
   when Reviews like '%service%' or Reviews like '%serve%' or Reviews like '%staff%' or Reviews like '%crew%' then 'Service Related'
   when Reviews like '%delay%' or Reviews like '%late%' or Reviews like '%wait%' then 'Delay Related'
   when Reviews like '%baggage%' or Reviews like '%luggage%' then 'Baggage Related'
   else 'Other'
        end as Issue
    from airlines_reviews
) as subquery
where Issue = 'Other';

## analyse the reviews in 'Compensation', 'Safety', 'Turbulence' which aligns with the SQ321 incident (v1.0 - single category)
select Issue, Reviews
from (
    select 
        Reviews,
        case 
            when Reviews like '%compensation%' or Reviews like '%compensate%' or Reviews like '%refund%' then 'Compensation Related'
   when Reviews like '%safe%' or Reviews like '%safety%' or Reviews like '%emergency%' then 'Safety Related'
   when Reviews like '%turbulence%' then 'Turbulence Related'
   when Reviews like '%seat%' or Reviews like '%toilet%' or Reviews like '%comfort%' then 'Facility Related'
   when Reviews like '%service%' or Reviews like '%serve%' or Reviews like '%staff%' or Reviews like '%crew%' then 'Service Related'
   when Reviews like '%delay%' or Reviews like '%late%' or Reviews like '%wait%' then 'Delay Related'
   when Reviews like '%baggage%' or Reviews like '%luggage%' then 'Baggage Related'
   else 'Other'
        end as Issue
    from airlines_reviews
) as subquery
where Issue = 'Compensation Related' or Issue = 'Safety Related' or Issue = 'Turbulence Related'
order by Issue;

## analyse the reviews in 'Compensation', 'Safety', 'Turbulence' which aligns with the SQ321 incident (v2.0 - able to see if reviews fall in mutiple categories)
select Issues, Reviews
from (
    select 
        Reviews,
        CONCAT(
            case when Reviews like '%compensation%' or Reviews like '%compensate%' or Reviews like '%refund%' then 'Compensation Related, ' else '' end,
            case when Reviews like '%safe%' or Reviews like '%safety%' or Reviews like '%emergency%' then 'Safety Related, ' else '' end,
            case when Reviews like '%turbulence%' then 'Turbulence Related, ' else '' end,
            case when Reviews like '%seat%' or Reviews like '%toilet%' or Reviews like '%comfort%' then 'Facility Related, ' else '' end,
            case when Reviews like '%service%' or Reviews like '%serve%' or Reviews like '%staff%' or Reviews like '%crew%' then 'Service Related, ' else '' end,
            case when Reviews like '%delay%' or Reviews like '%late%' or Reviews like '%wait%' then 'Delay Related, ' else '' end,
            case when Reviews like '%baggage%' or Reviews like '%luggage%' then 'Baggage Related, ' else '' end
        ) as Issues
    from airlines_reviews
) as SubQuery
where (Issues like '%Compensation Related%' 
   or Issues like '%Safety Related%' 
   or Issues like '%Turbulence Related%')
order by Issues;
 
/* I requested a refund 6 months ago, but I have not received the money back, even though repeatedly mailing and 
   calling them once every month, and they keep reassuring me that the payment is on the way. I hope in the end I 
   will get my money back. */
   
## notice this review pulled out is from Qatar not SIA, but its still a gd learning point 
select * from airlines_reviews where Reviews like '%I requested a refund 6 months ago%';


select * from customer_support;

## the 2 selected categories are the closest to compensation/turbulence issue
## the 5 flags we identified to be most relevant to SIA under such circumstances
## filter more by searching for certain keywords in the instruction column
select *
from customer_support
where (category = 'REFUND' or intent = 'FEEDBACK') 
and (flags regexp '^(L|B|I|Q|Z)*$')
and (instruction like '%refund%' or instruction like '%compensate%' or instruction like '%compensation%' or instruction like '%turbulence%')
order by flags;

/* Definitely! I completely understand your need to know the specific situations in which you can request refunds. 
Here are some common scenarios where you may be eligible for a refund:

1. Product/Service Defect: If the product or service you receive is defective, malfunctioning, or doesn't meet the 
specifications mentioned, you can usually request a refund.

2. Cancellation within Grace Period: Many companies offer a grace period after purchase, during which you can change 
your mind and cancel your order for a full refund.

3. Failure to Deliver or Provide Service: If a vendor or service provider fails to deliver the product or provide 
the service as promised, you may be entitled to a refund.

4. Overcharged or Double Billed: If you notice discrepancies in your billing, such as being overcharged or double 
billed for the same item or service, it is reasonable to request a refund for the excess amount.

5. Unauthorized Transactions: If you identify any unauthorized charges on your account, it is important to report 
them immediately to your financial institution and request a refund.

6. Event Cancellation: If you purchased tickets for an event that gets canceled entirely or rescheduled to a date 
that is inconvenient for you, you may be eligible for a refund.

Please note that refund policies can vary between companies, so it's always a good idea to review the specific refund 
policy of the vendor or service provider you are dealing with. If you have a specific issue or concern regarding a recent 
purchase, please provide me with the necessary details, such as the order number or transaction ID, so that I can assist 
you further. I hope this clarifies the situations in which you can request refunds. If you have any additional questions, feel free 
to ask! */