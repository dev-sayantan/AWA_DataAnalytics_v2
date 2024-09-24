CREATE DATABASE swiggy_orders_10k;
USE swiggy_orders_10k;
CREATE TABLE swiggy_orders(
    OrderID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    Customer_Name VARCHAR(50),
    Restaurant_Name VARCHAR(50),
    Order_Date Datetime,
    Delivery_Time Datetime,
    Delivery_Address VARCHAR(80),
    City VARCHAR(50),
    Delivery_Status VARCHAR(50),
    Order_Amount DECIMAL(10 , 2),
    Delivery_Agent VARCHAR(50)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/swiggy_orders.csv'
INTO TABLE test
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS (`OrderID`,
    `Customer_Name`,
    `Restaurant_Name`,
    @OrderDate,
    @DeliveryTime,
    `Delivery_Address`,
    `City`,
    `Delivery_Status`,
    `Order_Amount`,
    `Delivery_Agent` 
) 
SET
Order_Date = str_to_date(@OrderDate, '%Y-%m-%d %H:%i'),
Delivery_Time = str_to_date(@DeliveryTime, '%Y-%m-%d %H:%i');

select count(*) from swiggy_orders;
select * from swiggy_orders;
-- ------------------------------------------------------------------------------------------------------------------------------------
-- Easy Level:
-- 1. Extracting Date Components:
-- Extract the year, month, and day from the order_date column in the Swiggy dataset.
SELECT 
    OrderID,
    Customer_Name,
    EXTRACT(YEAR FROM Order_Date) AS Year_Of_Order,
    EXTRACT(MONTH FROM Order_Date) AS Month_Of_Order,
    EXTRACT(DAY FROM Order_Date) AS Day_Of_Order
FROM
    swiggy_orders;

-- 2. Current Timestamp:
-- Get the current timestamp and compare it with the delivery_time.
SELECT 
    Delivery_Time,
    CURRENT_TIMESTAMP() AS curr_time,
    DATEDIFF(Delivery_Time, CURRENT_TIMESTAMP) AS Day_Diff
FROM
    swiggy_orders;
    
-- 3. Date & Time Difference:
-- Calculate the number of days,hours,minutes,etc between the order_date and delivery_time and store it in respective columns.
-- Add 45 minutes to the delivery_time and show the updated time.
SELECT 
    Order_Date,
    Delivery_Time,
    datediff(Order_Date, Delivery_Time) AS Days_Between,
	hour(timediff(Order_Date, Delivery_Time)) AS Hours_Between,
    minute(timediff(Order_Date, Delivery_Time)) AS Minutes_Between
FROM
    swiggy_orders;
    
SELECT 
    Delivery_Time,
    DATE_ADD(Delivery_Time, INTERVAL 45 MINUTE) AS Updated_Time
FROM
    swiggy_orders;

-- 4. Orders Placed in Specific Months:
-- Find all orders placed in September of any year.
SELECT * FROM swiggy_orders WHERE extract(MONTH FROM Order_Date) = 9;

-- ---------------------------------------------------------------------------------------------------------------------------------
-- Intermediate Level:
-- 4. Time Zone Conversion:
-- Convert the delivery_time from UTC to a specific time zone (e.g., 'Asia/Kolkata').
SELECT 
    DELIVERY_TIME AS UTC_Delivery_Time,
    convert_tz(Delivery_Time,'+00:00', '+5:30') AS Asia_Kolkata_TZ
FROM
    swiggy_orders;
    
-- 5. Orders on Specific Weekends:
-- Find all orders placed on a weekend (Saturday or Sunday).
SELECT * FROM swiggy_orders WHERE extract(DAY FROM Order_Date) = 'Saturday' OR 'Sunday';
 -- --------------------------------------------------------------------------------------------------------------------------------- 
-- Advanced Level:
-- 6. Calculating Peak Hours:
-- Identify the peak delivery hours by extracting the hour from delivery_time and grouping by hour
-- Identify which day of the week has the most deliveries.
SELECT 
    EXTRACT(HOUR FROM Delivery_Time) AS Peak_Delivery_Hours,
    SUM(Order_Amount) AS Total_Order_Amount
FROM
    swiggy_orders
GROUP BY Peak_Delivery_Hours , Order_Amount
ORDER BY Total_Order_Amount DESC
LIMIT 10;

SELECT 
    weekday(Delivery_Time) AS WeekDay,
    COUNT(OrderID) AS Order_Counts
FROM
    swiggy_orders
GROUP BY WeekDay
ORDER BY Order_Counts DESC;

-- 7.Handling Daylight Saving Time:
-- Convert the delivery_time into a time zone that observes daylight saving time (e.g., 'America/New_York') and check if any orders fall during the daylight saving adjustment period.

SELECT DISTINCT month(Delivery_Time) AS each_month FROM swiggy_orders;
SELECT COUNT(OrderID)
FROM swiggy_orders
WHERE 
    convert_tz(delivery_time,'+5:30','-4:00') 
    BETWEEN '2023-03-12 02:00:00' AND '2023-11-05 02:00:00' 
    OR 
    convert_tz(delivery_time,'+5:30','-4:00')
    BETWEEN '2024-03-10 02:00:00' AND '2024-11-03 02:00:00';

-- 8.Identify Late Deliveries: 
-- Find orders where the delivery took more than 1 hour.
SELECT 
    COUNT(OrderID)
FROM
    swiggy_orders
WHERE
    HOUR(Delivery_Time) > EXTRACT(HOUR FROM DATE_ADD(Order_Date, INTERVAL 1 HOUR));
    
-- 9. Filtering Orders Between Two Date-Times:
-- Find all orders placed between specific date ranges, e.g., between '2023-09-01' and '2023-09-05' and orders placed between 5 PM and 7 PM both for those dates included and without those date too irrespective of dates.

SELECT 
    OrderID, Order_Date
FROM
    swiggy_orders
GROUP BY OrderID,Order_Date
HAVING Order_Date BETWEEN '2023-09-01' AND '2023-09-05'
    AND HOUR(Order_Date) BETWEEN 17 AND 19;

-- 10. Handling Leap Years:
-- Find orders placed on February 29th (during leap years).
SELECT OrderID,Order_Date FROM swiggy_orders 
WHERE 
(YEAR(Order_Date) % 4 = 0 OR YEAR(Order_Date) % 100 != 0 AND YEAR(Order_Date) % 400 = 0)
GROUP BY OrderID,Order_Date HAVING DAY(Order_Date) = 29 AND MONTH(Order_Date) = 2 ;

-- 11. Timestamp Arithmetic with Time Zones:
-- Calculate the time difference between the order time in 'Asia/Kolkata' and 'America/Los_Angeles'.
SELECT
	Order_Date,
    convert_tz(Order_Date, '+5:30', '-7:00') AS Order_Time_LA_TZ,
    Order_Date AS Order_Time_Kol_TZ,
    TIMEDIFF(Order_Date,convert_tz(Order_Date, '+5:30', '-7:00')) AS OrderTime_Diff_Hour_Kol_LA
FROM
    swiggy_orders;
    
-- 12. Finding the Most Recent Order:
-- Retrieve the most recent order placed in the last 7 days.
SELECT 
    orderid,
    SUBDATE(Order_Date, INTERVAL 7 DAY) AS most_recent_order_dt
FROM
    swiggy_orders
GROUP BY orderid , order_date
ORDER BY Order_Date DESC
LIMIT 1;

-- Expert Level:
-- 13. Calculate Average Delivery Time per City:
-- • Calculate the average delivery time for each city.
SELECT
	DISTINCT 
    City,
	ROUND(AVG(TIMESTAMPDIFF(MINUTE, Order_Date, Delivery_Time)), 0) AS deliveryTimeDiff_mins
FROM
	swiggy_orders
GROUP BY city
ORDER BY deliveryTimeDiff_mins DESC;

-- 14. Finding Busiest Days by City:
-- • Identify which day of the week has the highest number of orders for each city.
SELECT
	DISTINCT
    City,
    dayname(Order_Date) AS Week_Day,
    count(OrderID) AS num_of_orders
FROM
	swiggy_orders
GROUP BY city, Week_Day
ORDER BY Week_Day, num_of_orders DESC
LIMIT 10;
-- 15. Delayed Deliveries Based on Peak Hours:
-- • Identify orders that took longer during peak hours (5 PM - 8 PM).
SELECT DISTINCT
    OrderID,
    Order_Date,
    Delivery_Time,
    EXTRACT(HOUR FROM Delivery_Time) AS Peak_Delivery_Hours
FROM
    swiggy_orders
WHERE
    EXTRACT(HOUR FROM Delivery_Time) BETWEEN 17 AND 20
GROUP BY OrderID , Order_Date , Delivery_Time , Peak_Delivery_Hours
HAVING TIMESTAMPDIFF(MINUTE,
    Order_Date,
    Delivery_Time) AND Peak_Delivery_Hours < 20
ORDER BY Delivery_Time DESC;
-- 16. Orders with Week-to-Week Growth:
-- • Calculate week-on-week growth of orders.
SELECT extract(WEEK FROM Order_Date) AS week_to_week,
		orderid, Delivery_Status, Order_Amount AS Current_Order_Amount,
		LAG(Order_Amount,1,0) OVER(ORDER BY Order_Date) AS lagging_order_amount, 
        LEAD(Order_Amount,1) OVER(ORDER BY Order_Date) AS leading_order_amount,
        LEAD(Order_Amount,1) OVER(ORDER BY Order_Date) - LAG(Order_Amount,1,0) OVER(ORDER BY Order_Date) AS weekly_growth
from swiggy_orders
where Delivery_Status = 'Delivered' OR 'Pending'
GROUP BY orderid, Order_Amount
ORDER BY week_to_week ASC;

-- 17. Finding Orders Affected by Public Holidays:
-- • Identify orders placed on specific public holidays (e.g., New Year's Day, Diwali).
SELECT 
    orderid, Order_Date
FROM
    swiggy_orders
WHERE
	EXTRACT(DAY FROM order_date) = 1
        AND EXTRACT(MONTH FROM order_date) = 1
GROUP BY orderid , order_date
ORDER BY Order_Date ASC;