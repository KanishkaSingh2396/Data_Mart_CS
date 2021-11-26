# Data_Mart_CS_5

![image](https://user-images.githubusercontent.com/89623051/142603464-ad9b97d5-23ef-4af0-8b32-1f029cb3d8d4.png)

For this case study there is only a single table: data_mart.weekly_sales

![image](https://user-images.githubusercontent.com/89623051/142603675-1d5bc9df-160c-481a-8a70-e94c9074d051.png)


The columns details about the dataset:

1. Data Mart has international operations using a multi-region strategy
2. Data Mart has both, a retail and online platform in the form of a Shopify store front to serve their customers
3. Customer segment and customer_type data relates to personal age and demographics information that is shared with Data Mart
4. transactions is the count of unique purchases made through Data Mart and sales is the actual dollar amount of purchases
5. Each record in the dataset is related to a specific aggregated slice of the underlying sales data rolled up into a week_date value which represents the start of the sales week.

Case Study Questions

A. Data Cleansing Steps

In a single query, perform the following operations and generate a new table in the data_mart schema named clean_weekly_sales:

1. Convert the week_date to a DATE format
2. Add a week_number as the second column for each week_date value, for example any value from the 1st of January to 7th of January will be 1, 8th to 14th will be 2 etc
3. Add a month_number with the calendar month for each week_date value as the 3rd column
4. Add a calendar_year column as the 4th column containing either 2018, 2019 or 2020 values
5. Add a new column called age_band after the original segment column using the following mapping on the number inside the segment value

segment	age_band

1	Young Adults

2	Middle Aged

3 or 4	Retirees

Add a new demographic column using the following mapping for the first letter in the segment values:
segment | demographic |

C | Couples |

F | Families |

Ensure all null string values with an "unknown" string value in the original segment column as well as the new age_band and demographic columns

6. Generate a new avg_transaction column as the sales value divided by transactions rounded to 2 decimal places for each record

Solution Query

```sql
select 
  TO_DATE(week_date, 'DD/MM/YY') as week_date, 
  extract(week from TO_DATE(week_date, 'DD/MM/YY')) as week_number,
  extract(month from TO_DATE(week_date, 'DD/MM/YY')) as month_number,
  extract(year from to_date(week_date, 'dd/mm/yy')) as calender_year,
  region,
  platform,
  segment,
  (case when segment is null then 'Unknown' else segment end) as segment,
  (case 
    when right(segment, 1) = '1' then 'Young Adults'
    when right(segment, 1) = '2' then 'Middle Aged'
    when right(segment, 1) ='3' or right(segment, 1) = '4'  then 'Retirees'
    else 'Unknown'
  end )as age_band,
  (case 
    when left(segment, 1) = 'C' then 'Couples'
    when left(segment, 1) = 'F' then 'Families'
    else 'Unknown'
  end )as dempgraphics,
  customer_type,
  transactions,
  sales,
  round(sales::numeric / transactions::numeric, 2) AS avg_transaction
from data_mart.weekly_sales
```
Query Output

![image](https://user-images.githubusercontent.com/89623051/142603388-94cc7993-01a3-4ac1-bb78-b940eed834ba.png)



Part B. Data Exploration

Question 1 What day of the week is used for each week_date value?

```sql
select to_char(week_date, 'day') as weekday from base limit 1
```
![image](https://user-images.githubusercontent.com/89623051/142609613-630d214d-db93-40c6-8f04-ccc0c99e6c68.png)

Question 2 What range of week numbers are missing from the dataset?

```sql
WITH all_week_numbers AS (
  SELECT GENERATE_SERIES(1, 26) AS week_number
)
SELECT
  week_number
FROM all_week_numbers AS t1
WHERE EXISTS (
  SELECT 1
  FROM clean_weekly_sales AS t2
  WHERE t1.week_number != t2.week_number
);
```

![image](https://user-images.githubusercontent.com/89623051/142627520-6ae3b0c1-61c8-4960-b3e4-e2609af09311.png)

Question 3 How many total transactions were there for each year in the dataset?

```sql

select calender_year, count(transactions) as total_transactions_in_a_year, sum(transactions) as transactions_amount
from clean_weekly_sales
group by calender_year
order by calender_year
```

![image](https://user-images.githubusercontent.com/89623051/142628733-404cd87a-3147-44a4-9f70-3a26e8794340.png)

Question 4 What is the total sales for each region for each month?

```sql
select 
  distinct region, month_number, sum(sales) as total_sales
from clean_weekly_sales
group by region, month_number
order by month_number
```

![image](https://user-images.githubusercontent.com/89623051/142630877-f86c723c-bf45-4ad2-8704-ef38f8110400.png)

Question 4B What is the total sales for each region for first month?

```sql

with base_tb as(
select 
  distinct region, 
  month_number, 
  sum(sales) as total_sales,
  row_number() over(partition by region order by month_number) as rn
from clean_weekly_sales
group by region, month_number
order by region)

select * 
from base_tb
where rn = 1

```
![image](https://user-images.githubusercontent.com/89623051/142631993-30c2c152-a77c-4caf-af80-35dd00103a2d.png)

Question 5 What is the total count of transactions for each platform


```sql
select 
  platform, 
  count(transactions) as total_transactions,
  sum(transactions) as total_transactions_amount
from clean_weekly_sales
group by platform

```

![image](https://user-images.githubusercontent.com/89623051/142633860-4864065d-2a7d-4505-bc77-74e068850ff1.png)

Question 6 What is the percentage of sales for Retail vs Shopify for each month?

```sql
with base as(
select 
  date_trunc('month', week_date) as month_starting, 
  calender_year, 
  platform, 
  sum(sales) as total_sales
from clean_weekly_sales
group by calender_year, month_starting,platform
order by calender_year, month_starting),

tb1 as (select 
  *,
  row_number() over(partition by month_starting order by calender_year, month_starting) as rn
from base),

retail_tb as (select month_starting, total_sales as Retail_sales
from tb1 
where rn = 1),

Shopify_tb as (select month_starting, total_sales as Shopify_sales
from tb1
where rn = 2)

select 
  retail_tb.month_starting, 
  round(100*((Retail_sales)::numeric /  (Retail_sales::numeric + Shopify_sales::numeric)),2)as Retail_percentage, 
  round(100*((Shopify_sales)::numeric / (Retail_sales::numeric + Shopify_sales::numeric)),2)as Shopify_percentage
from retail_tb
left join Shopify_tb
on retail_tb.month_starting = Shopify_tb.month_starting

```

![image](https://user-images.githubusercontent.com/89623051/142662792-9eeafb74-e052-4425-b2cb-fa0a6c1ddb18.png)

What is the amount and percentage of sales by demographic for all year in the dataset?

```sql
select  dempgraphics as demographics, calender_year, sum(sales) as total_sales, round(100*(sum(sales)::numeric/40743634227), 2) as sales_percentage
from clean_weekly_sales
group by  calender_year , demographics
order by calender_year asc,  demographics asc
```

![image](https://user-images.githubusercontent.com/89623051/142715714-3bde874d-2620-4274-9417-57094b4fa716.png)


What is the amount and percentage of sales by demographic for each year in the dataset?

```sql
select  
  dempgraphics as demographics, 
  calender_year, 
  round(100*(sum(sales)::numeric /
  (sum(sum(sales)) over(partition by calender_year))::numeric),2) as percentage_sales
from clean_weekly_sales
group by  calender_year , demographics
order by calender_year asc,  demographics asc

```

![image](https://user-images.githubusercontent.com/89623051/142716527-954874d8-65b4-4b1a-90f2-380160e1d868.png)


Which age_band and demographic values contribute the most to Retail sales?

age_band sales contribution

```sql
select  
  age_band,
  round(
  100*(
  (sum(sales)) /
  (sum(sum(sales)) over())
  ),2) as age_band_contribution
from clean_weekly_sales
group by age_band
```

demographics sales contribution

```sql
select  
  dempgraphics as demographics,
  round(
  100*(
  (sum(sales)) /
  (sum(sum(sales)) over())
  ),2) as demographics_contribution
from clean_weekly_sales
group by demographics

```

age_band and demographics contribution

```sql
select
age_band,
dempgraphics as demographics,
round(
100*((sum(sales)::numeric) /
(sum(sum(sales)) over())::numeric)
,2) as percentage_contribution

from clean_weekly_sales
where platform = 'Retail'
group by age_band, demographics
order by percentage_contribution desc

```

Question 9 Can we use the avg_transaction column to find the average transaction size for each year for Retail vs Shopify? 
If not - how would you calculate it instead?

Approch One:

```sql
select 
  calender_year,
  platform,
  floor(avg(sales)/avg(transactions)) as avg_year_txn
from clean_weekly_sales
group by calender_year, platform
order by calender_year

```

![image](https://user-images.githubusercontent.com/89623051/142719459-2c1bbbad-f722-4066-86d7-381e4a7415e7.png)


Approach Two:

```sql
with base as (
select 
  calender_year,
  platform,
  floor(avg(sales)/avg(transactions)) as avg_year_txn,
  (row_number() over(partition by calender_year order by calender_year, platform)) as rn
from clean_weekly_sales
group by calender_year, platform
order by calender_year),

Retail as (select calender_year, avg_year_txn as retail_avg_year_txn from base
where rn = 1),

Shopify as (select calender_year, avg_year_txn as shopify_avg_year_txn from base
where rn = 2)

select *
from Retail R
left Join Shopify S
on R.calender_year = S.calender_year

```

![image](https://user-images.githubusercontent.com/89623051/142719472-70e0e3a7-f44b-488d-a78e-3c420d3fab2d.png)

Part C. Before & After Analysis


This technique is usually used when we inspect an important event and want to inspect the impact before and after a certain point in time.

Taking the week_date value of 2020-06-15 as the baseline week where the Data Mart sustainable packaging changes came into effect.

We would include all week_date values for 2020-06-15 as the start of the period after the change and the previous week_date values would be before

Using this analysis approach - answer the following questions:


Question 1

What is the total sales for the 4 weeks before and after 2020-06-15? What is the growth or reduction rate in actual values and percentage of sales?

Step 1: Identify week_number of week_date = '2020-06-15'
```sql
select week_number
from clean_weekly_sales
where week_date = '2020-06-15'
```

![image](https://user-images.githubusercontent.com/89623051/142722361-5c495282-487b-4edc-8fdc-f23fe96a5bc5.png)

Step 2 filter out the weeks before and after 2020-06-15and calculate sales difference and sales change

```sql
select 
  sum(case when week_number between 21 and 24 then sales end) as before_sales,
  sum(case when week_number between 25 and 28 then sales end) as after_sales,
  (sum(case when week_number between 25 and 28 then sales end) 
  - 
  sum(case when week_number between 21 and 24 then sales end)) as sales_fifferenece,
  
  round(100* ((sum(case when week_number between 25 and 28 then sales end) 
  - 
  sum(case when week_number between 21 and 24 then sales end))::numeric
  /
  sum(case when week_number between 21 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2020'
```

![image](https://user-images.githubusercontent.com/89623051/142723147-6a961cf2-a613-49fc-ad6f-2f36e32119d6.png)


Question 2: What about the entire 12 weeks before and after?

```sql
select 
  sum(case when week_number between 13 and 24 then sales end) as before_sales,
  sum(case when week_number between 25 and 36 then sales end) as after_sales,
  (sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end))::numeric
  /
  sum(case when week_number between 13 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2020'
```

![image](https://user-images.githubusercontent.com/89623051/142723326-e7a43cde-d5c9-46cd-bb17-70952fd76c33.png)

Question 3: How do the sale metrics for these 2 periods before and after compare with the previous years in 2018 and 2019?

4 weeks before and after impact

```sql

select 
  case when calender_year = '2018' then calender_year end as year,
  sum(case when week_number between 21 and 24 then sales end) as before_sales_4weeks,
  sum(case when week_number between 25 and 28 then sales end) as after_sales_4weeks,
  (sum(case when week_number between 25 and 28 then sales end) 
  - 
  sum(case when week_number between 21 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 28 then sales end) 
  - 
  sum(case when week_number between 21 and 24 then sales end))::numeric
  /
  sum(case when week_number between 21 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2018'
group by calender_year

union 

select 
  case when calender_year = '2019' then calender_year end as year,
  sum(case when week_number between 21 and 24 then sales end) as before_sales_4weeks,
  sum(case when week_number between 25 and 28 then sales end) as after_sales_4weeks,
  (sum(case when week_number between 25 and 28 then sales end) 
  - 
  sum(case when week_number between 21 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 28 then sales end) 
  - 
  sum(case when week_number between 21 and 24 then sales end))::numeric
  /
  sum(case when week_number between 21 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2019'
group by calender_year
union

select 
  case when calender_year = '2020' then calender_year end as year,
  sum(case when week_number between 21 and 24 then sales end) as before_sales_4weeks,
  sum(case when week_number between 25 and 28 then sales end) as after_sales_4weeks,
  (sum(case when week_number between 25 and 28 then sales end) 
  - 
  sum(case when week_number between 21 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 28 then sales end) 
  - 
  sum(case when week_number between 21 and 24 then sales end))::numeric
  /
  sum(case when week_number between 21 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2020'
group by calender_year

```

![image](https://user-images.githubusercontent.com/89623051/142724317-ae1e2656-9038-4c23-825b-4a03d2383cd7.png)

12 weeks before and after impact

```sql

select 
  case when calender_year = '2018' then calender_year end as year,
  sum(case when week_number between 13 and 24 then sales end) as before_sales_4weeks,
  sum(case when week_number between 25 and 36 then sales end) as after_sales_4weeks,
  (sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end))::numeric
  /
  sum(case when week_number between 13 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2018'
group by calender_year

union 

select 
  case when calender_year = '2019' then calender_year end as year,
  sum(case when week_number between 13 and 24 then sales end) as before_sales_4weeks,
  sum(case when week_number between 25 and 36 then sales end) as after_sales_4weeks,
  (sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end))::numeric
  /
  sum(case when week_number between 13 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2019'
group by calender_year

union

select 
  case when calender_year = '2020' then calender_year end as year,
  sum(case when week_number between 13 and 24 then sales end) as before_sales_4weeks,
  sum(case when week_number between 25 and 36 then sales end) as after_sales_4weeks,
  (sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end))::numeric
  /
  sum(case when week_number between 13 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2020'
group by calender_year

```

![image](https://user-images.githubusercontent.com/89623051/142724459-77a1dbc6-9feb-4084-9c2a-82e0dd98642f.png)


Part D. Bonus Question
Which areas of the business have the highest negative impact in sales metrics performance in 2020 for the 12 week before and after period?

1. region

2. platform

3. age_band

4. demographic

5. customer_type

SOLUTION

highest negative impact in region sales metrics performance in 2020 for the 12 week before and after period

```sql

select 
  calender_year,
  region,
  sum(case when week_number between 13 and 24 then sales end) as before_sales,
  sum(case when week_number between 25 and 36 then sales end) as after_sales,
  (sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end))::numeric
  /
  sum(case when week_number between 13 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2020'
group by region, calender_year

```

![image](https://user-images.githubusercontent.com/89623051/142724840-eb9bdf34-acef-4ca0-86b9-d4e0feb5958b.png)

2. highest negative impact on platform sales metrics performance in 2020 for the 12 week before and after period

```sql
select 
  calender_year,
  platform,
  sum(case when week_number between 13 and 24 then sales end) as before_sales,
  sum(case when week_number between 25 and 36 then sales end) as after_sales,
  (sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end))::numeric
  /
  sum(case when week_number between 13 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2020'
group by platform, calender_year
```

![image](https://user-images.githubusercontent.com/89623051/142724932-965d3bd1-9b2b-4db7-9093-7c0c7f311979.png)

3. highest negative impact on age_band sales metrics performance in 2020 for the 12 week before and after period

```sql
select 
  calender_year,
  age_band,
  sum(case when week_number between 13 and 24 then sales end) as before_sales,
  sum(case when week_number between 25 and 36 then sales end) as after_sales,
  (sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end))::numeric
  /
  sum(case when week_number between 13 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2020'
group by age_band, calender_year
```

![image](https://user-images.githubusercontent.com/89623051/142725069-d4d66807-0c2a-4380-9de2-a1bd9f39ff5e.png)

4. highest negative impact on demographics sales metrics performance in 2020 for the 12 week before and after period

```sql
select 
  calender_year,
  dempgraphics as demographics,
  sum(case when week_number between 13 and 24 then sales end) as before_sales,
  sum(case when week_number between 25 and 36 then sales end) as after_sales,
  (sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end))::numeric
  /
  sum(case when week_number between 13 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2020'
group by demographics, calender_year
```
![image](https://user-images.githubusercontent.com/89623051/142725167-42bb7ed7-681e-4b8c-ab81-fdf78ee2ea3c.png)

5. highest negative impact on demographics sales metrics performance in 2020 for the 12 week before and after period

```sql
select 
  calender_year,
  customer_type,
  sum(case when week_number between 13 and 24 then sales end) as before_sales,
  sum(case when week_number between 25 and 36 then sales end) as after_sales,
  (sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end)) as sales_differenece,
  
  round(100* ((sum(case when week_number between 25 and 36 then sales end) 
  - 
  sum(case when week_number between 13 and 24 then sales end))::numeric
  /
  sum(case when week_number between 13 and 24 then sales end)::numeric),2) as sales_change
from clean_weekly_sales
where calender_year = '2020'
group by customer_type, calender_year
```
![image](https://user-images.githubusercontent.com/89623051/142725240-5e2512aa-92fe-4c8a-83f3-8625cac10828.png)
