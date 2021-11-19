# Data_Mart_CS

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
