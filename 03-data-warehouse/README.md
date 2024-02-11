1. What is count of records for the 2022 Green Taxi Data??
```sql
SELECT count(1)
FROM `mage-zoomcamp-413210.green_taxi_2022.green_taxi_2022`
```

2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
```sql
SELECT distinct PULocationID
FROM `mage-zoomcamp-413210.green_taxi_2022.green_taxi_2022`;

SELECT distinct PULocationID
FROM `mage-zoomcamp-413210.green_taxi_2022_native.green_taxi`
```

3. How many records have a fare_amount of 0?
```sql
select count(1)
from `mage-zoomcamp-413210.green_taxi_2022.green_taxi_2022`
where fare_amount = 0;
```

4. What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
```sql
CREATE TABLE `mage-zoomcamp-413210.green_taxi_2022.green_taxi_2022_partitioned`
PARTITION BY DATE (lpep_pickup_datetime)
CLUSTER BY PULocationID
AS
SELECT * FROM `mage-zoomcamp-413210.green_taxi_2022.green_taxi_2022`;
```

5. Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
```sql
SELECT distinct PULocationID
FROM `mage-zoomcamp-413210.green_taxi_2022.green_taxi_2022_partitioned`
where lpep_pickup_datetime between 2022-06-01 and 2022-06-30;

SELECT distinct PULocationID
FROM `mage-zoomcamp-413210.green_taxi_2022_native.green_taxi`
where lpep_pickup_datetime between 2022-06-01 and 2022-06-30;
```
