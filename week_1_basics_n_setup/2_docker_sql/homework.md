## Week 1 Homework
## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--imageid string`
- `--iidfile string`
- `--idimage string`
- `--idfile string`

### Answer

```
docker build --help | grep "Write the image ID to the file"
```
```
      --iidfile string          Write the image ID to the file
```

answer: `--iidfile string`

## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- 1
- 6
- 3
- 7

### Answer

```
docker run --rm python:3.9 python3 -m pip list
```
```
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
```

answer: `3`

## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20689
- 20530
- 17630
- 21090

### Answer

```
SELECT count(1)
FROM taxi_trips
WHERE (lpep_pickup_datetime BETWEEN '2019-01-15 00:00:00.000' AND '2019-01-15 23:59:59.999')
  and (lpep_dropoff_datetime BETWEEN '2019-01-15 00:00:00.000' AND '2019-01-15 23:59:59.999');
```
```
count|
-----+
20530|
```

answer: `20530`

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- 2019-01-18
- 2019-01-28
- 2019-01-15
- 2019-01-10

### Answer

```
SELECT trip_distance,
       lpep_pickup_datetime
FROM taxi_trips
ORDER BY trip_distance DESC
LIMIT 1;
```
```
trip_distance|lpep_pickup_datetime   |
-------------+-----------------------+
       117.99|2019-01-15 19:27:58.000|
```

answer: `2019-01-15`

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- 2: 1282 ; 3: 254
- 2: 1282 ; 3: 274


### Answer

```
SELECT passenger_count,
       count(passenger_count)
FROM taxi_trips
WHERE (lpep_pickup_datetime BETWEEN '2019-01-01 00:00:00.000' AND '2019-01-01 23:59:59.999')
  AND passenger_count BETWEEN 2 AND 3
GROUP BY passenger_count
ORDER BY count(passenger_count) DESC;
```

```
passenger_count|count|
---------------+-----+
              2| 1282|
              3|  254|
```

answer: `- 2: 1282 ; 3: 254`

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- South Ozone Park
- Long Island City/Queens Plaza

### Answer

```
SELECT z2."Zone"
FROM taxi_trips t
INNER JOIN yellow_taxi_zones z1 ON z1."LocationID" = t."PULocationID"
INNER JOIN yellow_taxi_zones z2 ON z2."LocationID" = t."DOLocationID"
WHERE z1."Zone" = 'Astoria'
ORDER BY t.tip_amount DESC
LIMIT 1;
```
```
Zone                         |
-----------------------------+
Long Island City/Queens Plaza|
```

answer: `Long Island City/Queens Plaza`
## Submitting the solutions

* Form for submitting: [form](https://forms.gle/EjphSkR1b3nsdojv7)
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Thursday), 22:00 CET