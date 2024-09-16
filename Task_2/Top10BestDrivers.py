from __future__ import print_function 
import os
import sys
from operator import add
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

# Exception Handling: check if value is a float
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

# Function - Clean and validate the rows
def correctRows(p):
    # Validate row length and ensure trip distance, fare, and time are valid floats
    if len(p) == 17:
        if isfloat(p[5]) and isfloat(p[11]) and isfloat(p[16]):
            if float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0:
                return p

# Main
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main_task2 <file> <output>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Top10BestDrivers")
    spark = SparkSession(sc)

    # Read the CSV file as RDD
    rdd = sc.textFile(sys.argv[1]).map(lambda line: line.split(","))

    # Clean data using the correctRows function
    clean_rdd = rdd.filter(correctRows)

    # Task 2: Find the top 10 best drivers in terms of average earned money per minute
    results_2 = (clean_rdd
                 .map(lambda x: (x[1], (float(x[16]), float(x[4]) / 60)))  # (driver, (total_amount, trip_time_in_minutes))
                 .filter(lambda x: x[1][1] > 0)  # Ensure time > 0 to avoid division by zero
                 .mapValues(lambda v: (v[0], v[1], v[0] / v[1]))  # Calculate (total_amount, trip_time, money_per_minute)
                 .map(lambda x: (x[0], x[1][2]))  # (driver, money_per_minute)
                 .reduceByKey(lambda a, b: a + b)  # Sum the earnings per driver
                 .sortBy(lambda x: x[1], ascending=False)  # Sort by money per minute in descending order
                 .take(10))  # Get top 10

    # Save the results
    sc.parallelize(results_2).coalesce(1).saveAsTextFile(sys.argv[2])

    # Stop the SparkContext
    sc.stop()
