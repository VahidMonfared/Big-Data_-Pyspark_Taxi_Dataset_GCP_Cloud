from __future__ import print_function
import os
import sys
from operator import add
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import DataFrame
import numpy as np

# Exception Handling: check if value is a float
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

# Function - Clean and validate the rows
def correctRows(p):
    # Validate row length and ensure required fields are valid floats
    if len(p) == 17:
        if isfloat(p[16]) and isfloat(p[12]) and isfloat(p[5]) and isfloat(p[14]):
            return p

# Extract the hour from pickup datetime
def getHour(pickup_datetime):
    try:
        return int(pickup_datetime.split(" ")[1].split(":")[0])
    except:
        return -1

# Main
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main_task4 <file> <output>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Task4Analysis")
    spark = SparkSession(sc)

    # Read the CSV file as RDD
    rdd = sc.textFile(sys.argv[1]).map(lambda line: line.split(","))

    # Clean data using the correctRows function
    clean_rdd = rdd.filter(correctRows)

    ### Task 4.2: Top 10 Efficient Taxi Drivers (Money per Mile)
    driver_efficiency = (clean_rdd
                         .map(lambda x: (x[1], (float(x[16]), float(x[5]))))  # (driver_id, (total_amount, trip_distance))
                         .filter(lambda x: x[1][1] > 0)  # Filter trips with positive distance
                         .mapValues(lambda v: v[0] / v[1])  # Calculate money per mile
                         .reduceByKey(lambda a, b: (a + b) / 2)  # Average the efficiency per driver
                         .sortBy(lambda x: x[1], ascending=False)  # Sort by efficiency
                         .take(10))  # Top 10 drivers

    print("Top 10 Efficient Taxi Drivers (Money per Mile):", driver_efficiency)

    # Save the results to output directory
    sc.parallelize(driver_efficiency).coalesce(1).saveAsTextFile(sys.argv[2] + "/driver_efficiency")

    # Stop the SparkContext
    sc.stop()
