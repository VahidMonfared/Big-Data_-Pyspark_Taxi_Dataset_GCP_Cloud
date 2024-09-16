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
    # Validate row length and ensure trip distance and fare are valid floats
    if len(p) == 17:
        if isfloat(p[5]) and isfloat(p[11]):
            if float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0:
                return p

# Main
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main_task1 <file> <output>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Top10ActiveTaxis")
    spark = SparkSession(sc)

    # Read the CSV file as RDD
    rdd = sc.textFile(sys.argv[1]).map(lambda line: line.split(","))

    # Clean data using the correctRows function
    clean_rdd = rdd.filter(correctRows)

    # Task 1: Find the top 10 taxis with the highest number of distinct drivers
    # Using medallion as the taxi ID and hack_license as driver ID
    results_1 = (clean_rdd
                 .map(lambda x: (x[0], x[1]))  # (medallion, hack_license)
                 .distinct()  # Get distinct (medallion, hack_license) pairs
                 .map(lambda x: (x[0], 1))  # Convert to (medallion, 1)
                 .reduceByKey(add)  # Sum by medallion (number of distinct drivers)
                 .sortBy(lambda x: x[1], ascending=False)  # Sort by the count of drivers
                 .take(10))  # Get top 10

    # Save the results
    sc.parallelize(results_1).coalesce(1).saveAsTextFile(sys.argv[2])

    # Stop the SparkContext
    sc.stop()
