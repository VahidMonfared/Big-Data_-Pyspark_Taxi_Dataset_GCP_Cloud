from __future__ import print_function 
import os
import sys
from operator import add
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import hour

# Exception Handling: check if value is a float
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

# Function - Clean and validate the rows
def correctRows(p):
    # Validate row length and ensure surcharge and trip distance are valid floats
    if len(p) == 17:
        if isfloat(p[12]) and isfloat(p[5]):
            # Check that trip distance > 0 and surcharge >= 0
            if float(p[5]) > 0 and float(p[12]) >= 0:
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
        print("Usage: main_task3 <file> <output>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="BestTimeForTaxi")
    spark = SparkSession(sc)

    # Read the CSV file as RDD
    rdd = sc.textFile(sys.argv[1]).map(lambda line: line.split(","))

    # Clean data using the correctRows function
    clean_rdd = rdd.filter(correctRows)

    # Task 3: Find the best hour of the day based on profit ratio (surcharge / travel distance)
    results_3 = (clean_rdd
                 .map(lambda x: (getHour(x[2]), (float(x[12]) / float(x[5]))))  # (hour, profit_ratio)
                 .filter(lambda x: x[0] >= 0)  # Ensure valid hour
                 .mapValues(lambda x: (x, 1))  # Convert to (hour, (profit_ratio, count))
                 .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # Sum profit ratios and count occurrences
                 .mapValues(lambda v: v[0] / v[1])  # Calculate average profit ratio per hour
                 .sortBy(lambda x: x[1], ascending=False)  # Sort by highest average profit ratio
                 .take(1))  # Get the hour with the highest profit ratio

    # Save the results
    sc.parallelize(results_3).coalesce(1).saveAsTextFile(sys.argv[2])

    # Stop the SparkContext
    sc.stop()
