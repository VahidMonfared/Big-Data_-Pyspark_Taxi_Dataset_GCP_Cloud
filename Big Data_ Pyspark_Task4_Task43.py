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

    ### Task 4.3: Statistics (Mean, Median, Quantiles) for Tip Amount
    tip_rdd = clean_rdd.map(lambda x: float(x[14]))  # Extract tip amount

    # Mean
    mean_tip = tip_rdd.mean()

    # Median
    median_tip = np.median(tip_rdd.collect())

    # Quantiles
    quantiles = np.percentile(tip_rdd.collect(), [25, 50, 75])

    print(f"Mean Tip Amount: {mean_tip}")
    print(f"Median Tip Amount: {median_tip}")
    print(f"First Quantile (Q1): {quantiles[0]}, Third Quantile (Q3): {quantiles[2]}")

    # Save the results to output directory (now properly saving as text)
    result_rdd = sc.parallelize([f"Mean Tip: {mean_tip}", f"Median Tip: {median_tip}",
                                 f"Q1: {quantiles[0]}", f"Q3: {quantiles[2]}"])
    result_rdd.saveAsTextFile(sys.argv[2])

    # Stop the SparkContext
    sc.stop()
