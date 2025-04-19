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

    ### Task 4.1: Payment method percentages by hour (Cash vs. Card)
    total_payments = clean_rdd.count()
    
    # Cash payments and card payments percentages
    payment_by_hour = (clean_rdd
                       .map(lambda x: (getHour(x[2]), x[10]))  # Extract (hour, payment_type)
                       .filter(lambda x: x[0] >= 0)  # Ensure valid hours
                       .map(lambda x: (x[0], (1 if x[1] == "CSH" else 0, 1)))  # (hour, (is_cash, total))
                       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # Sum up by hour
                       .map(lambda x: (x[0], round((1 - x[1][0] / x[1][1]) * 100, 2))))  # Calculate percentage of card payments

    card_percent = clean_rdd.filter(lambda x: x[10] != "CSH").count() / total_payments * 100
    cash_percent = 100 - card_percent

    print(f"Total Percentage - Cash: {cash_percent:.2f}%, Card: {card_percent:.2f}%")
    print("Hourly percentages of card payments:", payment_by_hour.collect())

    # Save the results to output directory
    sc.parallelize(payment_by_hour.collect()).coalesce(1).saveAsTextFile(sys.argv[2] + "/payment_by_hour")

    # Stop the SparkContext
    sc.stop()
