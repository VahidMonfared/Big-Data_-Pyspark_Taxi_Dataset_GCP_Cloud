from __future__ import print_function
import sys
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
    # Validate row length and ensure required fields are valid floats
    if len(p) == 17:
        if isfloat(p[16]) and isfloat(p[12]) and isfloat(p[5]) and isfloat(p[14]):
            return p

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

    # Create a DataFrame from the RDD (for better processing)
    df = clean_rdd.map(lambda p: (float(p[16]), )).toDF(["tip_amount"])

    ### Task 4.4: IQR Outlier Detection for Tips
    # Calculate the quartiles using approxQuantile
    quantiles = df.approxQuantile("tip_amount", [0.25, 0.5, 0.75], 0.01)
    Q1, Q3 = quantiles[0], quantiles[2]
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Filter outliers based on IQR
    outliers_df = df.filter((df.tip_amount < lower_bound) | (df.tip_amount > upper_bound))

    # Get top 10 outliers
    top_10_outliers = outliers_df.orderBy(outliers_df.tip_amount.desc()).limit(10).collect()

    print(f"Top 10 Tip Outliers: {[row.tip_amount for row in top_10_outliers]}")

    # Save the results to output directory (as text file)
    sc.parallelize([ f"Top 10 Outliers: {[row.tip_amount for row in top_10_outliers]}"]).coalesce(1).saveAsTextFile(sys.argv[2] + "/tip_stats")

    # Stop the SparkContext
    sc.stop()
