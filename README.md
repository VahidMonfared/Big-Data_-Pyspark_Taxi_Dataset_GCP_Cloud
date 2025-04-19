Big-Data_-Pyspark_Taxi_Dataset_GCP_Cloud Analysis
Many different taxis have had multiple drivers. Write and execute a Spark Python program that computes the top ten taxis (and Top-10 Best Drivers , The best time of the day to Work on Taxi ) that have had the largest number of drivers. Your output should be a set of (medallion, number of drivers) pairs.
Large dataset: https://storage.googleapis.com/met-cs-777-data/taxi-data-sorted-large.csv.bz2
Note: You should consider that this is a real-world data set that might include wrongly formatted data
lines. You should clean up the data before the main processing, a line might not include all of the fields. If a data line is not correctly formatted, you should drop that line and do not consider it.
