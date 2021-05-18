import pyspark
from pyspark.sql import SparkSession
import sys


if __name__ == "__main__":
    sc = pyspark.SparkContext('local[*]')
    ss = SparkSession.builder.appName("to read csv file").getOrCreate()
    sc.setLogLevel("ERROR")

    inputPath = sys.argv[1]
    outputPath = sys.argv[2]

    regions = {"CA": None, "DE": None, "FR": None, "GB": None, "IN": None,
               "JP": None, "KR": None, "MX": None, "RU": None, "US": None}
    for key, value in regions.items():
        regions[key] = ss.read.csv(inputPath + key + "videos" + ".csv", header=True).rdd


