import sys,os
import pandas as pd
import numpy
import pyspark
from pyspark.sql import *
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as f
# from commons.Utils import Utils

import re
from pyspark.sql import Row

no_of_cpu = None
outputfile = None


def main(cpus):
  spark = SparkSession.builder.master(cpus).getOrCreate()
  df = spark.read.csv("airports.csv",header=True)
  df = df.filter(df.LATITUDE >= 10)
  df = df.filter(df.LATITUDE <= 90)
  df = df.filter(df.LONGITUDE <= -10)
  df = df.filter(df.LONGITUDE >= -90)
#   df.show()
  df = df.toPandas()  
  df["NAME"].to_csv(outputfile,sep=',',index=False)


if __name__ == "__main__":
    # global no_of_cpu
    no_of_cpu = sys.argv[1]
    outputfile = sys.argv[2]
#     print(no_of_cpu,outputfile)
    core = "local["+no_of_cpu+"]"
    main(core)
