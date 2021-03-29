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
  q1 = df.groupBy("COUNTRY").count()
  df = q1.selectExpr("COUNTRY as Country","count as Total")
  # df.printSchema()
  q1 = df.withColumn("Total",df.Total.cast('int'))
  # q1.printSchema()
  q1 = q1.toPandas()
  # # print(q1)
  q1.to_csv(outputfile,sep='\t',index=False)
  

if __name__ == "__main__":
    # global no_of_cpu
    no_of_cpu = sys.argv[1]
    outputfile = sys.argv[2]
#     print(no_of_cpu,outputfile)
    core = "local["+no_of_cpu+"]"
    main(core)
