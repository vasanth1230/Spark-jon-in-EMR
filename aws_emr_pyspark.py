#!/usr/bin/python
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark import SQLContext
from itertools import islice
from pyspark.sql.functions import col

import sys

if __name__ == '__main__':

    # creating the context
    sc = SparkContext(appName="aws_emr_pyspark")
    sqlContext = SQLContext(sc)

    # reading the first csv file and store it in an RDD
    rdd1 = sc.textFile('s3n://pyspark-test-kula/test.csv'
                       ).map(lambda line: line.split(','))

    # removing the first row as it contains the header
    rdd1 = rdd1.mapPartitionsWithIndex(lambda idx, it: (islice(it, 1,
            None) if idx == 0 else it))

    # converting the RDD into a dataframe
    df1 = rdd1.toDF(['policyID', 'statecode', 'county', 'eq_site_limit'])

    # dataframe which holds rows after replacing the 0's into null
    targetDf = df1.withColumn('eq_site_limit', when(df1['eq_site_limit'
                              ] == 0, 'null'
                              ).otherwise(df1['eq_site_limit']))

    df1WithoutNullVal = targetDf.filter(targetDf.eq_site_limit != 'null')

    rdd2 = sc.textFile('s3n://pyspark-test-kula/test2.csv'
                       ).map(lambda line: line.split(','))
    rdd2 = rdd2.mapPartitionsWithIndex(lambda idx, it: (islice(it, 1,
            None) if idx == 0 else it))
    df2 = df2.toDF(['policyID', 'zip', 'region', 'state'])

    innerjoineddf = df1WithoutNullVal.alias('a').join(df2.alias('b'),
            col('b.policyID') == col('a.policyID')).select([col('a.'
            + xx) for xx in a.columns] + [col('b.zip'), col('b.region'
            ), col('b.state')])

    innerjoineddf.write.parquet('s3n://pyspark-transformed-kula/test.parquet')
