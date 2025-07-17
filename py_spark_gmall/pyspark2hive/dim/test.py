# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

# 创建SparkSession并启用Hive支持
spark = SparkSession.builder \
    .appName("HiveConnector") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# 执行SQL查询并获取结果
df = spark.sql("SELECT * FROM gmall.ods_activity_rule WHERE ds='20250627'")
df.show()  # 显示结果

