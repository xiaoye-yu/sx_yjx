from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, coalesce, round
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TMSExpressOrgStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_express_org_stats(df: DataFrame):
    """写入目标表ads_express_org_stats，增加类型校验和错误处理"""
    try:
        # 1. 校验目标表结构
        target_table = "tms.ads_express_org_stats"
        spark = get_spark_session()
        target_df = spark.table(target_table)

        # 2. 按目标表列顺序和类型调整DataFrame
        df = df.select([col(c).cast(target_df.schema[c].dataType) for c in target_df.columns])

        df.write \
            .mode("append") \
            .insertInto(target_table)

        print(f"[INFO] 成功写入 {df.count()} 条新数据到 {target_table}")

    except Exception as e:
        print(f"[ERROR] 写入失败：{str(e)}")
        print("[ERROR] 待写入数据样例：")
        df.show(5)
        raise e  # 抛出异常，终止作业


def execute_express_org_stats_etl(target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行快递机构统计ETL，目标日期：{target_date}")

    # 1. 处理1天数据（1d表）
    # 1.1 派送成功数据
    org_deliver_1d = spark.table("tms.dws_trans_org_deliver_suc_1d") \
        .filter(col("dt") == target_date) \
        .groupBy("org_id", "org_name") \
        .agg(sum("order_count").alias("deliver_suc_count")) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 1.2 分拣数据
    org_sort_1d = spark.table("tms.dws_trans_org_sort_1d") \
        .filter(col("dt") == target_date) \
        .groupBy("org_id", "org_name") \
        .agg(sum("sort_count").alias("sort_count")) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 1.3 揽收数据（金额保留两位小数）
    org_receive_1d = spark.table("tms.dws_trans_org_receive_1d") \
        .filter(col("dt") == target_date) \
        .groupBy("org_id", "org_name") \
        .agg(
        sum("order_count").alias("receive_order_count"),
        round(sum("order_amount"), 2).alias("receive_order_amount")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 1.4 1d部分关联
    part1d = org_deliver_1d \
        .join(org_sort_1d, on=["dt", "recent_days", "org_id", "org_name"], how="full_outer") \
        .join(org_receive_1d, on=["dt", "recent_days", "org_id", "org_name"], how="full_outer") \
        .select(
        coalesce(org_deliver_1d["dt"], org_sort_1d["dt"], org_receive_1d["dt"]).alias("dt"),
        coalesce(org_deliver_1d["recent_days"], org_sort_1d["recent_days"], org_receive_1d["recent_days"]).alias(
            "recent_days"),
        coalesce(org_deliver_1d["org_id"], org_sort_1d["org_id"], org_receive_1d["org_id"]).alias("org_id"),
        coalesce(org_deliver_1d["org_name"], org_sort_1d["org_name"], org_receive_1d["org_name"]).alias("city_name"),
        col("receive_order_count"),
        col("receive_order_amount"),
        col("deliver_suc_count"),
        col("sort_count")
    )

    # 2. 处理多天数据（nd表）
    # 2.1 派送成功数据
    org_deliver_nd = spark.table("tms.dws_trans_org_deliver_suc_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("recent_days", "org_id", "org_name") \
        .agg(sum("order_count").alias("deliver_suc_count")) \
        .withColumn("dt", lit(target_date))

    # 2.2 分拣数据
    org_sort_nd = spark.table("tms.dws_trans_org_sort_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("recent_days", "org_id", "org_name") \
        .agg(sum("sort_count").alias("sort_count")) \
        .withColumn("dt", lit(target_date))

    # 2.3 揽收数据
    org_receive_nd = spark.table("tms.dws_trans_org_receive_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("recent_days", "org_id", "org_name") \
        .agg(
        sum("order_count").alias("receive_order_count"),
        round(sum("order_amount"), 2).alias("receive_order_amount")
    ) \
        .withColumn("dt", lit(target_date))

    # 2.4 nd部分关联
    partnd = org_deliver_nd \
        .join(org_sort_nd, on=["dt", "recent_days", "org_id", "org_name"], how="full_outer") \
        .join(org_receive_nd, on=["dt", "recent_days", "org_id", "org_name"], how="full_outer") \
        .select(
        coalesce(org_deliver_nd["dt"], org_sort_nd["dt"], org_receive_nd["dt"]).alias("dt"),
        coalesce(org_deliver_nd["recent_days"], org_sort_nd["recent_days"], org_receive_nd["recent_days"]).alias(
            "recent_days"),
        coalesce(org_deliver_nd["org_id"], org_sort_nd["org_id"], org_receive_nd["org_id"]).alias("org_id"),
        coalesce(org_deliver_nd["org_name"], org_sort_nd["org_name"], org_receive_nd["org_name"]).alias("city_name"),
        col("receive_order_count"),
        col("receive_order_amount"),
        col("deliver_suc_count"),
        col("sort_count")
    )

    # 3. 合并当前日期的新数据（不包含历史数据，避免重复写入）
    final_df = part1d.unionByName(partnd)

    # 4. 按目标表列顺序整理
    target_columns = [
        "dt", "recent_days", "org_id", "city_name",
        "receive_order_count", "receive_order_amount",
        "deliver_suc_count", "sort_count"
    ]
    final_df = final_df.select(target_columns)

    # 5. 写入目标表
    write_to_ads_express_org_stats(final_df)
    print(f"[INFO] ETL执行完成，目标日期：{target_date}")


if __name__ == "__main__":
    target_date = '2025-07-11'  # 可通过调度工具动态传入（如Airflow/Oozie）
    execute_express_org_stats_etl(target_date)