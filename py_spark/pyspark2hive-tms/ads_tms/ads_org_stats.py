from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col, coalesce, when, round
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TMSOrgStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_org_stats(df: DataFrame):
    """写入目标表ads_org_stats，增加类型校验和错误处理"""
    try:
        # 1. 校验目标表结构
        target_table = "tms.ads_org_stats"
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


def execute_org_stats_etl(target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行机构统计ETL，目标日期：{target_date}")

    # 1. 处理1天数据（1d表）：org_order_1d 与 org_trans_1d 全外连接
    # 1.1 订单数据（org_order_1d）
    org_order_1d = spark.table("tms.dws_trade_org_cargo_type_order_1d") \
        .filter(col("dt") == target_date) \
        .groupBy("org_id", "org_name") \
        .agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1))

    # 1.2 运输完成数据（org_trans_1d）
    org_trans_1d = spark.table("tms.dws_trans_org_truck_model_type_trans_finish_1d") \
        .filter(col("dt") == target_date) \
        .groupBy("org_id", "org_name") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("recent_days", lit(1)) \
        .withColumn("avg_trans_finish_distance",
                   when(col("trans_finish_count") > 0,
                        col("trans_finish_distance") / col("trans_finish_count")).otherwise(0)) \
        .withColumn("avg_trans_finish_dur_sec",
                   when(col("trans_finish_count") > 0,
                        col("trans_finish_dur_sec") / col("trans_finish_count")).otherwise(0))

    # 1.3 全外连接1d部分数据
    part1d = org_order_1d \
        .join(org_trans_1d,
              on=["dt", "recent_days", "org_id", "org_name"],
              how="full_outer") \
        .select(
        coalesce(org_order_1d["dt"], org_trans_1d["dt"]).alias("dt"),
        coalesce(org_order_1d["recent_days"], org_trans_1d["recent_days"]).alias("recent_days"),
        coalesce(org_order_1d["org_id"], org_trans_1d["org_id"]).alias("org_id"),
        coalesce(org_order_1d["org_name"], org_trans_1d["org_name"]).alias("org_name"),
        col("order_count"),
        col("order_amount"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec"),
        col("avg_trans_finish_distance"),
        col("avg_trans_finish_dur_sec")
    )

    # 2. 处理多天数据（nd表）：org_order_nd 与 org_trans_nd 内连接
    # 2.1 订单数据（org_order_nd）
    org_order_nd = spark.table("tms.dws_trade_org_cargo_type_order_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("org_id", "org_name", "recent_days") \
        .agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ) \
        .withColumn("dt", lit(target_date))

    # 2.2 运输完成数据（org_trans_nd）
    org_trans_nd = spark.table("tms.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == target_date) \
        .groupBy("org_id", "org_name", "recent_days") \
        .agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    ) \
        .withColumn("dt", lit(target_date)) \
        .withColumn("avg_trans_finish_distance",
                   when(col("trans_finish_count") > 0,
                        col("trans_finish_distance") / col("trans_finish_count")).otherwise(0)) \
        .withColumn("avg_trans_finish_dur_sec",
                   when(col("trans_finish_count") > 0,
                        col("trans_finish_dur_sec") / col("trans_finish_count")).otherwise(0))

    # 2.3 内连接nd部分数据
    partnd = org_order_nd \
        .join(org_trans_nd,
              on=["dt", "recent_days", "org_id", "org_name"],
              how="inner") \
        .select(
        col("dt"),
        col("recent_days"),
        col("org_id"),
        col("org_name"),
        col("order_count"),
        col("order_amount"),
        col("trans_finish_count"),
        col("trans_finish_distance"),
        col("trans_finish_dur_sec"),
        col("avg_trans_finish_distance"),
        col("avg_trans_finish_dur_sec")
    )

    # 3. 合并1d和nd部分数据（SQL中union逻辑，排除历史数据避免重复）
    final_df = part1d.unionByName(partnd)

    # 4. 按目标表列顺序整理
    target_columns = [
        "dt", "recent_days", "org_id", "org_name",
        "order_count", "order_amount",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
        "avg_trans_finish_distance", "avg_trans_finish_dur_sec"
    ]
    final_df = final_df.select(target_columns)

    final_df.show(5)
    # 5. 写入目标表
    write_to_ads_org_stats(final_df)
    print(f"[INFO] ETL执行完成，目标日期：{target_date}")


if __name__ == "__main__":
    target_date = '2025-07-11'  # 可通过调度工具动态传入（如Airflow/Oozie）
    execute_org_stats_etl(target_date)