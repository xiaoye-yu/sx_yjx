from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum, col
from pyspark.sql import DataFrame


def get_spark_session():
    """初始化SparkSession，使用tms数据库"""
    spark = SparkSession.builder \
        .appName("TMSLineStatsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


def write_to_ads_line_stats(df: DataFrame):
    """写入目标表，增加类型校验和错误处理"""
    try:
        # 1. 校验目标表结构
        target_table = "tms.ads_line_stats"
        target_df = get_spark_session().table(target_table)
        # 按目标表列顺序和类型调整DataFrame
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


def execute_line_stats_etl(target_date: str):
    spark = get_spark_session()
    print(f"[INFO] 开始执行线路统计ETL，目标日期：{target_date}")

    # 1. 处理新数据：读取源表并聚合（仅处理当前日期的数据）
    dws_df = spark.table("tms.dws_trans_shift_trans_finish_nd") \
        .filter(col("dt") == target_date)  # 仅过滤目标日期的数据

    # 2. 分组聚合（与原逻辑一致）
    aggregated_df = dws_df.groupBy(
        "line_id", "line_name", "recent_days"
    ).agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        sum("trans_finish_order_count").alias("trans_finish_order_count")
    ).withColumn("dt", lit(target_date))  # 添加当前日期

    # 3. 调整列顺序与目标表一致
    new_data_df = aggregated_df.select(
        "dt", "recent_days", "line_id", "line_name",
        "trans_finish_count", "trans_finish_distance",
        "trans_finish_dur_sec", "trans_finish_order_count"
    )

    # 4. 仅写入新数据（无需UNION历史数据）
    write_to_ads_line_stats(new_data_df)
    print(f"[INFO] ETL执行完成，目标日期：{target_date}")


if __name__ == "__main__":
    target_date = '2025-07-11'  # 可通过调度工具动态传入
    execute_line_stats_etl(target_date)